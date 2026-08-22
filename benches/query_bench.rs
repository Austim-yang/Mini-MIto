use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::context::SessionContext;
use mini_mito::{LSMTableProvider, Region};
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn key(i: u64) -> (Vec<u8>, i64) {
    (vec![i as u8], i as i64)
}
fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn setup_data(size: usize) -> (Arc<LSMTableProvider>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let region = Region::new(&wal_path).unwrap();
    for i in 0..size {
        region.write(key(i as u64), value(i as u64)).unwrap();
    }
    let provider = LSMTableProvider::new(region);
    (Arc::new(provider), dir)
}

fn bench_datafusion_full_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_full_scan");
    for size in [1_000, 10_000, 50_000].iter() {
        // setup 移出计时：只测查询成本
        let (provider, _dir) = setup_data(*size);
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.to_async(&rt).iter(|| {
                let provider = provider.clone();
                async move {
                    let ctx = SessionContext::new();
                    ctx.register_table("t", provider).unwrap();
                    let df = ctx.sql("SELECT * FROM t").await.unwrap();
                    let batches = df.collect().await.unwrap();
                    black_box(batches);
                }
            });
        });
    }
    group.finish();
}

fn bench_datafusion_projection(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let size = 10_000;
    let (provider, _dir) = setup_data(size);
    c.bench_function("datafusion_projection_tags_timestamp", |b| {
        b.to_async(&rt).iter(|| {
            let provider = provider.clone();
            async move {
                let ctx = SessionContext::new();
                ctx.register_table("t", provider).unwrap();
                let df = ctx.sql("SELECT tags, timestamp FROM t").await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            }
        });
    });
}

fn bench_datafusion_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let size = 10_000;
    let (provider, _dir) = setup_data(size);
    c.bench_function("datafusion_filter_timestamp_gt_half", |b| {
        b.to_async(&rt).iter(|| {
            let provider = provider.clone();
            async move {
                let ctx = SessionContext::new();
                ctx.register_table("t", provider).unwrap();
                let df = ctx
                    .sql(&format!("SELECT * FROM t WHERE timestamp > {}", size / 2))
                    .await
                    .unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_datafusion_full_scan,
    bench_datafusion_projection,
    bench_datafusion_filter
);
criterion_main!(benches);
