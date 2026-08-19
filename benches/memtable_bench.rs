use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mini_mito::Region;
use tempfile::tempdir;

fn key(i: u64) -> (Vec<u8>, i64) {
    (vec![i as u8], i as i64)
}
fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn bench_memtable_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_insert");
    for size in [100, 500, 1_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("wal.log");
                    let region = Region::new(&path).unwrap();
                    (region, dir)
                },
                |(region, _dir)| {
                    for i in 0..size {
                        region
                            .write(key(black_box(i)), value(black_box(i)))
                            .unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_memtable_flush(c: &mut Criterion) {
    c.bench_function("memtable_flush_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");
                let region = Region::new(&path).unwrap();
                for i in 0..10_000 {
                    region.write(key(i), value(i)).unwrap();
                }
                (region, dir)
            },
            |(region, _dir)| {
                region.flush().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_memtable_compact(c: &mut Criterion) {
    c.bench_function("memtable_compact_4_sst", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");
                let region = Region::new(&path).unwrap();
                for i in 0..5000 {
                    region.write(key(i), value(i)).unwrap();
                }
                (region, dir)
            },
            |(region, _dir)| {
                region.compact().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_memtable_insert,
    bench_memtable_flush,
    bench_memtable_compact
);
criterion_main!(benches);
