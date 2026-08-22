use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mini_mito::memtable::{
    Wal,
    wal::{Operation, SyncPolicy},
};
use tempfile::tempdir;

fn key(i: u64) -> (Vec<u8>, i64) {
    (vec![i as u8], i as i64)
}
fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn bench_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("wal.log");
                    let wal = mini_mito::memtable::Wal::with_sync_policy(&path, SyncPolicy::Never)
                        .unwrap();
                    (wal, dir)
                },
                |(mut wal, _dir)| {
                    for i in 0..size {
                        let op = mini_mito::memtable::wal::Operation::Insert {
                            key: key(black_box(i)),
                            seq: i,
                            value: value(black_box(i)),
                        };
                        wal.append(&op).unwrap();
                    }
                    wal.flush().unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_wal_recover(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_recover");
    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("wal.log");
                    {
                        let mut wal = Wal::new(&path).unwrap();
                        for i in 0..size {
                            let op = mini_mito::memtable::wal::Operation::Insert {
                                key: key(i),
                                seq: i,
                                value: value(i),
                            };
                            wal.append(&op).unwrap();
                        }
                        wal.close().unwrap();
                    }
                    (path, dir)
                },
                |(path, _dir)| {
                    let wal = Wal::new(&path).unwrap();
                    let mut count = 0usize;
                    wal.recover(&mut |_op: &Operation| {
                        count += 1;
                    })
                    .unwrap();
                    black_box(count);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_wal_append_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_bulk_append_frame");
    for batch_size in [10, 50, 100, 500].iter() {
        let ops: Vec<Operation> = (0..*batch_size)
            .map(|i| Operation::Insert {
                key: key(i),
                seq: i,
                value: value(i),
            })
            .collect();
        group.throughput(Throughput::Elements(*batch_size));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &ops, |b, ops| {
            let dir = tempdir().unwrap();
            let path = dir.path().join("frame.log");
            let mut wal =
                mini_mito::memtable::Wal::with_sync_policy(&path, SyncPolicy::Never).unwrap();
            b.iter(|| {
                wal.append_batch(black_box(ops)).unwrap();
            });
            wal.close().unwrap();
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_wal_append,
    bench_wal_recover,
    bench_wal_append_frame
);
criterion_main!(benches);
