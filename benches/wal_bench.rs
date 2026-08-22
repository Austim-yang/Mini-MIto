use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mini_mito::memtable::{Wal, wal::Operation};
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
                    let wal = Wal::new(&path).unwrap();
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

criterion_group!(benches, bench_wal_append, bench_wal_recover);
criterion_main!(benches);
