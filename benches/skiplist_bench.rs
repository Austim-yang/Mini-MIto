use std::{hint::black_box, vec};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mini_mito::memtable::SkipList;
use rand::{RngExt, rng};

fn key(i: u64) -> (Vec<u8>, i64) {
    (vec![i as u8], i as i64)
}

fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist_insert");
    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || SkipList::new(),
                |list| {
                    for i in 0..size {
                        list.insert(key(black_box(i)), i, Some(value(black_box(i))));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist_get");
    let size = 10_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let mut rng = rng();
    group.bench_function("hit", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(0..size) as u64);
            list.get(&key(idx));
        });
    });
    group.bench_function("miss", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(size..(size * 2)) as u64);
            list.get(&key(idx));
        });
    });
    group.finish();
}

fn bench_iter(c: &mut Criterion) {
    let list = SkipList::new();
    for i in 0..10_000 {
        list.insert(key(i),i,  Some(value(i)));
    }
    c.bench_function("skiplist_iter", |b| {
        b.iter(|| {
            let mut count = 0;
            for _ in list.iter() {
                count += 1;
            }
            black_box(count);
        });
    });
}

criterion_group!(benches, bench_insert, bench_get, bench_iter);
criterion_main!(benches);