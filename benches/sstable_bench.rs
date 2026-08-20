use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mini_mito::{memtable::SkipList, schema::TableSchema, sstable::sstable::SSTable};
use rand::{RngExt, rng};
use tempfile::tempdir;

fn key(i: u64) -> (Vec<u8>, i64) {
    (vec![i as u8], i as i64)
}
fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn bench_sstable_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_create");
    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let list = SkipList::new();
                    for i in 0..size {
                        list.insert(key(i), i, Some(value(i)));
                    }
                    (list, tempdir().unwrap())
                },
                |(list, dir)| {
                    let path = dir.path().join("test.sst");
                    let _ = SSTable::create_from_skiplist(
                        &list,
                        1,
                        &path,
                        true,
                        &TableSchema::default_table(),
                    )
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_sstable_get(c: &mut Criterion) {
    let size = 10_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())
        .unwrap();
    let mut rng = rng();

    c.bench_function("sstable_get_hit", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(0..size) as u64);
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
    c.bench_function("sstable_get_miss", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(size..(size * 2)) as u64);
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
}

fn bench_sstable_scan(c: &mut Criterion) {
    let size = 10_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())
        .unwrap();

    c.bench_function("sstable_scan_all", |b| {
        b.iter(|| {
            let _ = sst.scan(&key(0), &key(size - 1)).unwrap();
        });
    });

    c.bench_function("sstable_scan_range_10pct", |b| {
        b.iter(|| {
            let start = black_box(size / 10);
            let end = black_box(size / 10 * 2);
            let _ = sst.scan(&key(start as u64), &key(end as u64)).unwrap();
        });
    });
}

fn bench_sstable_scan_time_range(c: &mut Criterion) {
    let size: u64 = 100_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())
        .unwrap();
    let min = sst.min_key().clone();
    let max = sst.max_key().clone();

    c.bench_function("sstable_scan_iter_all_100k", |b| {
        b.iter(|| {
            let rows: Vec<_> = sst.scan_iter(&min, &max).unwrap().collect();
            black_box(rows);
        });
    });
    c.bench_function("sstable_scan_iter_time_range_100k", |b| {
        b.iter(|| {
            let rows: Vec<_> = sst
                .scan_iter_with_range(&min, &max, Some((20_000, 40_000)))
                .unwrap()
                .collect();
            black_box(rows);
        });
    });
}

fn bench_sstable_get_100k(c: &mut Criterion) {
    let size: u64 = 100_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())
        .unwrap();
    let mut rng = rng();

    c.bench_function("sstable_get_hit_100k", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(0..size));
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
    c.bench_function("sstable_get_miss_100k", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(size..(size * 2)));
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
}

fn bench_sstable_scan_100k(c: &mut Criterion) {
    let size: u64 = 100_000;
    let list = SkipList::new();
    for i in 0..size {
        list.insert(key(i), i, Some(value(i)));
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())
        .unwrap();

    c.bench_function("sstable_scan_all_100k", |b| {
        b.iter(|| {
            let _ = sst.scan(&key(0), &key(size - 1)).unwrap();
        });
    });
    c.bench_function("sstable_scan_range_10pct_100k", |b| {
        b.iter(|| {
            let start = black_box(size / 10);
            let end = black_box(size / 10 * 2);
            let _ = sst.scan(&key(start), &key(end)).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_sstable_create,
    bench_sstable_get,
    bench_sstable_get_100k,
    bench_sstable_scan,
    bench_sstable_scan_time_range,
    bench_sstable_scan_100k,
);
criterion_main!(benches);
