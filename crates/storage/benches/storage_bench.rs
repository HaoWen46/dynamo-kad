//! Benchmarks for the storage engine (WAL + Memtable).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::record::StorageRecord;
use dynamo_storage::wal::FsyncPolicy;
use std::collections::HashMap;
use tempfile::TempDir;

fn make_record(key: &str, value_size: usize) -> StorageRecord {
    let value = vec![0x42u8; value_size];
    let mut vclock = HashMap::new();
    vclock.insert("node1".to_string(), 1);
    StorageRecord::new(key.to_string(), value, vclock)
}

fn bench_engine_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_put");

    for size in [64, 1024, 4096] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

            let mut i = 0u64;
            b.iter(|| {
                let key = format!("key_{}", i);
                let record = make_record(&key, size);
                engine.put(record).unwrap();
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_engine_get(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

    // Pre-populate 1000 keys
    for i in 0..1000 {
        let record = make_record(&format!("key_{:04}", i), 256);
        engine.put(record).unwrap();
    }

    c.bench_function("engine_get", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{:04}", i % 1000);
            black_box(engine.get(&key));
            i += 1;
        });
    });
}

fn bench_engine_put_overwrite(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

    // Pre-populate 100 keys
    for i in 0..100 {
        let record = make_record(&format!("key_{:04}", i), 256);
        engine.put(record).unwrap();
    }

    c.bench_function("engine_put_overwrite", |b| {
        let mut version = 2u64;
        b.iter(|| {
            for i in 0..100 {
                let key = format!("key_{:04}", i);
                let value = vec![0x42u8; 256];
                let mut vclock = HashMap::new();
                vclock.insert("node1".to_string(), version);
                let record = StorageRecord::new(key, value, vclock);
                engine.put(record).unwrap();
            }
            version += 1;
        });
    });
}

fn bench_wal_append(c: &mut Criterion) {
    use dynamo_storage::wal::Wal;

    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("bench.wal");
    let mut wal = Wal::open(&wal_path, FsyncPolicy::None).unwrap();

    c.bench_function("wal_append", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let record = make_record(&format!("k_{}", i), 128);
            wal.append(&record).unwrap();
            i += 1;
        });
    });
}

criterion_group!(
    benches,
    bench_engine_put,
    bench_engine_get,
    bench_engine_put_overwrite,
    bench_wal_append
);
criterion_main!(benches);
