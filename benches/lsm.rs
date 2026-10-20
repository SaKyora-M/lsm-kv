//! 基准测试：写路径、读路径、compaction、并发读。
//!
//! 跑法：`cargo bench`（全部）、`cargo bench -- get_hit`（按名字过滤）、
//! `cargo bench -- --quick`（样本更少，数字略粗糙）。

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsm_kv::Db;

/// 基准数据集里的 key 数量。
const N: usize = 5_000;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    // 固定 ~75 字节，模拟常见的小 value
    format!("v-{i:08}-{}", "x".repeat(64)).into_bytes()
}

/// 开一个空库，并把刷盘阈值调到最大（永不自动 flush）。
fn db_empty() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.set_flush_threshold(usize::MAX);
    (dir, db)
}

/// 数据全部在 memtable 里的库。
fn db_memtable(n: usize) -> (tempfile::TempDir, Db) {
    let (dir, db) = db_empty();
    for i in 0..n {
        db.put(&key(i), &value(i)).unwrap();
    }
    (dir, db)
}

/// 数据已落成一张 SSTable 的库（memtable 为空）。
fn db_one_sstable(n: usize) -> (tempfile::TempDir, Db) {
    let (dir, db) = db_memtable(n);
    db.flush().unwrap();
    (dir, db)
}

// ── 写路径 ──

fn bench_write(c: &mut Criterion) {
    // 预先生成 key/value：避免把每次迭代的两次分配算进引擎耗时
    let keys: Vec<Vec<u8>> = (0..N).map(key).collect();
    let vals: Vec<Vec<u8>> = (0..N).map(value).collect();

    let mut g = c.benchmark_group("write");
    g.throughput(Throughput::Elements(1));

    // 只进 memtable + WAL：测的是 BTreeMap 插入 + WAL append 的纯开销
    g.bench_function("put_memtable", |b| {
        let (_dir, db) = db_empty();
        let mut i = 0usize;
        b.iter(|| {
            db.put(black_box(&keys[i % N]), black_box(&vals[i % N]))
                .unwrap();
            i += 1;
        });
    });

    // 阈值 256KB：测"写 + 定期刷盘"的真实吞吐（含 SSTable 编码与落盘）
    g.bench_function("put_with_flush", |b| {
        let (_dir, db) = db_empty();
        db.set_flush_threshold(256 * 1024);
        let mut i = 0usize;
        b.iter(|| {
            db.put(black_box(&keys[i % N]), black_box(&vals[i % N]))
                .unwrap();
            i += 1;
        });
    });

    g.finish();

    // 批量写：100 条记录共用一个 WAL 缓冲区，只做一次系统调用
    const BATCH: usize = 100;
    let batch: Vec<(&[u8], &[u8])> = (0..BATCH)
        .map(|j| (keys[j].as_slice(), vals[j].as_slice()))
        .collect();

    let mut gb = c.benchmark_group("write_batch");
    gb.throughput(Throughput::Elements(BATCH as u64));
    gb.bench_function("put_batch_100", |b| {
        let (_dir, db) = db_empty();
        b.iter(|| {
            db.put_batch(&batch).unwrap();
        });
    });
    gb.finish();
}

// ── 读路径 ──

fn bench_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("read");
    g.throughput(Throughput::Elements(1));

    let (_dir_mt, db_mt) = db_memtable(N);
    let (_dir_st, db_st) = db_one_sstable(N);

    // memtable 命中：BTreeMap 查找，最理想的情况
    g.bench_function("get_hit_memtable", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = key(i % N);
            i += 1;
            black_box(db_mt.get(black_box(&k)).unwrap());
        });
    });

    // SSTable 命中：open + 读 footer + 读 Bloom + 读索引 + 扫数据区
    g.bench_function("get_hit_sstable", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = key(i % N);
            i += 1;
            black_box(db_st.get(black_box(&k)).unwrap());
        });
    });

    // memtable 未命中：一次 BTreeMap 查找就返回
    g.bench_function("get_miss_memtable", |b| {
        b.iter(|| black_box(db_mt.get(black_box(b"no-such-key")).unwrap()));
    });

    // SSTable 未命中：Bloom 直接否定，不读数据区 —— Bloom 的全部价值所在
    g.bench_function("get_miss_sstable_bloom", |b| {
        b.iter(|| black_box(db_st.get(black_box(b"no-such-key")).unwrap()));
    });

    g.finish();
}

// ── compaction ──

fn bench_compact(c: &mut Criterion) {
    let mut keep: Vec<tempfile::TempDir> = Vec::new();

    c.bench_function("compact/4_tables_1k", |b| {
        b.iter_batched(
            // 每次迭代前现造 4 张表（不计入测量时间）
            || {
                let (dir, db) = db_empty();
                for t in 0..4 {
                    for i in 0..1000 {
                        let idx = t * 1000 + i;
                        db.put(&key(idx), &value(idx)).unwrap();
                    }
                    db.flush().unwrap();
                }
                (dir, db)
            },
            |(dir, db)| {
                db.compact().unwrap();
                // 目录留到整轮结束再删，避免把"删文件"算进 compact 耗时
                keep.push(dir);
            },
            BatchSize::PerIteration,
        );
    });
}

// ── 并发读 ──

fn bench_concurrent_read(c: &mut Criterion) {
    let (_dir, db) = db_one_sstable(N);
    let mut g = c.benchmark_group("concurrent_read");

    // 每个线程固定读 200 次，用 Throughput 折算成"每秒多少次读"，
    // 这样 1/2/4/8 线程的数字可以直接横向比较。
    for threads in [1usize, 2, 4, 8] {
        g.throughput(Throughput::Elements((threads * 200) as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let db = &db;
                            s.spawn(move || {
                                for i in 0..200 {
                                    let k = key((t * 200 + i) % N);
                                    black_box(db.get(black_box(&k)).unwrap());
                                }
                            });
                        }
                    });
                });
            },
        );
    }

    g.finish();
}

/// 纯 memtable 的并发读：没有文件 I/O，能干净地看出 `RwLock` 的读并行效果。
///
/// SSTable 那条路径每次 get 都要 open + 读文件，瓶颈在 OS 层面，
/// 锁是不是读写锁根本看不出来；要验证 ④-3 的改动，必须用这条纯内存路径。
fn bench_concurrent_read_memtable(c: &mut Criterion) {
    let (_dir, db) = db_memtable(N);
    let mut g = c.benchmark_group("concurrent_read_memtable");

    // 每线程 5000 次读（约 1ms/线程），把线程创建开销摊薄到 5% 以内
    for threads in [1usize, 2, 4, 8] {
        g.throughput(Throughput::Elements((threads * 5000) as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let db = &db;
                            s.spawn(move || {
                                for i in 0..5000 {
                                    let k = key((t * 5000 + i) % N);
                                    black_box(db.get(black_box(&k)).unwrap());
                                }
                            });
                        }
                    });
                });
            },
        );
    }

    g.finish();
}

criterion_group!(
    benches,
    bench_write,
    bench_read,
    bench_compact,
    bench_concurrent_read,
    bench_concurrent_read_memtable
);
criterion_main!(benches);
