# lsm-kv

一个用 Rust 从零实现的迷你 **LSM-Tree** 键值存储引擎。`src/` 约 2800 行，同步、无 async、无 unsafe，运行时只依赖 `crc32fast` 和 `parking_lot`。

LSM-Tree 的核心主张是**把随机写换成顺序写**：所有写入先进内存表并顺序追加 WAL，攒够一批再顺序落盘成不可变的有序表（SSTable）；后台合并这些表，让"查一个 key"需要的 I/O 保持在常数级。

这个项目把这套机制完整走了一遍：崩溃恢复、SSTable 二进制格式、Bloom Filter、稀疏索引、多层读取、合并、并发读写锁，以及一套可复现的基准测试。

---

## 特性

- **崩溃恢复**：WAL 每条记录带长度和 CRC32，启动时重放；被截断的残尾自动丢弃
- **不可变 SSTable**：数据区 + 稀疏索引 + Bloom 区 + 44 字节 footer（v3 格式）
- **分段校验和**：每条 entry 自带 CRC32，索引区和 Bloom 区各有段级 CRC32，坏文件报 `Corruption` 而不是返回错值
- **Bloom Filter**：10 bits/key、7 次哈希，未命中时只需读几 KB 就能否定
- **稀疏索引**：每 16 条记一个索引项，二分定位后只扫一个窗口
- **全量 compaction**：合并多张表、丢弃墓碑；先写新表再删旧表，任一时刻崩溃都不丢数据
- **读写锁并发**：`RwLock` 让读路径并行，8 线程读 memtable 吞吐 4.25×
- **76 个测试**，`cargo test` 全绿

---

## 性能

本机实测（Windows，16 核，criterion，5000 条数据）：

| 操作 | 耗时 |
|---|---|
| `put` 单条（仅 memtable + WAL） | 6.36 µs |
| `put_batch` 100 条一批 | 0.30 µs/条 |
| `get` 命中 memtable | 207 ns |
| `get` 命中 SSTable | 122 µs |
| `get` 未命中 SSTable（Bloom 拦截） | 71.9 µs |
| `compact`（合并 4 张 1000 条的表） | 12.3 ms |

并发读（纯 memtable，16 核）：

| 线程 | 吞吐 | 相对单线程 |
|---|---|---|
| 1 | 4.10 M/s | 1.00× |
| 2 | 7.34 M/s | 1.79× |
| 4 | 12.94 M/s | 3.16× |
| 8 | 17.45 M/s | 4.25× |

> 这些数字受文件系统影响很大：SSTable 路径在 Windows 上主要耗在 `open`/`read` 系统调用上，Linux 上会低不少。
> 复现：`cargo bench`。

---

## 快速开始

```bash
cargo test                          # 76 个测试
cargo run --example cli -- ./mydb   # 交互式 REPL
cargo bench                         # 基准测试
```

REPL 用法：

```
> put hello world
ok
> get hello
world
> delete hello
ok
> exit
```

当作库用：

```rust
use lsm_kv::Db;

fn main() -> lsm_kv::Result<()> {
    let db = Db::open("./data")?;

    db.put(b"key", b"value")?;
    assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));

    db.delete(b"key")?;
    assert_eq!(db.get(b"key")?, None);

    Ok(())
}
```

批量写（一次系统调用落一整批，适合日志回放、批量导入）：

```rust
use lsm_kv::Db;

fn main() -> lsm_kv::Result<()> {
    let db = Db::open("./data")?;

    let batch: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2")];
    db.put_batch(&batch)?;

    Ok(())
}
```

---

## 架构

```
写入： put ──► WAL 追加（顺序写）──► MemTable（BTreeMap）
                                          │
                                    达到 4MB 阈值
                                          ▼
                               flush 成 SSTable（有序、不可变）
                                          │
                                    表数 ≥ 4 张
                                          ▼
                                  全量 compact 合并成一张

读取： MemTable ──未命中──► 新 SSTable ──► 旧 SSTable ──► 都没有 = None
        （命中或遇到墓碑就停，不再往更旧的表找）
```

```
src/
├── lib.rs       模块声明与公开 API
├── db.rs        组装层：open / put / get / delete / flush / compact / put_batch
├── memtable.rs  内存表：BTreeMap + 墓碑三态
├── wal.rs       预写日志：分帧、CRC、重放与截断
├── sstable.rs   SSTable 读写：Bloom、稀疏索引、v3 校验和
└── error.rs     错误类型
```

### 几个关键设计

- **删除 = 写墓碑**：磁盘上的表不可变，删除只能在上层写一个 `None` 标记去覆盖旧值。读到墓碑必须停下，否则会读出已被删除的旧数据。
- **锁顺序**：同时拿多把锁时按 `memtable → sstables → wal`（前缀原则），读路径两把读锁不嵌套。`parking_lot` 的锁不可重入，持读锁再拿写锁同样会死锁。
- **flush 的可见性**：memtable 写锁持有到函数结束，靠局部变量**逆序析构**保证 sstables 锁先释放、memtable 锁后释放。否则读者可能看到"memtable 已清空、新表还没登记"的中间态，静默返回 `None`。
- **compaction 崩溃安全**：① 写新表 → ② 改内存 → ③ 删旧文件。任一时刻崩溃，最坏只留下冗余旧文件（读时按 id 新覆盖旧），绝不丢数据。
- **为什么可以丢墓碑**：只有"全量合并"才能安全丢——合并后底下不再有任何更老的表。改成 leveled 之后这条就不成立了。

---

## 存储格式

SSTable v3 的整体布局：

```
┌──────────┬──────────┬──────────┬────────────────────┐
│  数据区   │  索引区   │ Bloom 区  │   Footer (44B)     │
└──────────┴──────────┴──────────┴────────────────────┘
                                    index_offset  u64
                                    index_len     u64
                                    bloom_offset  u64
                                    bloom_len     u64
                                    index_crc32   u32
                                    bloom_crc32   u32
                                    magic "LSM3"
```

每条 entry：

```
[key_len u32][key][flags u8][value_len u32][value][crc32 u32]
```

完整的字节布局、每个字段的设计理由、边界情况（空表、端序坑、索引项计数）见 [`file/SSTable格式设计.md`](file/SSTable格式设计.md)。

WAL 记录：

```
[payload_len u32][crc32 u32][payload]
payload = [op u8][key_len u32][key][value_len u32][value]
```

---

## 已知限制

这是"mini-lsm"，不是"生产可用的 RocksDB"。刻意保留的边界：

- **compaction 是全量的**：每次合并所有表，数据量大时写放大很高；没有 leveled / tiered 分级
- **compaction 同步执行**：会阻塞读写，没有后台线程
- **只有单数据块**：没有块级索引，没有压缩
- **没有范围扫描**：只有点查 API（内部已有 `read_all` 作为雏形）
- **没有 fsync 策略**：进程崩溃安全，但掉电可能丢最近的写入
- **没有 MVCC / 快照 / 事务**

## Roadmap

- [ ] `scan(range)` 范围扫描
- [ ] leveled compaction + 后台 compaction 线程
- [ ] 块级索引与数据压缩
- [ ] 可配置的 fsync 策略与组提交

---

## 许可

MIT，见 [LICENSE](LICENSE)。
