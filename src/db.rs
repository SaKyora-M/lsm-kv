//! 存储引擎入口：把 MemTable、WAL、SSTable 组装成公开 API。
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::sstable::{self, Lookup, SstableMeta};
use crate::wal::{self, Wal, WalRecord};
/// MemTable 近似大小达到该阈值就刷盘。
pub const FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;
pub const COMPACTION_THRESHOLD: usize = 4;

/// SSTable 列表 + 下一个要分配的编号。
///
/// 两者绑在一起用同一把锁保护：
/// `flush` 里"push 新表 + `next_id += 1`"必须是一个原子动作，
/// 否则可能出现"表 push 了但编号没推进"的中间态。
struct SstableState {
    /// 已有的 SSTable，从旧到新排列（**末尾 = 最新**）。
    metas: Vec<SstableMeta>,
    /// 下一个要分配的 SSTable 编号。
    next_id: u64,
}

/// 存储引擎入口。
///
/// 内部结构：
/// - `dir`：数据目录
/// - `memtable`：内存表，最新数据都在这里
/// - `wal`：预写日志，负责持久化
/// - `sstables`：已落盘的有序表列表 + 下一个编号
/// - `flush_threshold`：刷盘阈值（测试里可调小）
///
/// 可变状态都包在 `Mutex` / `RwLock` 里，支持 `&self` 内部可变性 + 多线程共享。
///
/// **并发模型（阶段④-3）**：
/// - `memtable` / `sstables` 用 `RwLock`：读操作（`get`）取**共享读锁**，
///   多个读线程可以同时进行、互不阻塞；写操作（`put`/`delete`/`flush`/`compact`）取**排他写锁**。
/// - `wal` 仍是 `Mutex`：追加写本来就是串行的。
/// - `get` 的两把读锁**不嵌套**：先读 memtable、释放，再读 sstables，
///   读路径上没有"持着 A 等 B"的情况，因此读读之间完全并行。
///
/// **锁顺序约定**：同时拿多把锁时，必须按
/// `memtable → sstables → wal` 的顺序（前缀原则：拿了后面的，就不能再回头拿前面的）。
///
/// 当前各方法持有的锁：
/// - `flush`：memtable(w) → sstables(w) → wal（三把）
/// - `compact`：memtable(w) → sstables(w)（memtable 用于挡并发 flush）
/// - `put` / `delete`：临界区块内 memtable(w) → wal
/// - `get`：memtable(r) 释放后 → sstables(r)
/// - `maybe_compact`：sstables(r)，释放后才调 `compact`
///
/// 关键：`parking_lot` 的 `Mutex` / `RwLock` **都不可重入**——
/// 同一线程重复拿锁、或**持读锁时再拿写锁**，都会死锁。
/// 所以 `flush` / `compact` 返回后必须先把锁释放干净，调用方（如
/// `put` → `flush` → `maybe_compact` → `compact`）才能继续往下拿锁，
/// 绝不能在持锁时嵌套调用会再拿同一把锁的方法。
pub struct Db {
    /// 数据目录，SSTable 和 WAL 都在这里。
    dir: PathBuf,
    memtable: RwLock<MemTable>,
    wal: Mutex<Wal>,
    sstables: RwLock<SstableState>,
    /// 刷盘阈值。默认 `FLUSH_THRESHOLD`，测试里可调小。
    ///
    /// 用 `AtomicUsize` 而不是 `usize`：`put` / `delete` 是 `&self`，
    /// 测试需要在小数据量下改小阈值来触发自动 flush。
    /// 无锁原子读的开销可忽略。
    flush_threshold: AtomicUsize,
}

impl Db {
    /// 打开（或创建）位于 `path` 目录下的存储引擎实例。
    ///
    /// - 目录不存在会自动递归创建
    /// - 启动时扫描已有 SSTable，重建列表
    /// - 重放 WAL，恢复上次关闭前的数据（WAL 比 SSTable 新，会覆盖旧值）
    /// - 崩溃残留的半截 WAL 记录会被自动截断丢弃
    ///
    /// `impl AsRef<Path>` 表示：任何能"当作路径看"的类型都能传进来，
    /// 比如 `&str`、`String`、`&Path`、`PathBuf`。
    pub fn open(path: impl AsRef<Path>) -> Result<Db> {
        let dir = path.as_ref();

        // 确保目录存在（已存在则什么都不做）
        std::fs::create_dir_all(dir)?;

        // 0) 扫描已有的 SSTable，重建列表并推导 next_id。
        //    这些是比 WAL 更旧的持久数据；WAL 重放会覆盖它们。
        let metas = Self::scan_sstables(dir)?;
        let next_id = metas
            .last()
            .and_then(|m| parse_sst_id(&m.path))
            .map(|id| id + 1)
            .unwrap_or(1);

        let wal_path = dir.join("wal.log");

        // 1) 重放 WAL。文件不存在 = 首次启动，视为空日志。
        let (records, valid_len) = match wal::replay(&wal_path) {
            Ok(x) => x,
            // match 守卫：只有"文件不存在"这一种 Io 错误才当空处理，
            // 其它 Io 错误照常向上返回。
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                (Vec::new(), 0)
            }
            Err(e) => return Err(e),
        };

        // 2) 若文件里有崩溃残留的半截数据，截断到有效长度，
        //    这样后续追加写不会接在坏数据后面。
        let actual_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        if actual_len > valid_len {
            let f = std::fs::OpenOptions::new().write(true).open(&wal_path)?;
            f.set_len(valid_len)?; // set_len 把文件截到指定长度
        }

        // 3) 用重放出的记录重建内存表。
        let mut memtable = MemTable::new();
        for record in records {
            match record {
                WalRecord::Put { key, value } => memtable.put(&key, &value),
                WalRecord::Delete { key } => memtable.delete(&key),
            }
        }

        // 4) 打开 WAL，用于后续追加写入。
        let wal = Wal::open(&wal_path)?;

        Ok(Db {
            dir: dir.to_path_buf(),
            memtable: RwLock::new(memtable),
            wal: Mutex::new(wal),
            sstables: RwLock::new(SstableState { metas, next_id }),
            flush_threshold: AtomicUsize::new(FLUSH_THRESHOLD),
        })
    }

    /// 写入一个键值对（已存在则覆盖）。
    ///
    /// **顺序不能反**：先 append 到 WAL，成功后再改内存表。
    /// 这样即使两步之间崩溃，WAL 里有记录，重启能恢复。
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    // 先拿 memtable 锁，再拿 wal 锁（= flush 的 memtable→sstables→wal 前缀），
    // 让"写 WAL + 改内存"成为一个原子步骤：flush 要动 memtable 就先得拿到这把锁，
    // 所以不可能插到这两步中间，也就不会出现"WAL 被截断但记录只活在内存里"的丢失。
        let size = {
            let mut mt = self.memtable.write();
            self.wal.lock().put(key, value)?;
            mt.put(key, value);
            mt.approx_size()
        };
    // 上面块结束时 mt 已自动释放，此时再调 flush 不会自锁。
        if size >= self.flush_threshold.load(Ordering::Relaxed) {
            self.flush()?;
            if let Err(e) = self.maybe_compact() {
                eprintln!("warning: compaction failed: {e}");
            }
        }
        Ok(())
    }

    /// 批量写入：整批共用**一次** WAL 追加和一次锁，而不是 N 次。
    ///
    /// 为什么需要它：单条 `put` 的成本几乎全在 WAL 的那一次 `write_all`
    /// 系统调用上（本机实测 ~6µs）。批量写把 N 次调用压成 1 次，
    /// 单条摊销成本能降一个数量级——适合日志回放、批量导入这类场景。
    ///
    /// 原子性：和 `put` 一样，整批在**同一把 memtable 写锁**内完成
    /// "写 WAL + 改内存"，不会被并发 flush 从中间切开。
    ///
    /// 崩溃语义是"前缀持久"：WAL 里每条记录都有独立的长度和 CRC，
    /// 批写到一半崩溃时 `replay` 会在残缺处停下并截断，读不出半个值。
    pub fn put_batch(&self, kvs: &[(&[u8], &[u8])]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }
        let size = {
            let mut mt = self.memtable.write();
            self.wal.lock().put_batch(kvs)?;
            for (k, v) in kvs {
                mt.put(k, v);
            }
            mt.approx_size()
        };
        if size >= self.flush_threshold.load(Ordering::Relaxed) {
            self.flush()?;
            if let Err(e) = self.maybe_compact() {
                eprintln!("warning: compaction failed: {e}");
            }
        }
        Ok(())
    }

    /// 读取一个键。键不存在或已被删除，都返回 `None`。
    ///
    /// 查找顺序：MemTable → SSTable（从新到旧）。
    /// 每一层三态：
    /// - MemTable：`Some(Some(v))` = 有值；`Some(None)` = 墓碑；`None` = 未命中
    /// - SSTable：`Found(v)` / `Tombstone` / `NotFound`
    ///
    /// **遇到墓碑必须停**，不能继续往更旧的表找——
    /// 否则会读到已被删除的旧值。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1) 先查 MemTable。三态直接用 get_raw，能区分"墓碑"和"未命中"。
        {
            let mt = self.memtable.read();
            match mt.get_raw(key) {
                Some(Some(v)) => return Ok(Some(v.clone())),
                Some(None) => return Ok(None), // 墓碑：最新状态就是已删
                None => {}                     // 未命中：继续往 SSTable 查
            }
        }

        // 2) 从新到旧遍历 SSTable。sstables 里旧在前、新在后，
        //    所以用 rev() 反着来。
        let metas = self.sstables.read();
        for meta in metas.metas.iter().rev() {
            match sstable::get(&meta.path, key)? {
                Lookup::Found(v) => return Ok(Some(v)),
                Lookup::Tombstone => return Ok(None), // 墓碑：停
                Lookup::NotFound => continue,         // 继续往更旧查
            }
        }

        // 3) 全部未命中。
        Ok(None)
    }

    /// 删除一个键（写 tombstone，而不是物理删除）。
    ///
    /// 同样遵守"先写 WAL，再改内存表"。
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let size = {
            let mut mt = self.memtable.write();
            self.wal.lock().delete(key)?;
            mt.delete(key);
            mt.approx_size()
        };
        if size >= self.flush_threshold.load(Ordering::Relaxed) {
            self.flush()?;
            if let Err(e) = self.maybe_compact() {
                eprintln!("warning: compaction failed: {e}");
            }
        }
        Ok(())
    }    

    /// 把 MemTable 刷盘成一张新 SSTable，并清空 WAL 和 MemTable。
    ///
    /// **顺序严格如下，不能变**：
    /// 1. 写新 SSTable 文件
    /// 2. 截断 WAL 到 0
    /// 3. 清空 MemTable
    /// 4. 登记新表、推进 `next_id`
    ///
    /// 崩溃窗口分析：
    /// - ①后崩：SSTable 已落盘，WAL 未截。重启扫到新表 + 重放 WAL，
    ///   数据被覆盖成同一份，结果正确，只是白刷一次。
    /// - ②后崩：SSTable 已落盘，WAL 空。重启只读 SSTable，正确。
    /// - ③是纯内存操作，无崩溃风险。
    ///
    /// 任一 IO 失败都**提前返回错误**，且**不动内存状态**——
    /// 最坏结果就是下次再刷一遍。这是幂等的。
    ///
    /// **锁顺序**：memtable → sstables → wal（全项目统一）。
    ///
    /// **读可见性（RwLock 下尤其重要）**：memtable 的写锁要一直持有到函数结束，
    /// 而局部变量按**声明的逆序**析构，所以 `sstables` 写锁先释放、`memtable` 写锁后释放。
    /// 这保证任何读者要么看到"旧 memtable + 旧 metas"，要么看到"空 memtable + 新 metas"，
    /// 绝不会看到"memtable 已清空、但新表还没登记"——那会静默丢数据。
    pub fn flush(&self) -> Result<()> {
        // 一次性拿三把锁，顺序：memtable → sstables → wal。
        // 这里手动管理锁的获取顺序，作用域到函数结束。
        let mut memtable = self.memtable.write();

        // 空表不用刷，直接短路（避免产生空 SSTable 文件）。
        if memtable.approx_size() == 0 {
            return Ok(());
        }

        let mut sst = self.sstables.write();

        // ① 写 SSTable。文件名用当前 next_id。
        let id = sst.next_id;
        let path = self.dir.join(format!("{id:06}.sst"));
        let meta = sstable::write_sstable(&path, memtable.iter())?;

        // ② 截断 WAL。此时 SSTable 已落盘，WAL 里的记录可以安全丢弃。
        self.wal.lock().truncate()?;

        // ③ 清空 MemTable。
        *memtable = MemTable::new();

        // ④ 登记新表、推进 next_id（同一个锁内，原子）。
        sst.metas.push(meta);
        sst.next_id = id + 1;

        Ok(())
    }

/// 全量合并所有 SSTable（v1 简化版 compaction）。
///
/// 目标：把多张表合成一张，清理旧版本、丢弃墓碑、降低读放大。
///
/// **v1 为什么能安全丢墓碑**：这里是"全量合并"——把**所有** SSTable
/// 都读进来合成一张，合并后不再有任何更老的表存在于它们之下。
/// 因此一个 key 若合并结果里是墓碑，说明"它删掉了所有更旧版本，
/// 且没有更老的数据需要被这条墓碑遮住"，墓碑本身可以物理丢弃。
/// 将来若改成 leveled / 只合并部分表，墓碑就**不能**随便丢
/// （否则被删除的旧值会在更底层"复活"）。
///
/// **锁顺序**：memtable → sstables（与 `flush` 一致）。
/// 这里必须拿 memtable 锁，作用是挡住并发 flush：
/// flush 改 `metas` 前要先拿 memtable 锁，compact 拿着它，
/// flush 就进不来，`metas` 在 compact 期间不会被改。
///
/// **崩溃安全（关键）**：写盘顺序必须是
///   ① 写新表成功 → ② 改内存 `metas` → ③ 删旧文件
/// 绝不能在 ① 成功前改内存或删文件。这样任一时刻崩溃：
/// - ① 之前崩：旧文件原封不动，内存 `metas` 没变，重开扫旧表，数据完整。
/// - ① 之后、② 之前崩：新表已落盘但内存没登记，旧文件还在。
///   重开时 `scan_sstables` 扫到新表（id 更大）+ 旧表，读路径"新表覆盖旧表"，
///   结果**正确**，只是留了冗余文件（下次 compact 会再清掉）。
/// - ② 之后、③ 之前崩：内存已换成 [新表]，磁盘上新旧都在。
///   重开扫到全部，同样"新覆盖旧"，**正确**。
/// - ③ 之后崩：干净。
/// 即"最坏只留冗余旧文件，绝不丢数据"。
pub fn compact(&self) -> Result<()> {
    // 先拿 memtable 锁（挡住并发 flush），再拿 sstables 锁。
    // 与 flush 的 memtable → sstables 顺序一致，不会死锁。
    let _memtable = self.memtable.write();
    let mut sst = self.sstables.write();

    // 少于 2 张表没什么可合并的。
    if sst.metas.len() < 2 {
        return Ok(());
    }

    // 从旧到新把每张表读进同一个 BTreeMap。
    // 新表后 insert → 同 key 覆盖旧值，天然实现了"新版本优先"。
    let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
        std::collections::BTreeMap::new();
    for meta in &sst.metas {
        for (key, value) in sstable::read_all(&meta.path)? {
            merged.insert(key, value);
        }
    }

    // 全量合并后不再有更老的数据，墓碑可以安全丢弃。
    merged.retain(|_, v| v.is_some());

    // 记录输入文件，最后删。注意先 clone 路径，后面会替换 metas。
    let old_paths: Vec<PathBuf> = sst.metas.iter().map(|m| m.path.clone()).collect();

    if merged.is_empty() {
        // 合并结果为空（全是墓碑 / 或本来就是空）：清空内存列表、删掉所有输入文件。
        sst.metas.clear();
        for p in &old_paths {
            // 文件可能已不存在（极端情况），忽略 NotFound，其它错误照常返回。
            match std::fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        return Ok(());
    }

    // 用 next_id 写新表：id 一定比所有输入表大 → 读时天然覆盖旧表。
    let id = sst.next_id;
    let path = self.dir.join(format!("{id:06}.sst"));
    let meta = sstable::write_sstable(
        &path,
        merged.iter().map(|(k, v)| (k.as_slice(), v.as_deref())),
    )?;

    // ① 成功之后，才动内存：把 metas 换成 [新表]，推进 next_id。
    sst.metas = vec![meta];
    sst.next_id = id + 1;
    drop(sst); // 尽早释放 sstables 锁，删文件不再需要它

    // ③ 最后删旧文件。删失败不影响正确性（内存已是 [新表]，磁盘上冗余旧表
    //    下次 compact 或重开都会因 id 更小而排在后面），但仍把错误上报。
    for p in &old_paths {
        match std::fs::remove_file(p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }

    // _memtable 在这里随作用域结束释放。
    Ok(())
}

/// 检查 SSTable 张数，达到阈值就触发一次 compact。
///
/// **必须在 `flush()` 返回之后调用**：flush 返回时其内部
/// memtable/sstables/wal 三把锁都已释放，本函数再调 `compact()`
/// （内部重新按 memtable → sstables 拿锁）才不会和 flush 的锁嵌套。
/// parking_lot 的 Mutex / RwLock 都不可重入（**持读锁时再拿写锁同样死锁**）。
///
/// 这里只读一下张数就放锁，再调 compact，避免持锁调 compact。
fn maybe_compact(&self) -> Result<()> {
    let should = {
        let sst = self.sstables.read();
        sst.metas.len() >= COMPACTION_THRESHOLD
    };                                        // ← sstables 锁释放
    if should {
        self.compact()?;                      // ← compact 自己重新拿锁
    }
    Ok(())
}

    /// 调整刷盘阈值（字节），默认 [`FLUSH_THRESHOLD`]。
    ///
    /// 主要给测试和基准测试用：调小就能在小数据量下触发自动 flush / compaction。
    /// 生产环境一般不需要动它（换阈值属于调优，不属于正确性）。
    pub fn set_flush_threshold(&self, bytes: usize) {
        self.flush_threshold.store(bytes, Ordering::Relaxed);
    }

    /// 扫描目录下所有 `*.sst`，按编号升序读回元信息。
    ///
    /// 返回列表：旧表在前、新表在后（**末尾 = 最新**）。
    /// 目录里非 `*.sst` 文件、或文件名不是纯数字的 `*.sst` 都跳过。
    fn scan_sstables(dir: &Path) -> Result<Vec<SstableMeta>> {
        let mut ids: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("sst") {
                continue;
            }
            if let Some(id) = parse_sst_id(&path) {
                ids.push(id);
            }
        }
        ids.sort_unstable();

        let mut metas = Vec::with_capacity(ids.len());
        for id in ids {
            let path = dir.join(format!("{id:06}.sst"));
            metas.push(sstable::load_meta(&path)?);
        }
        Ok(metas)
    }
}

#[cfg(test)]
impl Db {
    /// 测试专用：调整刷盘阈值，方便用小数据触发自动 flush。
    /// （公开版本见 `set_flush_threshold`，基准测试也用它。）
    pub(crate) fn set_flush_threshold_for_test(&self, n: usize) {
        self.set_flush_threshold(n);
    }
}

/// 从 `000001.sst` 这类文件名解析出编号。
/// 解析失败（非纯数字）返回 `None`，调用方跳过。
fn parse_sst_id(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    stem.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 在临时目录里开一个 Db，返回 (目录, Db)。
    /// 目录要一直持有到测试结束，否则临时目录被删、Db 没意义。
    fn open_temp() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        (dir, db)
    }

    /// 把 flush 阈值调到很小，方便测试触发自动 flush。
    fn set_threshold(db: &Db, n: usize) {
        db.set_flush_threshold_for_test(n);
    }

    #[test]
    fn put_then_get() {
        let (_dir, db) = open_temp();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn overwrite_returns_latest() {
        let (_dir, db) = open_temp();
        db.put(b"k", b"old").unwrap();
        db.put(b"k", b"new").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn delete_returns_none() {
        let (_dir, db) = open_temp();
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn get_missing_returns_none() {
        let (_dir, db) = open_temp();
        assert_eq!(db.get(b"nope").unwrap(), None);
    }

    #[test]
    fn open_scans_existing_sstables() {
        let dir = tempfile::tempdir().unwrap();

        // 手动造两个 SSTable：000001.sst 和 000002.sst
        let p1 = dir.path().join("000001.sst");
        let entries1: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"a"[..], Some(&b"1"[..]))];
        sstable::write_sstable(&p1, entries1.into_iter()).unwrap();

        let p2 = dir.path().join("000002.sst");
        let entries2: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"b"[..], Some(&b"2"[..]))];
        sstable::write_sstable(&p2, entries2.into_iter()).unwrap();

        // 放两个干扰文件，应被跳过
        std::fs::write(dir.path().join("not-an-sst.txt"), b"junk").unwrap();
        std::fs::write(dir.path().join("abc.sst"), b"junk").unwrap();

        let db = Db::open(dir.path()).unwrap();

        {
            let sst = db.sstables.read();
            assert_eq!(sst.metas.len(), 2);
            assert_eq!(sst.metas[0].path, p1);
            assert_eq!(sst.metas[1].path, p2);
            // 旧→新，末尾最新
            assert_eq!(sst.metas[0].min_key, b"a".to_vec());
            assert_eq!(sst.metas[1].min_key, b"b".to_vec());
            assert_eq!(sst.next_id, 3);
        }
    }

    #[test]
    fn open_with_no_sstables_starts_at_one() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        let sst = db.sstables.read();
        assert_eq!(sst.next_id, 1);
        assert!(sst.metas.is_empty());
    }

    #[test]
    fn flush_moves_data_to_sstable() {
        let (_dir, db) = open_temp();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        db.flush().unwrap();

        // MemTable 空了
        assert_eq!(db.memtable.read().approx_size(), 0);
        // 有一张 SSTable
        let sst = db.sstables.read();
        assert_eq!(sst.metas.len(), 1);
        assert_eq!(sst.metas[0].entry_count, 2);
        assert_eq!(sst.next_id, 2);
        drop(sst);

        // WAL 空了
        let wal_path = _dir.path().join("wal.log");
        let (records, valid_len) = wal::replay(&wal_path).unwrap();
        assert!(records.is_empty());
        assert_eq!(valid_len, 0);

        // get 仍能读到（走 SSTable 分支，虽然现在 get 还没改，先走 memtable 会返回 None，
        // 这条断言等第 4 步 get 多级读完成后再加）
    }

    #[test]
    fn flush_then_reopen_reads_from_sstable() {
        // 持久化核心测试：写 → flush → drop Db → 重开 → 数据还在。
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"persist", b"yes").unwrap();
            db.flush().unwrap();
            // drop Db 时 WAL 关闭，SSTable 已在磁盘上
        }

        let db = Db::open(dir.path()).unwrap();
        // 数据真的从磁盘上的 SSTable 读回来了。
        assert_eq!(db.get(b"persist").unwrap(), Some(b"yes".to_vec()));

        let sst = db.sstables.read();
        assert_eq!(sst.metas.len(), 1);
        assert_eq!(sst.next_id, 2);
    }

    #[test]
    fn flush_empty_memtable_is_noop() {
        let (_dir, db) = open_temp();
        db.flush().unwrap();
        let sst = db.sstables.read();
        assert!(sst.metas.is_empty());
        assert_eq!(sst.next_id, 1);
    }

    #[test]
    fn put_auto_triggers_flush() {
        let (_dir, db) = open_temp();
        set_threshold(&db, 5); // 很小，写几条就触发

        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k3", b"v3").unwrap();

        // 至少触发过一次 flush，SSTable 列表非空
        let sst = db.sstables.read();
        assert!(!sst.metas.is_empty());
    }

    #[test]
fn get_reads_through_sstables_newest_first() {
    // 造两个 SSTable：旧表 k="old"，新表 k=tombstone。
    // get 必须返回 None（遇墓碑停），不能读到 "old"。
    let dir = tempfile::tempdir().unwrap();

    let p1 = dir.path().join("000001.sst");
    let entries1: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"k"[..], Some(&b"old"[..]))];
    sstable::write_sstable(&p1, entries1.into_iter()).unwrap();

    let p2 = dir.path().join("000002.sst");
    let entries2: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"k"[..], None)];
    sstable::write_sstable(&p2, entries2.into_iter()).unwrap();

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn get_reads_from_older_sstable_if_newer_not_found() {
    // 新表里没有 k，旧表里有 → 应该能读到旧值。
    let dir = tempfile::tempdir().unwrap();

    let p1 = dir.path().join("000001.sst");
    let entries1: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"k"[..], Some(&b"old"[..]))];
    sstable::write_sstable(&p1, entries1.into_iter()).unwrap();

    let p2 = dir.path().join("000002.sst");
    let entries2: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"other"[..], Some(&b"x"[..]))];
    sstable::write_sstable(&p2, entries2.into_iter()).unwrap();

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"old".to_vec()));
}

#[test]
fn memtable_shadows_sstable() {
    // 旧表里 k="old"，memtable 里 k="new" → 读到 "new"。
    let dir = tempfile::tempdir().unwrap();

    let p1 = dir.path().join("000001.sst");
    let entries1: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"k"[..], Some(&b"old"[..]))];
    sstable::write_sstable(&p1, entries1.into_iter()).unwrap();

    let db = Db::open(dir.path()).unwrap();
    db.put(b"k", b"new").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn memtable_tombstone_shadows_sstable() {
    // 旧表里 k="old"，memtable 里 k 是墓碑 → 读到 None。
    let dir = tempfile::tempdir().unwrap();

    let p1 = dir.path().join("000001.sst");
    let entries1: Vec<(&[u8], Option<&[u8]>)> = vec![(&b"k"[..], Some(&b"old"[..]))];
    sstable::write_sstable(&p1, entries1.into_iter()).unwrap();

    let db = Db::open(dir.path()).unwrap();
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn wal_replay_overrides_sstable() {
    // 场景：k 在 SSTable 里是 "old"，WAL 里是 "new"。
    // 重开后必须读到 "new"——WAL 比 SSTable 新。
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();          // k="old" 落到 SSTable，WAL 清空
        db.put(b"k", b"new").unwrap(); // k="new" 进 WAL + memtable
        // 不 flush，直接 drop：SSTable 里是 "old"，WAL 里是 "new"
    }

    let db = Db::open(dir.path()).unwrap();
    // 重开：扫到 SSTable 的 k="old"；重放 WAL 的 k="new" 进 memtable。
    // memtable 覆盖 SSTable → 读到 "new"。
    assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn wal_delete_overrides_sstable() {
    // SSTable 里 k="old"，WAL 里 k 是墓碑。
    // 重开后必须返回 None——墓碑比 SSTable 新。
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap(); // WAL 里写墓碑
    }

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn reopen_with_multiple_sstables() {
    // 造三层数据：SSTable1 k1，SSTable2 k2，WAL k3。
    // 重开后三个都能读到。
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.flush().unwrap();
        db.put(b"k3", b"v3").unwrap();
        // 不 flush，k3 留在 WAL
    }

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));

    // next_id 应该是 3（000001、000002 已存在，下一个是 3）
    let sst = db.sstables.read();
    assert_eq!(sst.next_id, 3);
    assert_eq!(sst.metas.len(), 2);
}

#[test]
fn sstable_id_continues_across_reopen() {
    // 关键回归：两次 open 期间都 flush，第二张表编号不能和第一张撞。
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
        // 落盘 000001.sst
    }
    {
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.sstables.read().next_id, 2);
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        // 应该落盘 000002.sst，不是覆盖 000001
    }

    let db = Db::open(dir.path()).unwrap();
    let sst = db.sstables.read();
    assert_eq!(sst.metas.len(), 2);
    assert_eq!(sst.next_id, 3);
    // 两张表都在
    assert!(dir.path().join("000001.sst").exists());
    assert!(dir.path().join("000002.sst").exists());
}

#[test]
fn compact_merges_overlapping_keys_newest_wins() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    // 造三张表：k 在三张表里值不同，后写的应该赢。
    db.put(b"k", b"v1").unwrap();
    db.put(b"only-old", b"a").unwrap();
    db.flush().unwrap();

    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();

    db.put(b"k", b"v3").unwrap();
    db.put(b"only-new", b"b").unwrap();
    db.flush().unwrap();

    assert_eq!(db.sstables.read().metas.len(), 3);

    db.compact().unwrap();

    // 只剩一张表
    assert_eq!(db.sstables.read().metas.len(), 1);
    // 值正确：新覆盖旧
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(db.get(b"only-old").unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.get(b"only-new").unwrap(), Some(b"b".to_vec()));
}

#[test]
fn compact_drops_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    db.put(b"dead", b"x").unwrap();
    db.put(b"alive", b"keep").unwrap();   // ← 加一条活数据，保证合并结果非空
    db.flush().unwrap();

    db.delete(b"dead").unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // 合并结果非空 → 剩 1 张表
    let sst = db.sstables.read();
    assert_eq!(sst.metas.len(), 1);

    // 墓碑被物理丢弃：新表里不应再有 dead
    let entries = sstable::read_all(&sst.metas[0].path).unwrap();
    assert!(entries.iter().all(|(k, _)| k.as_slice() != b"dead"));
    // 活数据还在
    assert!(entries.iter().any(|(k, v)| k.as_slice() == b"alive" && v.as_deref() == Some(&b"keep"[..])));
    drop(sst);

    assert_eq!(db.get(b"dead").unwrap(), None);
    assert_eq!(db.get(b"alive").unwrap(), Some(b"keep".to_vec()));
}

#[test]
fn compact_all_tombstones_removes_all_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    db.delete(b"a").unwrap();
    db.delete(b"b").unwrap();
    db.flush().unwrap();

    assert_eq!(db.sstables.read().metas.len(), 2);
    db.compact().unwrap();

    // 全墓碑 → 结果为空 → 表清空、文件删光
    let sst = db.sstables.read();
    assert!(sst.metas.is_empty());
    // 目录下不应再有 .sst
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
        .collect();
    assert!(leftovers.is_empty());
}

#[test]
fn compact_then_reopen_data_correct_and_single_table() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"a", b"old").unwrap();
        db.flush().unwrap();
        db.put(b"a", b"new").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
    }

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.sstables.read().metas.len(), 1);
    assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn compact_with_less_than_two_tables_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    // 0 张表
    db.compact().unwrap();
    assert!(db.sstables.read().metas.is_empty());

    // 1 张表
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    let id_before = db.sstables.read().next_id;
    db.compact().unwrap();
    let sst = db.sstables.read();
    assert_eq!(sst.metas.len(), 1);
    assert_eq!(sst.next_id, id_before); // 没写新表
}

#[test]
fn auto_compact_triggers_at_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    // 阈值调很小，让每次 put 都 flush（进而每张表都尝试 compact）
    set_threshold(&db, 1);

    // 写足够多条、每条形如不同 key，制造多张表
    // COMPACTION_THRESHOLD = 4，所以第 4 次 flush 后应触发 compact，表数回落
    for i in 0..8u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }

    // 触发过 compact → 表数不应无限制增长到 8，应 <= 阈值
    let n = db.sstables.read().metas.len();
    assert!(n < 8, "表数应被 compact 压下来，实际 {n}");
    assert!(n <= COMPACTION_THRESHOLD, "表数应 <= 阈值，实际 {n}");

    // 数据全部正确可读（新覆盖旧不误删）
    for i in 0..8u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(b"v".to_vec())
        );
    }
}

#[test]
fn auto_compact_after_delete_keeps_data_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    set_threshold(&db, 1);

    // 先写活数据
    for i in 0..6u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    // 删掉一半，触发大量 flush（每次 delete 一条墓碑表）
    for i in 0..3u32 {
        db.delete(format!("k{i}").as_bytes()).unwrap();
    }

    // 删掉的读到 None，没删的读到 Some —— 墓碑没被错误丢掉
    for i in 0..3u32 {
        assert_eq!(db.get(format!("k{i}").as_bytes()).unwrap(), None, "k{i} 应已删除");
    }
    for i in 3..6u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "k{i} 应仍存在"
        );
    }
}

#[test]
fn compact_advances_next_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();      // 000001
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();      // 000002

    let before = db.sstables.read().next_id; // 应为 3
    db.compact().unwrap();

    let sst = db.sstables.read();
    assert_eq!(sst.metas.len(), 1);
    // compact 用 next_id(3) 写新表 000003，之后 next_id 应为 4
    assert_eq!(sst.next_id, before + 1);
    // 新表文件名就是 000003.sst
    assert_eq!(parse_sst_id(&sst.metas[0].path), Some(before));
}

#[test]
fn compact_empty_result_does_not_advance_next_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.delete(b"a").unwrap();
    db.flush().unwrap();

    let before = db.sstables.read().next_id;
    db.compact().unwrap(); // 全墓碑 → 空结果分支

    let sst = db.sstables.read();
    assert!(sst.metas.is_empty());
    // 空结果没写新表，next_id 不应推进
    assert_eq!(sst.next_id, before);
}

#[test]
fn compact_deletes_old_files_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();      // 000001
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();      // 000002
    db.compact().unwrap();    // 写 000003，删 000001/000002

    // 目录里应只剩新表一个 .sst
    let ssts: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
        .map(|e| e.path())
        .collect();
    assert_eq!(ssts.len(), 1, "旧表应被删光，实际剩 {ssts:?}");
    // 且就是 metas 里登记的那张
    assert_eq!(ssts[0], db.sstables.read().metas[0].path);
}

// ── 阶段④-3：并发正确性 ──

#[test]
fn db_is_send_and_sync() {
    // `&Db` 要能跨线程共享（thread::scope / Arc<Db>），这条在编译期就保证。
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db>();
}

#[test]
fn concurrent_puts_do_not_lose_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    const THREADS: usize = 8;
    const PER_THREAD: usize = 60;

    // thread::scope：直接借用 &db，不需要 Arc，也不需要 'static。
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = &db;
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    let k = format!("t{t}-{i:03}");
                    db.put(k.as_bytes(), b"v").unwrap();
                }
            });
        }
    });

    // 每条写入都必须能读回来：既不能丢，也不能被别的线程覆盖。
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let k = format!("t{t}-{i:03}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()), "{k} 丢了");
        }
    }
}

#[test]
fn concurrent_writes_with_flush_and_compaction() {
    // 阈值调得很小：并发写的同时不断触发 flush 和 compact。
    // 这是三把锁（memtable 写锁 / sstables 写锁 / WAL 锁）交错最密集的路径，
    // 也是"flush 期间读者看到 memtable 已空但新表未登记"这类 bug 的唯一暴露点。
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    set_threshold(&db, 128);

    const THREADS: usize = 4;
    const PER_THREAD: usize = 40;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = &db;
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    let k = format!("t{t}-{i:03}");
                    db.put(k.as_bytes(), b"0123456789").unwrap();
                }
            });
        }
    });

    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let k = format!("t{t}-{i:03}");
            assert_eq!(
                db.get(k.as_bytes()).unwrap(),
                Some(b"0123456789".to_vec()),
                "{k} 丢了"
            );
        }
    }
}

#[test]
fn concurrent_readers_and_writers() {
    // 写线程不断写新 key 并触发 flush，读线程反复读一批固定 key。
    // 断言：读线程每一次都必须读到原值，绝不能读到 None（那说明 flush 的可见性有洞）。
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    set_threshold(&db, 96);

    const FIXED: usize = 20;
    for i in 0..FIXED {
        db.put(format!("fixed-{i:02}").as_bytes(), b"v").unwrap();
    }

    std::thread::scope(|s| {
        for t in 0..2 {
            let db = &db;
            s.spawn(move || {
                for i in 0..40 {
                    db.put(format!("w{t}-{i:03}").as_bytes(), b"x").unwrap();
                }
            });
        }
        for _ in 0..4 {
            let db = &db;
            s.spawn(move || {
                for _ in 0..25 {
                    for i in 0..FIXED {
                        assert_eq!(
                            db.get(format!("fixed-{i:02}").as_bytes()).unwrap(),
                            Some(b"v".to_vec()),
                            "并发 flush 期间读到了错误结果"
                        );
                    }
                }
            });
        }
    });
}

// ── 阶段④-4：批量写 ──

#[test]
fn put_batch_writes_every_pair() {
    let (_dir, db) = open_temp();
    let kvs: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];

    db.put_batch(&kvs).unwrap();

    for (k, v) in &kvs {
        assert_eq!(db.get(k).unwrap(), Some(v.to_vec()), "批量写的 {} 丢了", String::from_utf8_lossy(k));
    }
}

#[test]
fn put_batch_empty_is_noop() {
    let (_dir, db) = open_temp();
    db.put_batch(&[]).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
}

#[test]
fn put_batch_survives_reopen_via_wal() {
    // 不 flush，直接重开：数据只能从 WAL 批写的那段内容里恢复。
    let dir = tempfile::tempdir().unwrap();

    let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("k{i:03}").into_bytes()).collect();
    let vals: Vec<Vec<u8>> = (0..100).map(|i| format!("v{i:03}").into_bytes()).collect();

    {
        let db = Db::open(dir.path()).unwrap();
        let kvs: Vec<(&[u8], &[u8])> =
            keys.iter().zip(&vals).map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        db.put_batch(&kvs).unwrap();
    }

    let db = Db::open(dir.path()).unwrap();
    for (k, v) in keys.iter().zip(&vals) {
        assert_eq!(db.get(k).unwrap(), Some(v.clone()), "重开后 {} 丢了", String::from_utf8_lossy(k));
    }
}
}