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

/// SSTable 张数达到该阈值就触发一次全量合并。
pub const COMPACTION_THRESHOLD: usize = 4;

/// SSTable 列表 + 下一个编号。同一把锁保护，保证"push 新表 + 推进编号"是原子的。
struct SstableState {
    /// 已有的 SSTable，从旧到新排列（**末尾 = 最新**）。
    metas: Vec<SstableMeta>,
    /// 下一个要分配的 SSTable 编号。
    next_id: u64,
}

/// 存储引擎入口，组装 MemTable / WAL / SSTable。
///
/// **并发模型**：`memtable` / `sstables` 用 `RwLock`（读共享、写排他），
/// `wal` 用 `Mutex`。`get` 的两把读锁不嵌套，读读之间完全并行。
///
/// **锁顺序**：memtable → sstables → wal（前缀原则）。各方法持锁：
/// - `flush`：memtable(w) → sstables(w) → wal
/// - `compact`：memtable(w) → sstables(w)
/// - `put` / `delete` / `put_batch`：memtable(w) → wal
/// - `get`：memtable(r) 释放后 → sstables(r)
///
/// `parking_lot` 的锁**不可重入**（持读锁再拿写锁同样死锁），
/// 所以 `flush` / `compact` 返回后必须先释放干净再嵌套调用。
pub struct Db {
    /// 数据目录，SSTable 和 WAL 都在这里。
    dir: PathBuf,
    memtable: RwLock<MemTable>,
    wal: Mutex<Wal>,
    sstables: RwLock<SstableState>,
    /// 刷盘阈值，默认 `FLUSH_THRESHOLD`。
    ///
    /// 用原子类型是因为 `put` 等方法只有 `&self`，而测试/基准要动态调小它。
    flush_threshold: AtomicUsize,
}

impl Db {
    /// 打开（或创建）`path` 目录下的引擎实例。
    ///
    /// 启动时扫描已有 SSTable 重建列表，再重放 WAL（比 SSTable 新，会覆盖旧值）；
    /// 崩溃残留的半截 WAL 记录会被截断丢弃。
    pub fn open(path: impl AsRef<Path>) -> Result<Db> {
        let dir = path.as_ref();

        // 确保目录存在（已存在则什么都不做）
        std::fs::create_dir_all(dir)?;

        // 扫描已有 SSTable，重建列表并推导下一个编号
        let metas = Self::scan_sstables(dir)?;
        let next_id = metas
            .last()
            .and_then(|m| parse_sst_id(&m.path))
            .map(|id| id + 1)
            .unwrap_or(1);

        let wal_path = dir.join("wal.log");

        // 1) 重放 WAL；文件不存在 = 首次启动
        let (records, valid_len) = match wal::replay(&wal_path) {
            Ok(x) => x,
            // 只有"文件不存在"当空日志处理，其它 IO 错误照常上抛
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (Vec::new(), 0),
            Err(e) => return Err(e),
        };

        // 截掉崩溃残留的半截数据，后续追加写才不会接在坏数据后面
        let actual_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        if actual_len > valid_len {
            let f = std::fs::OpenOptions::new().write(true).open(&wal_path)?;
            f.set_len(valid_len)?; // set_len 把文件截到指定长度
        }

        // 3) 用重放出的记录重建内存表
        let mut memtable = MemTable::new();
        for record in records {
            match record {
                WalRecord::Put { key, value } => memtable.put(&key, &value),
                WalRecord::Delete { key } => memtable.delete(&key),
            }
        }

        // 4) 打开 WAL 供后续追加
        let wal = Wal::open(&wal_path)?;

        Ok(Db {
            dir: dir.to_path_buf(),
            memtable: RwLock::new(memtable),
            wal: Mutex::new(wal),
            sstables: RwLock::new(SstableState { metas, next_id }),
            flush_threshold: AtomicUsize::new(FLUSH_THRESHOLD),
        })
    }

    /// 写入（覆盖旧值）。
    ///
    /// **顺序不能反**：先 append WAL，成功后再改内存表，这样中间崩溃也能恢复。
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // memtable 写锁包住"写 WAL + 改内存"两步，flush 拿不到这把锁就插不进来
        let size = {
            let mut mt = self.memtable.write();
            self.wal.lock().put(key, value)?;
            mt.put(key, value);
            mt.approx_size()
        };
        // 块结束时 mt 已释放，再调 flush 不会自锁
        if size >= self.flush_threshold.load(Ordering::Relaxed) {
            self.flush()?;
            if let Err(e) = self.maybe_compact() {
                eprintln!("warning: compaction failed: {e}");
            }
        }
        Ok(())
    }

    /// 批量写入：整批共用一次 WAL 追加和一把锁，适合日志回放、批量导入。
    ///
    /// 单条 `put` 的成本几乎全在 `write_all` 系统调用上，批量写把 N 次压成 1 次。
    /// 原子性与 `put` 相同；崩溃语义是"前缀持久"（每条记录自带长度和 CRC）。
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

    /// 读取一个键，不存在或已删除都返回 `None`。
    ///
    /// 顺序：MemTable → SSTable（从新到旧）。**遇到墓碑必须停**，
    /// 继续往更旧的表找会读到已被删除的旧值。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 先查 MemTable；用 get_raw 才能区分墓碑和未命中
        {
            let mt = self.memtable.read();
            match mt.get_raw(key) {
                Some(Some(v)) => return Ok(Some(v.clone())),
                Some(None) => return Ok(None), // 墓碑：最新状态就是已删
                None => {}                     // 未命中：继续往 SSTable 查
            }
        }

        // 从新到旧遍历 SSTable（metas 旧在前，所以 rev）
        let metas = self.sstables.read();
        for meta in metas.metas.iter().rev() {
            match sstable::get(&meta.path, key)? {
                Lookup::Found(v) => return Ok(Some(v)),
                Lookup::Tombstone => return Ok(None), // 墓碑：停
                Lookup::NotFound => continue,         // 继续往更旧查
            }
        }

        Ok(None)
    }

    /// 删除：写墓碑，不物理删除。同样先写 WAL 再改内存表。
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
    /// **顺序不能变**：① 写新表 → ② 截断 WAL → ③ 清空 MemTable → ④ 登记新表。
    /// 任一时刻崩溃，最坏只是白刷一次或重放一遍，结果都正确；
    /// IO 失败提前返回且不动内存状态，所以这个操作是幂等的。
    ///
    /// **读可见性**：memtable 写锁持有到函数结束，局部变量逆序析构使
    /// `sstables` 锁先释放、`memtable` 后释放。否则读者可能看到
    /// "memtable 已清空、新表还没登记"，静默返回 `None`。
    pub fn flush(&self) -> Result<()> {
        let mut memtable = self.memtable.write();

        // 空表不刷，避免产生空 SSTable 文件
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

    /// 全量合并所有 SSTable。
    ///
    /// **为什么能丢墓碑**：所有表合成一张后，底下不再有更老的数据，墓碑就可以物理丢弃。
    /// 若改成 leveled / 只合并部分表，墓碑就不能随便丢。
    ///
    /// **崩溃安全**：① 写新表 → ② 改内存 → ③ 删旧文件。任一时刻崩溃，
    /// 最坏只留下冗余旧文件（读时按 id 新覆盖旧），不会丢数据。
    ///
    /// 持 memtable 锁是为了挡住并发 flush，保证合并期间 `metas` 不被改。
    pub fn compact(&self) -> Result<()> {
        // 先拿 memtable 锁挡住并发 flush，再拿 sstables 锁（与 flush 顺序一致）
        let _memtable = self.memtable.write();
        let mut sst = self.sstables.write();

        // 少于 2 张表没什么可合并的。
        if sst.metas.len() < 2 {
            return Ok(());
        }

        // 从旧到新读进同一个 BTreeMap：新表后 insert，天然实现"新版本优先"
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for meta in &sst.metas {
            for (key, value) in sstable::read_all(&meta.path)? {
                merged.insert(key, value);
            }
        }

        // 全量合并后不再有更老的数据，墓碑可以安全丢弃。
        merged.retain(|_, v| v.is_some());

        // 先记下输入文件路径，最后删
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

        // ① 成功之后才动内存：metas 换成 [新表]，推进编号
        sst.metas = vec![meta];
        sst.next_id = id + 1;
        drop(sst); // 尽早释放 sstables 锁，删文件不再需要它

        // ③ 最后删旧文件。删失败不影响正确性（重开时按 id 新覆盖旧），但仍上报
        for p in &old_paths {
            match std::fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        // _memtable 在函数结束时释放
        Ok(())
    }

    /// 表数达到阈值就触发一次 compact。
    ///
    /// 必须在 `flush()` 返回之后调用，且先放掉 `sstables` 读锁再调 `compact()`——
    /// 锁不可重入，持锁嵌套调用会死锁。
    fn maybe_compact(&self) -> Result<()> {
        let should = {
            let sst = self.sstables.read();
            sst.metas.len() >= COMPACTION_THRESHOLD
        }; // ← sstables 锁释放
        if should {
            self.compact()?; // ← compact 自己重新拿锁
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

        // memtable 已空，读必须走 SSTable 分支
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
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
            db.flush().unwrap(); // k="old" 落到 SSTable，WAL 清空
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
        db.put(b"alive", b"keep").unwrap(); // ← 加一条活数据，保证合并结果非空
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
        assert!(
            entries
                .iter()
                .any(|(k, v)| k.as_slice() == b"alive" && v.as_deref() == Some(&b"keep"[..]))
        );
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
            assert_eq!(
                db.get(format!("k{i}").as_bytes()).unwrap(),
                None,
                "k{i} 应已删除"
            );
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
        db.flush().unwrap(); // 000001
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap(); // 000002

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
        db.flush().unwrap(); // 000001
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap(); // 000002
        db.compact().unwrap(); // 写 000003，删 000001/000002

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
                assert_eq!(
                    db.get(k.as_bytes()).unwrap(),
                    Some(b"v".to_vec()),
                    "{k} 丢了"
                );
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
            assert_eq!(
                db.get(k).unwrap(),
                Some(v.to_vec()),
                "批量写的 {} 丢了",
                String::from_utf8_lossy(k)
            );
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
            let kvs: Vec<(&[u8], &[u8])> = keys
                .iter()
                .zip(&vals)
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            db.put_batch(&kvs).unwrap();
        }

        let db = Db::open(dir.path()).unwrap();
        for (k, v) in keys.iter().zip(&vals) {
            assert_eq!(
                db.get(k).unwrap(),
                Some(v.clone()),
                "重开后 {} 丢了",
                String::from_utf8_lossy(k)
            );
        }
    }
}
