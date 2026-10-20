//! 端到端集成测试：只通过公开 API 验证整个引擎的行为。
//!
//! 这些测试从"外部用户"的视角使用 `lsm_kv::Db`，
//! 覆盖阶段①最核心的两个承诺：数据持久化、崩溃恢复。

use lsm_kv::Db;

/// 在临时目录里开一个 Db，返回 (目录, Db)。
/// 目录必须持有到测试结束，否则临时目录被删。
fn open_db() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn persistence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // 第一次打开：写入 + 删除，然后 drop（模拟进程退出、文件关闭）
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"foo", b"bar").unwrap();
        db.put(b"num", b"42").unwrap();
        db.delete(b"foo").unwrap();
        // db 离开作用域，文件被关闭
    }

    // 第二次打开：数据应从 WAL 重放回来
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"num").unwrap(), Some(b"42".to_vec())); // put 持久化了
    assert_eq!(db.get(b"foo").unwrap(), None);                  // delete 也持久化了
}

#[test]
fn recovery_ignores_truncated_tail() {
    let dir = tempfile::tempdir().unwrap();

    // 先正常写两条，然后关闭
    {
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
    }

    // 模拟崩溃：把 wal.log 最后 3 个字节砍掉（第二条记录写了一半）
    let wal_path = dir.path().join("wal.log");
    let data = std::fs::read(&wal_path).unwrap();
    std::fs::write(&wal_path, &data[..data.len() - 3]).unwrap();

    // 重新打开：第一条恢复，第二条的半截被干净丢弃
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2").unwrap(), None);
}

#[test]
fn overwrite_and_delete_semantics() {
    let (_dir, db) = open_db();

    db.put(b"key", b"1").unwrap();
    db.put(b"key", b"2").unwrap(); // 覆盖
    assert_eq!(db.get(b"key").unwrap(), Some(b"2".to_vec()));

    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);

    // 删除后再写入，应能正常读回（验证 tombstone 不会卡死后续写入）
    db.put(b"key", b"3").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn keys_and_values_are_raw_bytes() {
    let (_dir, db) = open_db();

    // 故意用非 UTF-8 的字节（0xff、0x00），证明键值不限定字符串
    let key = [0xff, 0x00, 0x01];
    let value = [0xde, 0xad, 0xbe, 0xef];

    db.put(&key, &value).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value.to_vec()));
}