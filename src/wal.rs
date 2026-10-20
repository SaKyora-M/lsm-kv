//! WAL（预写日志）记录的二进制编码与解码。
//!
//! 记录格式见文件末尾的长图注释，或看 encode/decode 函数。

use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::Write; // write_all 是 Write trait 的方法，必须引入 trait
use std::path::{Path, PathBuf};

/// 操作类型：写入一个键值对。
pub const OP_PUT: u8 = 1;
/// 操作类型：删除一个键。
pub const OP_DELETE: u8 = 2;

/// 解码后的逻辑记录。
///
/// `PartialEq` 让我们能直接用 `==` / `assert_eq!` 比较两条记录，
/// 测试里很方便。
#[derive(Debug, PartialEq)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// 把一条逻辑记录编码成磁盘上的字节序列。
///
/// 布局：
///   [payload_len u32 LE][crc32 u32 LE][op u8][key_len u32 LE][key][value_len u32 LE][value]
pub fn encode(op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    // 第一步：拼出 payload（op + key_len + key + value_len + value）
    let mut payload = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    payload.push(op);
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    // 第二步：对 payload 算 CRC32
    let crc = crc32fast::hash(&payload);

    // 第三步：拼出完整记录 = 长度 + CRC + payload
    let mut record = Vec::with_capacity(8 + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&payload);
    record
}

/// 从字节切片解码出一条记录。
///
/// `buf` 必须恰好是"一条完整记录"（长度 + CRC + payload 都齐全），
/// 不能多也不能少。长度不对、CRC 不符都返回 `Corruption` 错误。
pub fn decode(buf: &[u8]) -> Result<WalRecord> {
    // 一条记录至少有 8 字节（长度 4 + CRC 4）
    if buf.len() < 8 {
        return Err(Error::Corruption("record too short".into()));
    }

    let payload_len = read_u32(&buf[0..4]) as usize;
    let expected_crc = read_u32(&buf[4..8]);

    // 长度字段必须和实际剩余字节对得上
    if buf.len() != 8 + payload_len {
        return Err(Error::Corruption("record length mismatch".into()));
    }

    let payload = &buf[8..];

    // 重算 CRC，和存的对比，不一致说明数据被改坏了
    if crc32fast::hash(payload) != expected_crc {
        return Err(Error::Corruption("CRC mismatch".into()));
    }

    decode_payload(payload)
}

/// 从 payload（不含长度和 CRC）解析出逻辑记录。
fn decode_payload(payload: &[u8]) -> Result<WalRecord> {
    if payload.len() < 5 {
        return Err(Error::Corruption("payload too short".into()));
    }

    let op = payload[0];
    let key_len = read_u32(&payload[1..5]) as usize;

    // 定位 value_len 字段：它紧跟在 key 后面
    let value_len_offset = 5 + key_len;
    if payload.len() < value_len_offset + 4 {
        return Err(Error::Corruption("truncated key".into()));
    }
    let value_len = read_u32(&payload[value_len_offset..value_len_offset + 4]) as usize;

    // 定位 value，并校验整个 payload 的长度严格一致
    let value_start = value_len_offset + 4;
    if payload.len() != value_start + value_len {
        return Err(Error::Corruption("truncated value".into()));
    }

    let key = payload[5..5 + key_len].to_vec();
    let value = payload[value_start..value_start + value_len].to_vec();

    match op {
        OP_PUT => Ok(WalRecord::Put { key, value }),
        OP_DELETE => Ok(WalRecord::Delete { key }),
        other => Err(Error::Corruption(format!("unknown op code: {other}"))),
    }
}

/// 从字节切片的前 4 个字节读出一个 u32（小端序）。
///
/// `bytes[0..4]` 是长度不定的切片 `[u8]`，而 `from_le_bytes` 需要
/// 定长数组 `[u8; 4]`，所以用 `try_into()` 转换；`.unwrap()` 是因为
/// 所有调用处都已经先检查过"至少有 4 字节"。
fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
}

/// 追加写的 WAL 文件句柄。
pub struct Wal {
    /// WAL 文件路径。`truncate` 需要重开文件时用。
    path: PathBuf,
    file: File,
}

impl Wal {
    /// 打开（不存在则创建）一个 WAL 文件，用于追加写。
    pub fn open(path: &Path) -> Result<Wal> {
        let file = OpenOptions::new()
            .create(true)  // 不存在就新建
            .append(true)  // 写入永远追加到末尾（WAL 的关键）
            .open(path)?;  // ? 把 io::Error 自动转成我们的 Error
        Ok(Wal {
            path: path.to_path_buf(),
            file,
        })
    }

    /// 追加一条 Put 记录。
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let record = encode(OP_PUT, key, value);
        self.file.write_all(&record)?; // write_all 保证整条写完整
        Ok(())
    }

    /// 追加一条 Delete 记录（value 为空）。
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let record = encode(OP_DELETE, key, b"");
        self.file.write_all(&record)?;
        Ok(())
    }

    /// 批量追加 Put 记录：全部编码进一个缓冲区，**一次** `write_all` 落盘。
    ///
    /// 逐条 `put` 是"一条记录一次系统调用"，而单条写入的成本几乎全耗在这个
    /// 系统调用上（本机实测 ~6µs）。批量写把 N 次压成 1 次，
    /// 单条摊销成本能降一个数量级。
    ///
    /// 崩溃语义是"**前缀持久**"：可能只写下去一部分，但每条记录都自带
    /// "长度 + CRC"，`replay` 会在残缺处停下并截断（见 `replay`），
    /// 不会把半条记录当成有效数据读出来。
    pub fn put_batch(&mut self, kvs: &[(&[u8], &[u8])]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for (key, value) in kvs {
            buf.extend_from_slice(&encode(OP_PUT, key, value));
        }
        self.file.write_all(&buf)?;
        Ok(())
    }

    /// 把 WAL 截断到 0 字节，用于刷盘后清空日志。
    ///
    /// Windows 上以 `append(true)` 打开的文件不允许 `set_len(0)`
    /// （`FILE_APPEND_DATA` 和 `SetEndOfFile` 需要的 `FILE_WRITE_DATA`
    /// 不兼容），所以这里不用 `set_len`，而是：
    /// 1. 先把旧句柄 sync 到磁盘（保证之前写入的已落盘）
    /// 2. 以 `write(true)` 模式重开，`set_len(0)` 截断
    /// 3. 再以 `append(true)` 模式重开，替换 `self.file`
    pub fn truncate(&mut self) -> Result<()> {
        // 1) 旧文件刷盘：保证之前的写入已经落到磁盘，
        //    重开文件后不会读到"还在 OS 缓冲里"的旧记录。
        self.file.sync_all()?;

        // 2) 以 write(true) 重开并截断。
        //    作用域块让临时句柄 f 立刻 drop，避免和 self.file 同时
        //    持有文件（Windows 上通常允许共享，但显式 drop 更干净）。
        {
            let f = OpenOptions::new().write(true).open(&self.path)?;
            f.set_len(0)?;
            f.sync_all()?;
        }

        // 3) 重新以 append(true) 打开，替换旧句柄。
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        Ok(())
    }
}

/// 重放一个 WAL 文件，返回 `(完整有效的记录列表, 有效数据长度)`。
///
/// "有效数据长度"是最后一条完好记录的末尾位置。崩溃残留的半截数据
/// 会被丢弃，调用方（Db）据此截断文件、清掉残尾。
pub fn replay(path: &Path) -> Result<(Vec<WalRecord>, u64)> {
    let data = std::fs::read(path)?; // 整文件读进内存

    let mut records = Vec::new();
    let mut offset = 0usize;

    while offset < data.len() {
        // 连 8 字节（长度 + CRC）都不够 → 残尾，停止
        if data.len() - offset < 8 {
            break;
        }
        let payload_len = read_u32(&data[offset..offset + 4]) as usize;
        let total_len = 8 + payload_len;

        // 剩余字节凑不满一条完整记录 → 被截断，停止
        if data.len() - offset < total_len {
            break;
        }

        // 切出"恰好一条记录"，交给 decode 做 CRC 校验和解析
        let record_bytes = &data[offset..offset + total_len];
        match decode(record_bytes) {
            Ok(record) => {
                records.push(record);
                offset += total_len;
            }
            // decode 失败（CRC 不符等）→ 这条及之后都不可信，当作坏尾部停止
            Err(_) => break,
        }
    }

    Ok((records, offset as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_put() {
        let bytes = encode(OP_PUT, b"hello", b"world");
        let decoded = decode(&bytes).unwrap();
        assert_eq!(
            decoded,
            WalRecord::Put {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
            }
        );
    }

    #[test]
    fn roundtrip_delete() {
        let bytes = encode(OP_DELETE, b"gone", b"");
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, WalRecord::Delete { key: b"gone".to_vec() });
    }

    #[test]
    fn crc_detects_corruption() {
        let mut bytes = encode(OP_PUT, b"k", b"v");
        // 翻转最后一个字节（它在 payload 的 value 区里），
        // `^= 0xff` 是"按位异或"，把每一位取反。
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(matches!(decode(&bytes), Err(Error::Corruption(_))));
    }

    #[test]
    fn decode_rejects_truncated() {
        let bytes = encode(OP_PUT, b"k", b"v");
        // 砍掉最后一个字节，长度就对不上了
        let truncated = &bytes[..bytes.len() - 1];
        assert!(matches!(decode(truncated), Err(Error::Corruption(_))));
    }

        #[test]
    fn append_then_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        // 内层块：让 wal 在 replay 之前被 drop（关闭文件），模拟真实重启
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.put(b"k1", b"v1").unwrap();
            wal.put(b"k2", b"v2").unwrap();
            wal.delete(b"k1").unwrap();
        }

        let (records, _) = replay(&path).unwrap();
        assert_eq!(
            records,
            vec![
                WalRecord::Put { key: b"k1".to_vec(), value: b"v1".to_vec() },
                WalRecord::Put { key: b"k2".to_vec(), value: b"v2".to_vec() },
                WalRecord::Delete { key: b"k1".to_vec() },
            ]
        );
    }

    #[test]
    fn replay_stops_at_truncated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut wal = Wal::open(&path).unwrap();
        wal.put(b"k1", b"v1").unwrap();
        wal.put(b"k2", b"v2").unwrap();
        drop(wal); // 关闭文件

        // 砍掉最后 3 个字节，模拟"第二条记录写了一半就崩溃"
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 3]).unwrap();

        let (records, valid_len) = replay(&path).unwrap();
        assert_eq!(records.len(), 1); // 只恢复了第一条完整的
        assert_eq!(
            records[0],
            WalRecord::Put { key: b"k1".to_vec(), value: b"v1".to_vec() }
        );
        // 有效长度 = 第一条记录的长度
        assert_eq!(valid_len, encode(OP_PUT, b"k1", b"v1").len() as u64);
    }

    #[test]
    fn truncate_clears_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.put(b"k1", b"v1").unwrap();
            wal.put(b"k2", b"v2").unwrap();
            wal.truncate().unwrap();
            wal.put(b"k3", b"v3").unwrap();
        }

        let (records, _) = replay(&path).unwrap();
        assert_eq!(
            records,
            vec![WalRecord::Put { key: b"k3".to_vec(), value: b"v3".to_vec() }]
        );
    }
}