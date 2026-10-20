//! WAL（预写日志）：记录编码、追加写与重放。

use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// 操作类型：写入一个键值对。
pub const OP_PUT: u8 = 1;
/// 操作类型：删除一个键。
pub const OP_DELETE: u8 = 2;

/// 解码后的逻辑记录。
#[derive(Debug, PartialEq)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// 把一条逻辑记录编码成磁盘上的字节序列。
///
/// 布局：`[payload_len u32][crc32 u32][op u8][key_len u32][key][value_len u32][value]`
/// CRC 只覆盖 payload，不含长度字段和 CRC 自身。
pub fn encode(op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    payload.push(op);
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    let crc = crc32fast::hash(&payload);

    let mut record = Vec::with_capacity(8 + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&payload);
    record
}

/// 从字节切片解码出一条记录。
///
/// `buf` 必须恰好是一条完整记录，多一个字节少一个字节都报 `Corruption`。
pub fn decode(buf: &[u8]) -> Result<WalRecord> {
    if buf.len() < 8 {
        return Err(Error::Corruption("record too short".into()));
    }

    let payload_len = read_u32(&buf[0..4]) as usize;
    let expected_crc = read_u32(&buf[4..8]);

    if buf.len() != 8 + payload_len {
        return Err(Error::Corruption("record length mismatch".into()));
    }

    let payload = &buf[8..];
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

    let value_len_offset = 5 + key_len;
    if payload.len() < value_len_offset + 4 {
        return Err(Error::Corruption("truncated key".into()));
    }
    let value_len = read_u32(&payload[value_len_offset..value_len_offset + 4]) as usize;

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

/// 从切片前 4 字节读一个小端 u32。调用方保证至少 4 字节。
fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
}

/// 追加写的 WAL 文件句柄。
pub struct Wal {
    /// 文件路径，`truncate` 重开文件时要用。
    path: PathBuf,
    file: File,
}

impl Wal {
    /// 打开（不存在则创建）一个 WAL 文件。
    pub fn open(path: &Path) -> Result<Wal> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            file,
        })
    }

    /// 追加一条 Put 记录。
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let record = encode(OP_PUT, key, value);
        self.file.write_all(&record)?;
        Ok(())
    }

    /// 追加一条 Delete 记录（value 为空）。
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let record = encode(OP_DELETE, key, b"");
        self.file.write_all(&record)?;
        Ok(())
    }

    /// 批量追加 Put 记录：整批编码进一个缓冲区，一次 `write_all` 落盘。
    ///
    /// 单条写入的成本几乎全在系统调用上，批量写把 N 次压成 1 次。
    /// 崩溃语义是"前缀持久"：每条记录仍自带长度和 CRC，`replay` 会在残缺处停下。
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
    /// （`FILE_APPEND_DATA` 与 `SetEndOfFile` 需要的 `FILE_WRITE_DATA` 不兼容），
    /// 所以这里先 sync，再以 `write(true)` 重开截断，最后以 `append(true)` 重开。
    pub fn truncate(&mut self) -> Result<()> {
        self.file.sync_all()?;

        {
            let f = OpenOptions::new().write(true).open(&self.path)?;
            f.set_len(0)?;
            f.sync_all()?;
        }

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        Ok(())
    }
}

/// 重放一个 WAL 文件，返回 `(完整有效的记录列表, 有效数据长度)`。
///
/// 有效数据长度是最后一条完好记录的末尾位置；崩溃残留的半截数据被丢弃，
/// 调用方据此截断文件。
pub fn replay(path: &Path) -> Result<(Vec<WalRecord>, u64)> {
    let data = std::fs::read(path)?;

    let mut records = Vec::new();
    let mut offset = 0usize;

    while offset < data.len() {
        // 连长度 + CRC 都不够，或剩余字节凑不满一条完整记录 → 残尾
        if data.len() - offset < 8 {
            break;
        }
        let payload_len = read_u32(&data[offset..offset + 4]) as usize;
        let total_len = 8 + payload_len;
        if data.len() - offset < total_len {
            break;
        }

        // CRC 不符说明这条及之后都不可信，当作坏尾部
        let record_bytes = &data[offset..offset + total_len];
        match decode(record_bytes) {
            Ok(record) => {
                records.push(record);
                offset += total_len;
            }
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
        assert_eq!(
            decode(&bytes).unwrap(),
            WalRecord::Put {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
            }
        );
    }

    #[test]
    fn roundtrip_delete() {
        let bytes = encode(OP_DELETE, b"gone", b"");
        assert_eq!(
            decode(&bytes).unwrap(),
            WalRecord::Delete {
                key: b"gone".to_vec()
            }
        );
    }

    #[test]
    fn crc_detects_corruption() {
        let mut bytes = encode(OP_PUT, b"k", b"v");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(matches!(decode(&bytes), Err(Error::Corruption(_))));
    }

    #[test]
    fn decode_rejects_truncated() {
        let bytes = encode(OP_PUT, b"k", b"v");
        let truncated = &bytes[..bytes.len() - 1];
        assert!(matches!(decode(truncated), Err(Error::Corruption(_))));
    }

    #[test]
    fn append_then_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

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
                WalRecord::Put {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec()
                },
                WalRecord::Put {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec()
                },
                WalRecord::Delete {
                    key: b"k1".to_vec()
                },
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
        drop(wal);

        // 砍掉最后 3 字节，模拟"第二条写了一半就崩溃"
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 3]).unwrap();

        let (records, valid_len) = replay(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            WalRecord::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec()
            }
        );
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
            vec![WalRecord::Put {
                key: b"k3".to_vec(),
                value: b"v3".to_vec()
            }]
        );
    }
}
