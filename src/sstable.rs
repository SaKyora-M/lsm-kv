//! SSTable 的写入与读取。文件格式见 `file/SSTable格式设计.md`（当前 v3）。

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// 稀疏索引间隔 N：每隔 N 条数据记一个索引项（对应格式文档 §5.1）。
pub const SPARSE_INDEX_INTERVAL: usize = 16;

/// footer 魔数（4 字节标签，v3）
const MAGIC: &[u8; 4] = b"LSM3";
/// footer 固定字节数：4×u64 + 2×u32 + 4 字节 magic
const FOOTER_SIZE: usize = 44;
/// 每条 entry 末尾 CRC32 的字节数
const ENTRY_CRC_SIZE: usize = 4;

/// Bloom：每个 key 分配的位数。
pub const BITS_PER_KEY: usize = 10;

/// Bloom：每个 key 使用的哈希次数（理论最优 ≈ ln2 × BITS_PER_KEY ≈ 6.9，取 7）。
pub const BLOOM_HASHES: usize = 7;

/// entry 的 flags 字节：bit0 = tombstone（其余位当前保留为 0）。
pub const FLAG_TOMBSTONE: u8 = 0b0000_0001;

/// 写入过程中在内存里攒的稀疏索引项。
struct IndexEntry {
    key: Vec<u8>,
    offset: u64, // 该 entry 在文件中的绝对字节偏移
}

/// 一个 SSTable 文件的元信息（刷盘后返回给 db.rs，用于维护 SSTable 列表）。
#[derive(Debug, Clone)]
pub struct SstableMeta {
    pub path: PathBuf,
    pub min_key: Vec<u8>, // 文件覆盖的最小 key
    pub max_key: Vec<u8>, // 文件覆盖的最大 key
    pub entry_count: u32, // 数据区条数（统计/调试用）
}

/// 单表查找的三态结果。
///
/// `NotFound` 要继续往更老的表找，`Tombstone` 可以直接断定"已删除"。
#[derive(Debug, PartialEq)]
pub enum Lookup {
    Found(Vec<u8>),
    Tombstone,
    NotFound,
}

// ── 小端读取辅助 ──

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[0..8].try_into().unwrap())
}

// ── 按需读取辅助 ──

/// 从文件 `pos` 处读 `len` 字节。只读需要的区间，不整文件读入。
fn read_at(file: &mut File, pos: u64, len: usize) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(pos))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

// ── Bloom Filter ──

/// 64 位 FNV-1a 哈希。
///
/// Bloom 用两个不同基各算一次得到 h1/h2，再用双哈希 `(h1 + i*h2) % m`
/// 扩展出 k 个位置（Kirsch-Mitzenmacher）。
fn fnv1a64(key: &[u8], basis: u64) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = basis;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// 计算 Bloom 位图需要的位数：`max(64, n * BITS_PER_KEY)`，向上对齐到 8 的倍数。
///
/// 下限 64：n 很小时 `n*10` 太少，7 次哈希会挤在极少数位上，假阳性率反而飙升。
fn bloom_m_bits(n: usize) -> usize {
    // 必须对齐到 8 的倍数：读侧用 `bits.len()*8` 反推模数，两边不一致会让
    // 同一个 key 算出不同位下标 → 假阴性 → 丢数据。
    let raw = std::cmp::max(64, n.saturating_mul(BITS_PER_KEY));
    raw.div_ceil(8) * 8
}

/// 根据全部 key 构建 Bloom 位图（裸字节数组，`ceil(m_bits/8)` 字节）。
///
/// **无假阴性保证**：每个 key 的 `BLOOM_HASHES` 个位都置 1，所以文件里存在的
/// key 一定被判"可能存在"。假阳性（不存在的 key 偶尔被判可能存在）是允许的。
///
/// `keys` 只借用不复制；调用方在写完数据区后已持有 key 切片。
pub fn bloom_build<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    // 先收集一遍拿到 n（Iterator 只能消费一次，而我们需要 n 来定位位下标）。
    let keys: Vec<&[u8]> = keys.collect();
    let m_bits = bloom_m_bits(keys.len());
    let m_bytes = m_bits.div_ceil(8);
    let mut bits = vec![0u8; m_bytes];

    for key in keys {
        let h1 = fnv1a64(key, 0xcbf2_9ce4_8422_2325);
        // h2 置奇数：避免 h2 与 m_bits 有公因子导致 k 个落点退化。
        let h2 = fnv1a64(key, 0x9e37_79b9_7f4a_7c15) | 1;
        for i in 0..BLOOM_HASHES as u64 {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2)) % m_bits as u64) as usize;
            bits[pos / 8] |= 1 << (pos % 8); // LSB 优先
        }
    }
    bits
}

/// 判断 key **是否可能**在位图里。
///
/// `false` 一定不存在，可安全跳过整张表；`true` 只是可能存在（有假阳性）。
/// **空位图返回 `true`**：表示"本文件没有 Bloom"，不能当成不存在，否则会丢数据。
pub fn bloom_may_contain(bits: &[u8], key: &[u8]) -> bool {
    if bits.is_empty() {
        return true; // 没有 Bloom 信息 → 保守地"可能存在"
    }
    let m_bits = bits.len() * 8;
    let h1 = fnv1a64(key, 0xcbf2_9ce4_8422_2325);
    let h2 = fnv1a64(key, 0x9e37_79b9_7f4a_7c15) | 1;
    for i in 0..BLOOM_HASHES as u64 {
        let pos = (h1.wrapping_add(i.wrapping_mul(h2)) % m_bits as u64) as usize;
        if bits[pos / 8] & (1 << (pos % 8)) == 0 {
            return false; // 任一位为 0 → 一定不存在
        }
    }
    true
}

/// 文件末尾的 44 字节 footer（小端）。
///
/// `index_offset(0..8) | index_len(8..16) | bloom_offset(16..24) | bloom_len(24..32)
/// | index_crc32(32..36) | bloom_crc32(36..40) | magic(40..44)`
///
/// footer 自身没有 CRC（会循环依赖），由 magic + 区间校验兜底。
#[derive(Debug, Clone, Copy)]
struct Footer {
    index_offset: usize,
    index_len: usize,
    bloom_offset: usize,
    bloom_len: usize,
    index_crc: u32,
    bloom_crc: u32,
}

// ── footer / 索引解析 ──

/// 解析 footer，只做"取末尾 44 字节 + 校验 magic + 拼字段"。
///
/// 入参可以是整个文件，也可以恰好是 `FOOTER_SIZE` 字节。
/// 区间校验交给 `validate_ranges`（只传 footer 的调用方还拿不到文件总长）。
fn parse_footer(data: &[u8]) -> Result<Footer> {
    if data.len() < FOOTER_SIZE {
        return Err(Error::Corruption("sstable too small for footer".into()));
    }
    let footer = &data[data.len() - FOOTER_SIZE..];

    // magic：4 字节标签，直接按字节比较，不涉及端序（见格式文档 §5.7）
    if &footer[40..44] != MAGIC {
        return Err(Error::Corruption("bad sstable magic".into()));
    }

    // 小端读取辅助：把 [u8; N] 拼成整数
    let read_u64_at = |off: usize| -> usize {
        let mut b = [0u8; 8];
        b.copy_from_slice(&footer[off..off + 8]);
        u64::from_le_bytes(b) as usize
    };
    let read_u32_at = |off: usize| -> u32 {
        let mut b = [0u8; 4];
        b.copy_from_slice(&footer[off..off + 4]);
        u32::from_le_bytes(b)
    };

    Ok(Footer {
        index_offset: read_u64_at(0),
        index_len: read_u64_at(8),
        bloom_offset: read_u64_at(16),
        bloom_len: read_u64_at(24),
        index_crc: read_u32_at(32),
        bloom_crc: read_u32_at(36),
    })
}

/// 校验三个区间都在 `[0, file_len - FOOTER_SIZE)` 内，且索引区在 Bloom 区之前。
///
/// `file_len` 必须是文件总长；用带溢出检查的加法，避免回绕绕过上界判断。
fn validate_ranges(file_len: usize, footer: &Footer) -> Result<()> {
    let data_end = file_len
        .checked_sub(FOOTER_SIZE)
        .ok_or_else(|| Error::Corruption("sstable too small".into()))?;

    let index_end = footer
        .index_offset
        .checked_add(footer.index_len)
        .ok_or_else(|| Error::Corruption("index range overflow".into()))?;
    if index_end > data_end {
        return Err(Error::Corruption("index out of range".into()));
    }

    let bloom_end = footer
        .bloom_offset
        .checked_add(footer.bloom_len)
        .ok_or_else(|| Error::Corruption("bloom range overflow".into()))?;
    if bloom_end > data_end {
        return Err(Error::Corruption("bloom out of range".into()));
    }

    // Bloom 区必须在索引区之后；否则坏文件能把数据字节当位图读，产生假阴性。
    if footer.bloom_len > 0 && footer.bloom_offset < index_end {
        return Err(Error::Corruption("bloom overlaps index".into()));
    }

    Ok(())
}

/// 校验一段区间的 CRC32（索引区 / Bloom 区各一段）。
///
/// `what` 只用于错误信息，形如 `"index"` 或 `"bloom"`。
fn check_section_crc(bytes: &[u8], expected: u32, what: &str) -> Result<()> {
    let actual = crc32fast::hash(bytes);
    if actual != expected {
        return Err(Error::Corruption(format!(
            "{what} checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
        )));
    }
    Ok(())
}

/// 解析索引区，返回 `(entry_count, Vec<(key, offset)>)`。
///
/// `index_data` 是已按 footer 切好、且通过 CRC 校验的索引段。
fn parse_index(index_data: &[u8]) -> Result<(usize, Vec<(Vec<u8>, u64)>)> {
    if index_data.len() < 8 {
        return Err(Error::Corruption("index too short".into()));
    }
    let entry_count = read_u32(&index_data[0..4]) as usize;
    let index_count = read_u32(&index_data[4..8]) as usize;

    let mut index: Vec<(Vec<u8>, u64)> = Vec::with_capacity(index_count);
    let mut pos = 8usize;
    for _ in 0..index_count {
        if pos + 4 > index_data.len() {
            return Err(Error::Corruption("index entry truncated".into()));
        }
        let key_len = read_u32(&index_data[pos..pos + 4]) as usize;
        pos += 4;
        if pos + key_len + 8 > index_data.len() {
            return Err(Error::Corruption("index entry truncated".into()));
        }
        let k = index_data[pos..pos + key_len].to_vec();
        pos += key_len;
        let off = read_u64(&index_data[pos..pos + 8]);
        pos += 8;
        index.push((k, off));
    }

    Ok((entry_count, index))
}

// ── 编码 ──

/// 把一条逻辑记录编码成 entry 字节。
///
/// 布局：`[key_len u32][key][flags u8][value_len u32][value][crc32 u32]`
/// CRC 覆盖从 `key_len` 到 `value` 的全部字节，不含 CRC 自身。
fn encode_entry(key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let flags = if value.is_some() { 0 } else { FLAG_TOMBSTONE };
    let value_len = value.map_or(0, |v| v.len());

    let mut bytes = Vec::with_capacity(4 + key.len() + 1 + 4 + value_len + ENTRY_CRC_SIZE);
    bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(key);
    bytes.push(flags);
    bytes.extend_from_slice(&(value_len as u32).to_le_bytes());
    if let Some(v) = value {
        bytes.extend_from_slice(v);
    }

    // 先算 CRC 再追加，保证覆盖范围就是"这之前写下的全部字节"
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

// ── 写入 ──

/// 把一组**按 key 升序**的 entry 序列化成 v3 SSTable 文件。
///
/// 布局：`[数据区][索引区][Bloom区][Footer 44B]`，各段都带 CRC32。
/// `value` 为 `None` 表示 tombstone；调用方保证 `entries` 非空。
pub fn write_sstable<'a>(
    path: impl AsRef<Path>,
    entries: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
) -> Result<SstableMeta> {
    let path = path.as_ref();
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    let mut offset: u64 = 0;
    let mut entry_count: usize = 0;
    let mut index_buf: Vec<IndexEntry> = Vec::new();
    // 只存 key 引用（不复制字节），供循环后构建 Bloom 与 min/max key
    let mut keys: Vec<&'a [u8]> = Vec::new();
    let mut min_key: Option<&'a [u8]> = None;
    let mut max_key: Option<&'a [u8]> = None;

    for (key, value) in entries {
        if entry_count % SPARSE_INDEX_INTERVAL == 0 {
            index_buf.push(IndexEntry {
                key: key.to_vec(),
                offset,
            });
        }
        if min_key.is_none() {
            min_key = Some(key);
        }
        max_key = Some(key);
        keys.push(key);

        let bytes = encode_entry(key, value);
        writer.write_all(&bytes)?;
        offset += bytes.len() as u64;
        entry_count += 1;
    }

    // 数据区写完，索引区从这里开始
    let index_offset = offset;

    // 索引区先在内存里拼好，再一次性写
    let mut index_block = Vec::new();
    index_block.extend_from_slice(&(entry_count as u32).to_le_bytes());
    index_block.extend_from_slice(&(index_buf.len() as u32).to_le_bytes());
    for e in &index_buf {
        index_block.extend_from_slice(&(e.key.len() as u32).to_le_bytes());
        index_block.extend_from_slice(&e.key);
        index_block.extend_from_slice(&e.offset.to_le_bytes());
    }
    let index_len = index_block.len() as u64;
    let index_crc = crc32fast::hash(&index_block);
    writer.write_all(&index_block)?;

    // Bloom 区：用收集到的 key 引用构建位图，写索引区之后
    let bloom_offset = index_offset + index_len;
    let bloom_bytes = bloom_build(keys.into_iter());
    let bloom_len = bloom_bytes.len() as u64;
    let bloom_crc = crc32fast::hash(&bloom_bytes);
    writer.write_all(&bloom_bytes)?;

    // Footer：index_offset | index_len | bloom_offset | bloom_len
    //        | index_crc32 | bloom_crc32 | MAGIC
    let mut footer = Vec::with_capacity(FOOTER_SIZE);
    footer.extend_from_slice(&index_offset.to_le_bytes());
    footer.extend_from_slice(&index_len.to_le_bytes());
    footer.extend_from_slice(&bloom_offset.to_le_bytes());
    footer.extend_from_slice(&bloom_len.to_le_bytes());
    footer.extend_from_slice(&index_crc.to_le_bytes());
    footer.extend_from_slice(&bloom_crc.to_le_bytes());
    footer.extend_from_slice(MAGIC);
    debug_assert_eq!(footer.len(), FOOTER_SIZE);
    writer.write_all(&footer)?;

    // 显式 flush：BufWriter 在 drop 时也会 flush，但失败会被吞掉
    writer.flush()?;

    Ok(SstableMeta {
        path: path.to_path_buf(),
        min_key: min_key.expect("刷盘的表非空（写路径保证 ≥1 条）").to_vec(),
        max_key: max_key.expect("刷盘的表非空（写路径保证 ≥1 条）").to_vec(),
        entry_count: entry_count as u32,
    })
}

// ── 读取 ──

/// 在单个 SSTable 里查找 key。
///
/// 顺序：footer → Bloom（**先校验 CRC 再用**）→ 索引 → 命中窗口。
/// Bloom 判定不存在时直接返回，不读索引和数据区；
/// 只读需要的那几段，总字节数与文件大小无关。
///
/// 必须先校验 Bloom 再使用：坏位图会造成假阴性，等于静默丢数据。
pub fn get(path: &Path, key: &[u8]) -> Result<Lookup> {
    let mut file = File::open(path)?;

    let file_len = file.metadata()?.len() as usize;
    if file_len < FOOTER_SIZE {
        return Err(Error::Corruption("sstable too small".into()));
    }

    // footer：magic + 四个区间字段 + 两段 CRC
    let footer_buf = read_at(&mut file, (file_len - FOOTER_SIZE) as u64, FOOTER_SIZE)?;
    let footer = parse_footer(&footer_buf)?;
    validate_ranges(file_len, &footer)?;

    // Bloom：先校验 CRC 再做判断
    if footer.bloom_len > 0 {
        let bloom = read_at(&mut file, footer.bloom_offset as u64, footer.bloom_len)?;
        check_section_crc(&bloom, footer.bloom_crc, "bloom")?;

        if !bloom_may_contain(&bloom, key) {
            // 键一定不在本文件，数据区一个字节都不用读
            return Ok(Lookup::NotFound);
        }
    }

    // 索引段：校验 CRC 后解析，二分找"最后一个 key ≤ 目标"的索引项
    let index_raw = read_at(&mut file, footer.index_offset as u64, footer.index_len)?;
    check_section_crc(&index_raw, footer.index_crc, "index")?;
    let (_entry_count, index) = parse_index(&index_raw)?;

    if index.is_empty() {
        return Ok(Lookup::NotFound);
    }
    let i = index.partition_point(|(k, _)| k.as_slice() <= key);
    if i == 0 {
        return Ok(Lookup::NotFound);
    }
    let (_, offset) = index[i - 1];

    // 只读该索引项覆盖的窗口，最多 SPARSE_INDEX_INTERVAL 条 entry；
    // 越界说明索引项本身坏了
    if offset as usize >= footer.index_offset {
        return Err(Error::Corruption(format!(
            "index offset {offset} out of data range (index_offset={})",
            footer.index_offset
        )));
    }
    let window = read_at(
        &mut file,
        offset,
        (footer.index_offset as u64 - offset) as usize,
    )?;

    // 窗口内顺序扫，每条 entry 的 CRC 由 decode_entry 校验
    let mut pos = 0usize;
    while pos < window.len() {
        let (k, flags, v, next) = decode_entry(&window, pos, offset as usize)?;
        match k.as_slice().cmp(key) {
            Ordering::Equal => {
                if flags & FLAG_TOMBSTONE != 0 {
                    return Ok(Lookup::Tombstone);
                }
                return Ok(Lookup::Found(v));
            }
            Ordering::Greater => return Ok(Lookup::NotFound),
            Ordering::Less => pos = next as usize,
        }
    }

    Ok(Lookup::NotFound)
}

/// 整表读出：按 key 升序返回全部条目，`None` 表示墓碑。
///
/// 不走索引，从偏移 0 顺序扫到数据区末尾，因此能拿到**每一条** entry。
/// **墓碑原样返回，不做过滤**——能不能丢由上层（全量 compaction）决定。
///
/// 主要供 `Db::compact` 使用。
pub fn read_all(path: &Path) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let data = std::fs::read(path)?;

    let footer = parse_footer(&data)?;
    validate_ranges(data.len(), &footer)?;

    // 全量读也要先把索引区和 Bloom 区校验一遍，坏文件不能被当成正常文件归并
    let index_raw = &data[footer.index_offset..footer.index_offset + footer.index_len];
    let bloom_raw = &data[footer.bloom_offset..footer.bloom_offset + footer.bloom_len];
    check_section_crc(index_raw, footer.index_crc, "index")?;
    check_section_crc(bloom_raw, footer.bloom_crc, "bloom")?;

    // 扫描边界是 index_offset（Bloom 区在索引区之后，不能被当成 entry）
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < footer.index_offset {
        let (key, flags, value, next) = decode_entry(&data, pos, 0)?;
        let val = if flags & FLAG_TOMBSTONE != 0 {
            None
        } else {
            Some(value)
        };
        out.push((key, val));
        pos = next as usize;
    }
    Ok(out)
}

/// 读取 SSTable 元信息，不加载全部数据。
///
/// `min_key` 取第一个索引项；`max_key` 从最后一个索引项往后扫到最后一条。
/// 用于 `Db::open` 重建表列表（旧表在前、新表在后）。
pub fn load_meta(path: impl AsRef<Path>) -> Result<SstableMeta> {
    let path = path.as_ref().to_path_buf();
    let data = std::fs::read(&path)?;

    let footer = parse_footer(&data)?;
    validate_ranges(data.len(), &footer)?;

    // 索引区先过 CRC32：坏索引会让下面的 parse_index 读出错误的条目
    let index_raw = &data[footer.index_offset..footer.index_offset + footer.index_len];
    check_section_crc(index_raw, footer.index_crc, "index")?;
    let (entry_count, index) = parse_index(index_raw)?;

    if index.is_empty() {
        return Err(Error::Corruption(format!(
            "sstable {path:?} has empty sparse index"
        )));
    }

    let min_key = index[0].0.clone();

    let last = index.last().unwrap();
    let mut max_key = last.0.clone();
    let mut offset = last.1 as usize;
    while offset < footer.index_offset {
        let (k, _flags, _v, next) = decode_entry(&data, offset, 0)?;
        max_key = k;
        offset = next as usize;
    }

    Ok(SstableMeta {
        path,
        min_key,
        max_key,
        entry_count: entry_count as u32,
    })
}

/// 从 `data[pos..]` 解析一条 entry，返回 `(key, flags, value, 下一条偏移)`。
///
/// `base` 是 `data[0]` 对应的文件绝对偏移，**只用于错误信息**：
/// `get` 只把"命中的那一段窗口"读进内存，窗口内偏移要加上 `base`
/// 才是文件里的真实位置，否则报出来的偏移没法定位。
/// 返回值"下一条偏移"与 `pos` 使用同一套坐标系（窗口内相对 / 全文件绝对）。
///
/// 解析完 value 后必须校验末尾的 CRC32：不一致返回 `Corruption`。
fn decode_entry(data: &[u8], pos: usize, base: usize) -> Result<(Vec<u8>, u8, Vec<u8>, u64)> {
    let abs = base + pos;
    if pos + 4 > data.len() {
        return Err(Error::Corruption(format!(
            "entry truncated at offset {abs}"
        )));
    }
    let key_len = read_u32(&data[pos..pos + 4]) as usize;
    let mut p = pos + 4;
    if p + key_len + 1 + 4 + ENTRY_CRC_SIZE > data.len() {
        return Err(Error::Corruption(format!(
            "entry truncated at offset {abs}"
        )));
    }
    let key = data[p..p + key_len].to_vec();
    p += key_len;
    let flags = data[p];
    p += 1;
    let value_len = read_u32(&data[p..p + 4]) as usize;
    p += 4;
    if p + value_len + ENTRY_CRC_SIZE > data.len() {
        return Err(Error::Corruption(format!(
            "entry truncated at offset {abs}"
        )));
    }
    let value = data[p..p + value_len].to_vec();
    p += value_len;

    // 校验 entry 级 CRC32：覆盖 [pos, p)，不含 crc 自己
    let stored = read_u32(&data[p..p + ENTRY_CRC_SIZE]);
    let actual = crc32fast::hash(&data[pos..p]);
    if stored != actual {
        return Err(Error::Corruption(format!(
            "entry checksum mismatch at offset {abs}: stored {stored:#010x}, actual {actual:#010x}"
        )));
    }
    p += ENTRY_CRC_SIZE;

    Ok((key, flags, value, p as u64))
}

// ── 单元测试 ──

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_entry_put() {
        let bytes = encode_entry(b"a", Some(b"x"));
        // key_len=1, 'a', flags=0, value_len=1, 'x'，末尾 4 字节是 crc32
        assert_eq!(&bytes[..11], &[1, 0, 0, 0, b'a', 0, 1, 0, 0, 0, b'x']);
        assert_eq!(bytes.len(), 11 + ENTRY_CRC_SIZE);
        assert_eq!(read_u32(&bytes[11..]), crc32fast::hash(&bytes[..11]));
    }

    #[test]
    fn encode_entry_empty_value() {
        let bytes = encode_entry(b"k", Some(b""));
        // key_len=1, 'k', flags=0, value_len=0
        assert_eq!(&bytes[..10], &[1, 0, 0, 0, b'k', 0, 0, 0, 0, 0]);
        assert_eq!(bytes.len(), 10 + ENTRY_CRC_SIZE);
        assert_eq!(read_u32(&bytes[10..]), crc32fast::hash(&bytes[..10]));
    }

    #[test]
    fn encode_entry_tombstone() {
        let bytes = encode_entry(b"k", None);
        // key_len=1, 'k', flags=1, value_len=0
        assert_eq!(&bytes[..10], &[1, 0, 0, 0, b'k', 1, 0, 0, 0, 0]);
        assert_eq!(bytes.len(), 10 + ENTRY_CRC_SIZE);
        assert_eq!(read_u32(&bytes[10..]), crc32fast::hash(&bytes[..10]));
    }

    #[test]
    fn write_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (&b"a"[..], Some(&b"1"[..])),
            (&b"b"[..], None),
            (&b"c"[..], Some(&b"3"[..])),
            (&b"d"[..], Some(&b""[..])), // 空值，必须和 tombstone 区分开
        ];
        let meta = write_sstable(&path, entries.into_iter()).unwrap();

        assert_eq!(meta.entry_count, 4);
        assert_eq!(meta.min_key, b"a".to_vec());
        assert_eq!(meta.max_key, b"d".to_vec());

        assert_eq!(get(&path, b"a").unwrap(), Lookup::Found(b"1".to_vec()));
        assert_eq!(get(&path, b"b").unwrap(), Lookup::Tombstone);
        assert_eq!(get(&path, b"c").unwrap(), Lookup::Found(b"3".to_vec()));
        assert_eq!(get(&path, b"d").unwrap(), Lookup::Found(b"".to_vec()));
        assert_eq!(get(&path, b"zzz").unwrap(), Lookup::NotFound);
        assert_eq!(get(&path, b"").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn sparse_index_crosses_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        // 40 条数据 > N=16，跨多个索引项；最后追加一条删除标记
        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..40u32)
            .map(|i| {
                (
                    format!("key-{i:02}").into_bytes(),
                    Some(i.to_string().into_bytes()),
                )
            })
            .collect();
        entries.push((b"zzz".to_vec(), None));

        let borrowed: Vec<(&[u8], Option<&[u8]>)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        write_sstable(&path, borrowed.into_iter()).unwrap();

        assert_eq!(get(&path, b"key-00").unwrap(), Lookup::Found(b"0".to_vec()));
        assert_eq!(
            get(&path, b"key-17").unwrap(),
            Lookup::Found(b"17".to_vec())
        );
        assert_eq!(
            get(&path, b"key-39").unwrap(),
            Lookup::Found(b"39".to_vec())
        );
        assert_eq!(get(&path, b"zzz").unwrap(), Lookup::Tombstone);
        assert_eq!(get(&path, b"aaa").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn footer_bytes_are_correct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries = vec![(&b"k1"[..], Some(&b"v1"[..]))];
        write_sstable(&path, entries.into_iter()).unwrap();

        let data = std::fs::read(&path).unwrap();
        let len = data.len();

        // 末尾 4 字节是 MAGIC
        assert_eq!(&data[len - 4..], MAGIC);

        // footer 44 字节，字段顺序：
        // index_offset | index_len | bloom_offset | bloom_len | index_crc32 | bloom_crc32 | magic
        let index_offset = read_u64(&data[len - 44..len - 36]);
        let index_len = read_u64(&data[len - 36..len - 28]);
        let bloom_offset = read_u64(&data[len - 28..len - 20]);
        let bloom_len = read_u64(&data[len - 20..len - 12]);
        let index_crc = read_u32(&data[len - 12..len - 8]);
        let bloom_crc = read_u32(&data[len - 8..len - 4]);

        // index_offset = 唯一那条 entry 的字节数（含末尾 4 字节 crc）
        let expected_entry_len = encode_entry(b"k1", Some(b"v1")).len() as u64;
        assert_eq!(index_offset, expected_entry_len);

        // 三段紧邻：bloom_offset = index_offset + index_len
        assert_eq!(bloom_offset, index_offset + index_len);
        // Bloom 大小：1 条 key → m_bits = max(64, 10) → 64 位 → 8 字节
        assert_eq!(bloom_len, 8);
        // footer 之前的合法数据区恰好是数据+索引+Bloom
        assert_eq!(
            index_offset + index_len + bloom_len,
            (len - FOOTER_SIZE) as u64
        );

        // 两个段级 CRC 必须与磁盘上的实际字节一致
        let (io, il) = (index_offset as usize, index_len as usize);
        let (bo, bl) = (bloom_offset as usize, bloom_len as usize);
        assert_eq!(index_crc, crc32fast::hash(&data[io..io + il]));
        assert_eq!(bloom_crc, crc32fast::hash(&data[bo..bo + bl]));
    }

    #[test]
    fn load_meta_roundtrip_small() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (&b"a"[..], Some(&b"1"[..])),
            (&b"b"[..], None),
            (&b"c"[..], Some(&b"3"[..])),
        ];
        let written = write_sstable(&path, entries.into_iter()).unwrap();

        let loaded = load_meta(&path).unwrap();
        assert_eq!(loaded.path, path);
        assert_eq!(loaded.min_key, b"a".to_vec());
        assert_eq!(loaded.max_key, b"c".to_vec());
        assert_eq!(loaded.entry_count, written.entry_count);
    }

    #[test]
    fn load_meta_crosses_index_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        // 40 条 > SPARSE_INDEX_INTERVAL，max_key 必须从最后一个索引项往后扫
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..40u32)
            .map(|i| {
                (
                    format!("key-{i:02}").into_bytes(),
                    Some(i.to_string().into_bytes()),
                )
            })
            .collect();
        let borrowed: Vec<(&[u8], Option<&[u8]>)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        write_sstable(&path, borrowed.into_iter()).unwrap();

        let loaded = load_meta(&path).unwrap();
        assert_eq!(loaded.min_key, b"key-00".to_vec());
        assert_eq!(loaded.max_key, b"key-39".to_vec());
        assert_eq!(loaded.entry_count, 40);
    }

    #[test]
    fn load_meta_rejects_too_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.sst");
        std::fs::write(&path, b"too small").unwrap();

        assert!(load_meta(&path).is_err());
    }

    #[test]
    fn read_all_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (&b"a"[..], Some(&b"1"[..])),
            (&b"b"[..], None), // 墓碑必须原样带出
            (&b"c"[..], Some(&b"3"[..])),
        ];
        write_sstable(&path, entries.into_iter()).unwrap();

        let all = read_all(&path).unwrap();
        assert_eq!(
            all,
            vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), None),
                (b"c".to_vec(), Some(b"3".to_vec())),
            ]
        );
    }

    #[test]
    fn read_all_crosses_sparse_index_blocks() {
        // 40 条 > N=16，确保跨多个索引段也能全量读出（不依赖索引二分）
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..40u32)
            .map(|i| {
                (
                    format!("key-{i:02}").into_bytes(),
                    Some(i.to_string().into_bytes()),
                )
            })
            .collect();
        entries.push((b"zzz".to_vec(), None));

        let borrowed: Vec<(&[u8], Option<&[u8]>)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        write_sstable(&path, borrowed.into_iter()).unwrap();

        let all = read_all(&path).unwrap();
        assert_eq!(all.len(), 41);
        assert_eq!(all[0].0, b"key-00".to_vec());
        assert_eq!(all[39].0, b"key-39".to_vec());
        assert_eq!(all[40], (b"zzz".to_vec(), None));
    }

    #[test]
    fn bloom_no_false_negative() {
        // 硬要求：写进去的 key，查它必然 true。
        let keys: Vec<Vec<u8>> = (0..500u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let bits = bloom_build(keys.iter().map(|k| k.as_slice()));

        for k in &keys {
            assert!(
                bloom_may_contain(&bits, k),
                "假阴性！key {:?} 被误判不存在",
                String::from_utf8_lossy(k)
            );
        }
    }

    #[test]
    fn bloom_false_positive_rate_acceptable() {
        // 查 1000 个不存在的 key，误判率应远低于 10%（理论 ~0.8%）。
        let present: Vec<Vec<u8>> = (0..500u32)
            .map(|i| format!("present-{i}").into_bytes())
            .collect();
        let bits = bloom_build(present.iter().map(|k| k.as_slice()));

        let mut fp = 0;
        let n_absent = 1000u32;
        for i in 0..n_absent {
            let k = format!("absent-{i}").into_bytes();
            if bloom_may_contain(&bits, &k) {
                fp += 1;
            }
        }
        let rate = fp as f64 / n_absent as f64;
        assert!(rate < 0.10, "假阳性率 {rate:.3} 过高");
    }

    #[test]
    fn bloom_empty_bitmap_returns_true() {
        // 没有 Bloom 信息时保守返回 true，绝不能返回 false（否则丢数据）。
        assert!(bloom_may_contain(&[], b"anything"));
    }

    #[test]
    fn bloom_single_key() {
        let bits = bloom_build(std::iter::once(&b"only"[..]));
        // m_bits 下限 64 位 = 8 字节
        assert_eq!(bits.len(), 8);
        assert!(bloom_may_contain(&bits, b"only"));
    }

    #[test]
    fn bloom_m_bits_is_byte_aligned() {
        // 读侧用 bits.len()*8 反推模数，写侧 m_bits 必须是 8 的倍数，
        // 否则两边取模不一致会假阴性。
        for n in [0usize, 1, 5, 100, 513, 4097] {
            let m = bloom_m_bits(n);
            assert_eq!(m % 8, 0, "n={n} 时 m_bits={m} 未字节对齐");
            assert!(m >= 64);
            assert!(m >= n.saturating_mul(BITS_PER_KEY));
        }
    }

    #[test]
    fn get_v3_found_tombstone_notfound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (&b"a"[..], Some(&b"1"[..])),
            (&b"b"[..], None),           // 墓碑
            (&b"c"[..], Some(&b""[..])), // 空值，必须和墓碑区分
            (&b"d"[..], Some(&b"4"[..])),
        ];
        write_sstable(&path, entries.into_iter()).unwrap();

        assert_eq!(get(&path, b"a").unwrap(), Lookup::Found(b"1".to_vec()));
        assert_eq!(get(&path, b"b").unwrap(), Lookup::Tombstone);
        assert_eq!(get(&path, b"c").unwrap(), Lookup::Found(b"".to_vec())); // 空值 ≠ 墓碑
        assert_eq!(get(&path, b"d").unwrap(), Lookup::Found(b"4".to_vec()));
        assert_eq!(get(&path, b"absent").unwrap(), Lookup::NotFound); // 走 Bloom miss
        assert_eq!(get(&path, b"").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn get_v3_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        write_sstable(&path, std::iter::once((&b"only"[..], Some(&b"v"[..])))).unwrap();
        assert_eq!(get(&path, b"only").unwrap(), Lookup::Found(b"v".to_vec()));
        assert_eq!(get(&path, b"nope").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn get_v3_crosses_sparse_index_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..40u32)
            .map(|i| {
                (
                    format!("key-{i:02}").into_bytes(),
                    Some(i.to_string().into_bytes()),
                )
            })
            .collect();
        let borrowed: Vec<(&[u8], Option<&[u8]>)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        write_sstable(&path, borrowed.into_iter()).unwrap();

        assert_eq!(get(&path, b"key-00").unwrap(), Lookup::Found(b"0".to_vec()));
        assert_eq!(
            get(&path, b"key-17").unwrap(),
            Lookup::Found(b"17".to_vec())
        );
        assert_eq!(
            get(&path, b"key-39").unwrap(),
            Lookup::Found(b"39".to_vec())
        );
        assert_eq!(get(&path, b"aaa").unwrap(), Lookup::NotFound); // 小于 min
        assert_eq!(get(&path, b"zzz").unwrap(), Lookup::NotFound); // 大于 max
    }

    #[test]
    fn corrupt_magic_rejected() {
        // magic 被改坏（或旧版本文件）→ 报 Corruption，不静默误读。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.sst");
        write_sstable(&path, std::iter::once((&b"k"[..], Some(&b"v"[..])))).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let len = data.len();
        data[len - 4..].copy_from_slice(b"XXXX"); // 破坏 magic
        std::fs::write(&path, &data).unwrap();

        assert!(matches!(get(&path, b"k"), Err(Error::Corruption(_))));
    }

    #[test]
    fn get_miss_returns_notfound() {
        // miss 路径返回 NotFound（不 panic）。
        // 严格证明"只读了 footer+Bloom"需要注入 I/O 计数，这里靠代码顺序保证
        // （std::fs::read 在 Bloom 判定之后才发生）。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let owned: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..100u32)
            .map(|i| (format!("k{i}").into_bytes(), Some(b"v".to_vec())))
            .collect();
        let borrowed: Vec<(&[u8], Option<&[u8]>)> = owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        write_sstable(&path, borrowed.into_iter()).unwrap();

        assert_eq!(get(&path, b"definitely-absent").unwrap(), Lookup::NotFound);
    }

    // ── v3 校验和：篡改测试 ──

    #[test]
    fn entry_checksum_detects_field_corruption() {
        // 对第一条 entry（key="a", value="1"）的每个字段各改一个字节，
        // get 和 read_all 都必须报 Corruption，绝不能返回错值或静默 NotFound。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> =
            vec![(&b"a"[..], Some(&b"1"[..])), (&b"b"[..], Some(&b"2"[..]))];
        write_sstable(&path, entries.into_iter()).unwrap();
        let pristine = std::fs::read(&path).unwrap();

        // 第一条 entry 的字节布局：
        // [0..4] key_len | [4..5] key | [5] flags | [6..10] value_len | [10..11] value | [11..15] crc
        let cases = [
            ("key", 4usize),
            ("flags", 5),
            ("value_len", 6),
            ("value", 10),
            ("crc", 11),
        ];

        for (what, off) in cases {
            let mut data = pristine.clone();
            data[off] ^= 0xFF; // 翻转一个字节
            std::fs::write(&path, &data).unwrap();

            assert!(
                matches!(get(&path, b"a"), Err(Error::Corruption(_))),
                "改坏 {what}（偏移 {off}）后 get 应报 Corruption"
            );
            assert!(
                matches!(read_all(&path), Err(Error::Corruption(_))),
                "改坏 {what}（偏移 {off}）后 read_all 应报 Corruption"
            );
        }
    }

    #[test]
    fn bloom_checksum_detects_corruption() {
        // 坏 Bloom 最危险：它可能造成假阴性，把存在的 key 判成"不存在"。
        // 有了段级 CRC，这种情况必须变成显式 Corruption。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> =
            vec![(&b"a"[..], Some(&b"1"[..])), (&b"b"[..], Some(&b"2"[..]))];
        write_sstable(&path, entries.into_iter()).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let footer = parse_footer(&data).unwrap();
        data[footer.bloom_offset] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        assert!(matches!(get(&path, b"a"), Err(Error::Corruption(_))));
    }

    #[test]
    fn index_checksum_detects_corruption() {
        // 索引区坏掉会让二分指向错误位置，必须被 CRC 挡住。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        let entries: Vec<(&[u8], Option<&[u8]>)> =
            vec![(&b"a"[..], Some(&b"1"[..])), (&b"b"[..], Some(&b"2"[..]))];
        write_sstable(&path, entries.into_iter()).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let footer = parse_footer(&data).unwrap();
        data[footer.index_offset] ^= 0xFF; // 改坏索引区开头（entry_count）
        std::fs::write(&path, &data).unwrap();

        assert!(matches!(get(&path, b"a"), Err(Error::Corruption(_))));
        assert!(matches!(load_meta(&path), Err(Error::Corruption(_))));
    }

    #[test]
    fn truncated_file_rejected_by_all_read_paths() {
        // 截断文件：footer 都不完整，三个入口都必须报错而不是 panic。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        write_sstable(&path, std::iter::once((&b"k"[..], Some(&b"v"[..])))).unwrap();
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() / 2]).unwrap();

        assert!(get(&path, b"k").is_err());
        assert!(read_all(&path).is_err());
        assert!(load_meta(&path).is_err());
    }
}
