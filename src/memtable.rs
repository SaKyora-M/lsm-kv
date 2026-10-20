//! 内存表：有序映射 + 删除墓碑。

use std::collections::BTreeMap;

/// 内存中的有序键值表。
///
/// 值用 `Option<Vec<u8>>`：`Some` 有值，`None` 是墓碑。
/// `BTreeMap` 保证 key 升序，刷盘时可顺序写出 SSTable。
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// 近似字节数，用于判断是否刷盘。
    ///
    /// 覆盖写只加不减：宁可高估早刷盘，也不漏刷。
    approx_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// 写入（覆盖旧值）。
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.approx_size += key.len() + value.len();
        self.map.insert(key.to_vec(), Some(value.to_vec()));
    }

    /// 删除：写入墓碑。
    pub fn delete(&mut self, key: &[u8]) {
        self.approx_size += key.len();
        self.map.insert(key.to_vec(), None);
    }

    /// 读取值。**无法区分墓碑和不存在**，两者都返回 `None`；
    /// 需要区分时用 [`get_raw`](Self::get_raw)。
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.map.get(key) {
            Some(Some(v)) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// 读取三态：`Some(Some(v))` 有值 / `Some(None)` 墓碑 / `None` 不存在。
    pub fn get_raw(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.map.get(key)
    }

    /// 按 key 升序迭代，`value` 为 `None` 表示墓碑。
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        m.put(b"b", b"2");

        assert_eq!(m.get(b"a"), Some(&b"1"[..]));
        assert_eq!(m.get(b"b"), Some(&b"2"[..]));
        assert_eq!(m.get(b"c"), None);

        m.delete(b"a");
        // get 无法区分墓碑与不存在
        assert_eq!(m.get(b"a"), None);
    }

    #[test]
    fn get_raw_three_states() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        m.delete(b"b");

        assert_eq!(m.get_raw(b"a"), Some(&Some(b"1".to_vec())));
        assert_eq!(m.get_raw(b"b"), Some(&None));
        assert_eq!(m.get_raw(b"c"), None);
    }

    #[test]
    fn iter_is_sorted_and_marks_tombstone() {
        let mut m = MemTable::new();
        m.put(b"b", b"2");
        m.put(b"a", b"1");
        m.delete(b"c");

        let items: Vec<(Vec<u8>, Option<Vec<u8>>)> = m
            .iter()
            .map(|(k, v)| (k.to_vec(), v.map(|s| s.to_vec())))
            .collect();

        assert_eq!(
            items,
            vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), Some(b"2".to_vec())),
                (b"c".to_vec(), None),
            ]
        );
    }

    #[test]
    fn approx_size_only_grows() {
        let mut m = MemTable::new();
        assert_eq!(m.approx_size(), 0);

        m.put(b"abc", b"de"); // +5
        assert_eq!(m.approx_size(), 5);

        m.put(b"abc", b"x"); // 覆盖只加不减：+4
        assert_eq!(m.approx_size(), 9);

        m.delete(b"abc"); // +3
        assert_eq!(m.approx_size(), 12);
    }
}
