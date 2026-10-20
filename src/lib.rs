//! 一个用 Rust 从零实现的迷你 LSM-Tree 键值存储引擎。
//!
//! 对外只暴露 [`Db`]，以及配套的错误类型 [`Error`] / [`Result`]。
//! 存储格式见 `file/SSTable格式设计.md`。

mod db;
mod error;
mod memtable;
mod sstable;
mod wal;

pub use db::Db;
pub use error::{Error, Result};
