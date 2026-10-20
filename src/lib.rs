//! mini-lsm：一个用 Rust 实现的迷你 LSM-Tree 键值存储引擎。
//!
//! 阶段①目标：内存 KV + WAL，程序重启后数据不丢。
//!
//! 对外只暴露一个入口 [`Db`]，以及配套的错误类型 [`Error`] / [`Result`]。

mod db;
mod error;
mod memtable;
mod sstable;
mod wal;

pub use db::Db;
pub use error::{Error, Result};