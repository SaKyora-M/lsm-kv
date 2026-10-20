//! 引擎统一的错误类型与 `Result` 别名。

use std::fmt;

/// 所有公开 API 返回的错误。
///
/// 新增错误种类时直接往枚举里加变体，不影响已有调用方。
#[derive(Debug)]
pub enum Error {
    /// 底层 I/O 错误。
    Io(std::io::Error),
    /// 数据损坏：CRC 校验失败、记录被截断等。
    Corruption(String),
}

/// `Result<T>` 等价于 `std::result::Result<T, Error>`。
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(msg) => write!(f, "data corruption: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Corruption(_) => None,
        }
    }
}

/// 让 `?` 自动把 `std::io::Error` 转成 `Error`。
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inner() -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ))
    }

    #[test]
    fn io_error_auto_converts_via_question_mark() {
        fn outer() -> Result<()> {
            inner()?;
            Ok(())
        }

        assert!(matches!(outer().unwrap_err(), Error::Io(_)));
    }
}
