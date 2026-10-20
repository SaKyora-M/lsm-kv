//! 引擎统一的错误类型与 `Result` 别名。
//!
//! 全项目所有返回 `Result` 的函数都用这里的类型，
//! 避免每个模块各定义一套错误。

use std::fmt;

/// `mini-lsm` 所有公开 API 返回的错误类型。
///
/// 阶段①只有两类错误：底层 I/O 错误、数据损坏。
/// 后续阶段（SSTable 解码、Compaction）需要新错误时，
/// 直接往这个枚举里加变体即可，不用改现有调用方。
#[derive(Debug)]
pub enum Error {
    /// 底层文件/系统 I/O 错误（打开失败、读写失败、路径不存在等）。
    ///
    /// 变体里"包着"一个真实的 `std::io::Error`，保留原始错误细节。
    Io(std::io::Error),

    /// 数据损坏：例如 WAL 记录的 CRC 校验不通过、记录被截断。
    ///
    /// 用 `String` 存一段描述，方便定位是哪条记录坏了。
    Corruption(String),
}

/// 统一的 `Result` 别名：`Result<T>` 等价于 `std::result::Result<T, Error>`。
///
/// 这是个"类型别名"，不是新类型，纯粹是为了少打字、保持一致。
pub type Result<T> = std::result::Result<T, Error>;

// ── 下面三个 impl 让 Error 成为一个"合格"的错误类型 ──

/// 控制 `{}` 打印出来长什么样（给人看的话术）。
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `{e}` 会把里面的 io::Error 用它自己的 Display 格式化出来
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(msg) => write!(f, "data corruption: {msg}"),
        }
    }
}

/// 标记这是标准错误类型，并通过 `source()` 暴露"底层原因"。
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Io 里包着真实的 io::Error，把它作为底层原因返回，
            // 这样错误链可以一层层往下追。
            Error::Io(e) => Some(e),
            // Corruption 没有底层错误，返回 None 表示"这就是最底层"。
            Error::Corruption(_) => None,
        }
    }
}

/// 关键实现：让 `?` 运算符能自动把 `std::io::Error` 转成我们的 `Error`。
///
/// 有了它，以后任何返回 `Result<T>`（我们的别名）的函数里，
/// 都可以直接写 `File::open(p)?`，编译器会自动做这个转换。
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

// ── 单元测试 ──
#[cfg(test)]
mod tests {
    use super::*; // 引入父模块里的 Error、Result 等

    /// 一个返回标准 io::Result 的函数，模拟真实的文件操作失败。
    fn inner() -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ))
    }

    #[test]
    fn io_error_auto_converts_via_question_mark() {
        // outer 返回我们的 Result 别名；内部调用 inner()?，
        // 靠 From<io::Error> 自动把 io::Error 转成 Error。
        fn outer() -> Result<()> {
            inner()?;
            Ok(())
        }

        let err = outer().unwrap_err();
        // matches!(值, 模式) 判断值是否匹配某个模式，返回 bool。
        assert!(matches!(err, Error::Io(_)));
    }
}