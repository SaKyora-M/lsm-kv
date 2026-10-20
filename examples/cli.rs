//! 命令：
//!   put <key> <value>    写入键值对
//!   get <key>            读取键
//!   delete <key>         删除键
//!   compact              手动触发一次全量合并
//!   exit / quit          退出

use std::io::{self, Write};

use lsm_kv::Db;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 数据目录：优先用命令行第一个参数，否则默认 ./data
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./data".to_string());

    let db = Db::open(&dir)?;
    println!("mini-lsm 已打开，数据目录: {dir}");

    loop {
        // print! 不会自动换行，stdout 是行缓冲，要手动 flush 才能立刻显示提示符
        print!("> ");
        io::stdout().flush()?;

        let mut line = String::new();
        // read_line 返回读到的字节数；读到 0 表示 EOF（Ctrl+Z / Ctrl+D），退出
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }

        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "put" => {
                if parts.len() != 3 {
                    println!("用法: put <key> <value>");
                    continue;
                }
                db.put(parts[1].as_bytes(), parts[2].as_bytes())?;
                println!("ok");
            }
            "get" => {
                if parts.len() != 2 {
                    println!("用法: get <key>");
                    continue;
                }
                match db.get(parts[1].as_bytes())? {
                    Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                    None => println!("(nil)"),
                }
            }
            "delete" => {
                if parts.len() != 2 {
                    println!("用法: delete <key>");
                    continue;
                }
                db.delete(parts[1].as_bytes())?;
                println!("ok");
            }
            "compact" => {
                // 手动触发一次全量合并：合并所有 SSTable、清理旧版本、丢墓碑。
                db.compact()?;
                println!("ok (compacted)");
            }
            "exit" | "quit" => break,
            other => println!("未知命令: {other}，支持 put/get/delete/compact/exit"),
        }
    }

    println!("bye");
    Ok(())
}
