use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use mini_mito::{Key, LSMTableProvider, Memtable, Value};
use tempfile::tempdir;

fn k(tag: u8, ts: i64) -> Key {
    (vec![tag], ts)
}
fn v(s: &str) -> Value {
    s.as_bytes().to_vec()
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let dir = tempdir().expect("创建临时目录失败");
    let wal_path = dir.path().join("wal.log");

    let mut memtable = Memtable::new(&wal_path).expect("打开 Memtable 失败");

    memtable.insert(k(1, 1000), v("value1"))?;
    memtable.insert(k(2, 2000), v("value2"))?;
    memtable.insert(k(1, 3000), v("value3"))?;

    memtable.flush()?;

    memtable.insert(k(3, 4000), v("value4"))?;
    memtable.insert(k(2, 5000), v("value5"))?;

    let provider = LSMTableProvider::new(memtable);
    let ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(provider))?;

    println!("=== 全表查询 ===");
    let df = ctx.sql("SELECT * FROM my_table").await?;
    let batches = df.collect().await?;
    for batch in batches {
        println!("{:?}", batch);
    }

    println!("\n=== 投影查询 (tags, timestamp) ===");
    let df = ctx.sql("SELECT tags, timestamp FROM my_table").await?;
    let batches = df.collect().await?;
    for batch in batches {
        println!("{:?}", batch);
    }

    println!("\n=== 过滤查询 (timestamp > 2000) ===");
    let df = ctx
        .sql("SELECT * FROM my_table WHERE timestamp > 2000")
        .await?;
    let batches = df.collect().await?;
    for batch in batches {
        println!("{:?}", batch);
    }

    Ok(())
}