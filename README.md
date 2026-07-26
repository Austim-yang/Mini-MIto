# Mini Mito

[![Rust](https://img.shields.io/badge/rust-2024%20edition-blue)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

一个基于 Rust 的 LSM-Tree 存储引擎演示。灵感来源于 GreptimeDB 的 Mito 引擎。

LSM-Tree（Log-Structured Merge-Tree）是现代高性能数据库（如 GreptimeDB、LevelDB、RocksDB）的核心存储架构。它将随机写转换为顺序写，大幅提升写入吞吐量，非常适合时序数据和日志型数据。

## 当前状态

| 组件 | 说明 |
| :--- | :--- |
| **跳表（SkipList）** | 基于`crossbeam-skiplist`，支持插入、查询、删除、迭代；存储` (Vec<u8>, i64) → Vec<u8>` |
| **预写日志（WAL）** | 追加写入，JSON 序列化，支持崩溃恢复。 |
| **Memtable** | 封装跳表和 WAL，启动时自动恢复数据，提供统一的读写接口。 自动刷新阈值（默认 1000 条），超限时生成 SSTable 并重置 WAL；查询按新到旧顺序合并跳表和 SSTable。 |
| **SSTable** | 基于 Parquet 格式存储键值对（Binary 列，序列化为 JSON 字节），支持` create_from_skiplist`、点查 `get` 和范围扫描 `scan`，附带 `min_key/max_key` 元数据加速过滤。 |
| **Manifest 管理** | 记录所有 SSTable 的元信息（ID、路径、键范围、条目数），重启时恢复；若 Manifest 丢失，自动扫描目录重建 |
| **DataFusion 集成** | 实现 `TableProvider` 和 `ExecutionPlan`，将 LSM 存储暴露为 SQL 表（`tags`、`timestamp`、`fields` 三列）；支持投影和过滤 |
| **单元测试** | 覆盖SkipList、内存表、WAL、SSTable 和持久化恢复，全部通过。 |
| **基准测试** | 使用 Criterion 进行多维度性能测量（插入、查询、扫描、刷新、压缩、全表扫描等） |

## 技术栈

- Rust 2024 Edition
- `crossbeam-skiplist`
- `serde` + `serde_json`
- `datafusion`
- `tokio`
- `rand`
- `tempfile`
- `parquet` + `arrow` + `arrow-schema`
- `criterion`

## 构建与运行

```bash
git clone <your-repo-url>
cd mini_mito
cargo build
cargo test
```

## 运行基准测试

```bash
cargo bench
```

## 参考资料

- [GreptimeDB Mito 存储引擎设计](https://docs.greptime.com/contributor-guide/storage-engine/overview) —— 本项目的主要灵感来源
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) —— 原始论文

## License

[MIT](LICENSE)
