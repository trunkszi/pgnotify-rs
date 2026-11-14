# pgnotify

[English](README.md)

基于非阻塞 libpq、由调用方驱动的 PostgreSQL LISTEN/NOTIFY 库。
`PgListener` 在单个线程上独占连接。应用提供套接字就绪事件和定时器到期事件，
库本身不需要异步运行时或工作线程。Linux 应用可将其接入已有的 io_uring Proactor。

## 工作区与功能

| 路径 | 用途 |
| --- | --- |
| `crates/pgnotify` | 监听器、有界值、通道注册表与适配器 |
| `crates/pgnotify-macros` | 类型化载荷与路由派生宏 |
| `crates/pgnotify/tests` | 协议、生命周期、适配器与编译契约测试 |
| `crates/pgnotify/examples` | 调用方持有 Proactor 的示例 |
| `crates/pgnotify/benches` | 路由、查找与编码基准 |
| `xtask` | 工作区质量门禁 |

`pgnotify` 默认启用 `uring`、`diesel` 和 `sonic`。禁用默认功能可仅使用 libpq
监听器；单独选择 `sonic` 可使用 JSON 载荷而不启用适配器。`uring` 使用 `liburing`；
`diesel` 提供同步 PostgreSQL 集成；`sonic` 提供有界 JSON 序列化。
派生宏由独立的 `pgnotify-macros` crate 提供。

构建使用 `rust-toolchain.toml` 指定的工具链，并通过 `pq-sys` 捆绑 libpq。
原生构建需要 C 工具链和 CMake。运行 Proactor 还需要允许 io_uring 的 Linux 环境。

## 监听器生命周期

通过 `PgListener::new(options, hook)` 创建监听器，用 `subscribe` 注册所需通道，
再用 `connect` 发起连接。使用数值数据库地址或显式的数值 `hostaddr`，避免阻塞式域名解析。

接入自有事件循环时，读取 `poll_interest()` 和 `timer_deadline()`，然后调用
`on_event(ListenerEvent::Socket { ... })` 或 `on_event(ListenerEvent::Timer { generation })`。
投递先前注册的套接字事件前，通过 `owns_socket` 检查其代际。
每个定时器代际只安排一次，并在其时长到期时投递；重复读取同一截止时间不能重启超时。
零时长的延续会安排下一批有界通知处理。

`NotifyHook` 提供通知和生命周期回调。`NotificationRef` 借用 libpq 存储，
在 `on_notify` 返回时失效。若应用数据需要跨越回调生命周期，必须在回调内复制。
订阅确认先于就绪回调。重连时，监听器先重放所需通道，再调用
`on_reconnected(epoch, backend_pid)`。断连期间发送的通知会丢失；应在 epoch 改变后对账应用状态。
发生故障时依次调用 `on_error` 和 `on_disconnect`，随后进入有界重试退避。
`begin_shutdown` 关闭核心监听器并使待处理代际失效。

## 通道注册表与载荷边界

`ChannelTable::new()` 默认容纳 256 项；`with_capacity` 接受 1..=65536。
`subscribe` 和 `register` 校验名称，重复注册时返回已有 ID。
`lookup`、`channel` 和 `remove` 提供名称/ID 查找与移除；ID 在移除前保持稳定，之后可被复用。

名称必须是 1..=63 字节的 UTF-8，且不含 NUL。名称查找最多经过 126 条十六进制 trie 边，
与通道数量无关，也不分配内存。注册属于控制路径操作，可以分配内存。

`PayloadBuf` 最多保存 7999 个已提交字节。JSON 编码复用栈存储，包括私有的序列化暂存区。
编码结果恰好为 7999 字节时成功；更大时返回 `PayloadTooLarge`。
暂存区预留容量与已提交输出长度不同。应用查找和编码的分配断言并不意味着 libpq 或 Diesel 从不分配内存。

## 类型化通知

在载荷类型上使用 Serde 的 `Serialize`、`Deserialize` 派生以及 `pgnotify-macros` 的
`NotifyPayload`。启用 `sonic` 功能，并为 `serde` 启用 `derive` 功能。
路由枚举将变体关联到通道名称，支持单元变体、单字段元组变体和单字段结构体变体。

在应用的 `Cargo.toml` 中添加以下依赖：

```toml
[dependencies]
pgnotify = { version = "0.1.0", default-features = false, features = ["sonic"] }
pgnotify-macros = { version = "0.1.0" }
serde = { version = "1", features = ["derive"] }
```

派生宏通过 `pgnotify::sonic_api` 访问编解码器。

```rust
use pgnotify::{NotificationRef, NotifyHook, NotifyPayload as _, NotifyRouter as _, PayloadBuf};
use pgnotify_macros::{NotifyPayload, NotifyRouter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, NotifyPayload)]
struct Price { ticks: u64 }

#[derive(Debug, NotifyRouter)]
enum Events {
    #[channel = "prices"]
    Price(Price),
    #[channel = "heartbeat"]
    Heartbeat,
}

fn main() -> pgnotify::Result<()> {
    let mut out = PayloadBuf::new();
    Price { ticks: 42 }.encode_payload(&mut out)?;
    let event = Events::decode_notification(NotificationRef {
        backend_pid: 0,
        channel_id: None,
        channel: "prices",
        payload: out.as_str()?,
    })?;
    assert!(matches!(event, Some(Events::Price(Price { ticks: 42 }))));
    let mut hook = Events::router_hook(|result: pgnotify::Result<Events>| {
        match result {
            Ok(event) => println!("{event:?}"),
            Err(error) => eprintln!("{error}"),
        }
    });
    hook.on_notify(NotificationRef {
        backend_pid: 0, channel_id: None, channel: "heartbeat", payload: "",
    });
    Ok(())
}
```

`NotifyRouter` 在宏展开时拒绝无效或重复的通道，并提供 `CHANNELS`、带引号的
`LISTEN_BATCH_SQL`、`channel_name`、`encode_event_payload` 和 `decode_notification`。
分派使用有界字节匹配，不做哈希；未知通道返回 `Ok(None)`。
单元变体要求空载荷。已知通道的无效载荷返回错误。

`Events::router_hook` 创建 `EventsRouterHook<R, F>`。每个枚举都有独立的 hook 名称，
因此同一模块可以容纳多个路由器。回调接收 `pgnotify::Result<R>`，其中包含类型化事件或解码/监听器错误。
适配器的 `on_disconnect` 为空，因而一次监听器故障只投递一次。
本例中的打印是应用策略，不是热路径要求。

## Proactor 集成

`PgListenerDriver::new(subsystem, listener)` 包装监听器，但不持有 ring。
为每个驱动分配独立且非零的 `SubsystemId`，并让驱动始终使用同一个 ring。
状态改变后调用 `flush(&mut ring)`，通过 `CompletionDispatcher` 路由完成事件。
调用方将完成事件分派给其所属子系统时，其他协议可以共享这个 ring。

对于只服务监听器的 ring，`shutdown` 在有界预算内排空在途工作。
共享 ring 的持有方调用 `begin_shutdown`，继续执行 `flush` 和完成事件分派，
并保留驱动直到 `has_pending_io()` 为 false。取消确认和目标完成事件分别退役。
关闭出错后应保留驱动和 ring，以便继续排空。

以下示例监听 `events` 通道约 30 秒：

```sh
DATABASE_URL='postgresql://127.0.0.1/pgnotify?sslmode=disable' \
  cargo run -p pgnotify --example listener_driver --locked
```

## Diesel 集成

`DieselNotifyExt` 扩展调用方持有的同步 `diesel::PgConnection`。
`notify_channel` 发送经过校验的 UTF-8 载荷；`notify_event` 先将路由变体编码到
`PayloadBuf`，再通过绑定参数执行 `pg_notify`。通知遵循 PostgreSQL 事务提交语义。
`listen_channel`、`unlisten_channel` 和 `unlisten_all` 对通道标识符加引号。

`drain_notifications(budget, callback)` 最多投递指定数量的当前可用通知，并传播回调错误。
其借用视图没有通道 ID，因为 Diesel 连接不持有 `ChannelTable`。
Diesel 方法可能阻塞和分配内存。非阻塞借用接收路径应使用独立的 `PgListener`，
阻塞式 CRUD 不应放入该事件循环。`DieselNotifyError` 保留编解码/校验错误和原始 Diesel 错误。

## 验证

从工作区根目录运行以下命令：

```sh
cargo xtask ci
cargo test -p pgnotify --test compile_contracts --locked
cargo test -p pgnotify --test router_macro_tests --locked
cargo test -p pgnotify --test postgres_real_table_e2e_tests --locked
cargo bench -p pgnotify --bench notify_dispatch_bench --no-run --locked
cargo bench -p pgnotify --bench notify_dispatch_bench --locked
```

`cargo xtask ci` 检查格式，针对默认、仅核心和仅 sonic 配置运行拒绝警告的 Clippy，
测试这些配置，并构建拒绝警告的工作区 rustdoc。GitHub 工作流运行相同的门禁命令。
基准单独运行，报告 P50/P90/P99/P99.9 延迟，并对分派、通道查找和数值载荷编码进行分配断言。

确定性监听器测试使用可控的回环 PostgreSQL 协议对端。
`postgres_real_table_e2e_tests` 另行驱动真实 PostgreSQL 表、PL/pgSQL 触发器、类型化路由和真实 io_uring ring。
四个场景覆盖：INSERT/UPDATE 载荷与查询行核对；1000 次严格有序、无丢失无重复的写入及 7999 字节载荷；
事务回滚隔离；终止后端后重连、订阅重放与表数据对账。
还会检查 DELETE 不产生 INSERT/UPDATE 触发器事件。
重连审计区分实时通知与通过 SQL 恢复的行；NOTIFY 本身无法恢复断连期间发送的消息。

表测试套件与全部五个实时 Diesel 测试都不使用 ignore 标记。
它们仅通过 `PGNOTIFY_TEST_DATABASE_URL` 选择显式的测试数据库，忽略 `DATABASE_URL`。
未指定测试专用地址时，共享冷路径 fixture 辅助程序会复用或启动 `pgnotify-pg-e2e`，
使用 `postgres:18.6` 并绑定 `127.0.0.1:55433`，等待 `pg_isready`。
健康的容器不会被重启或移除。该回退路径需要 Docker；显式设置 `PGNOTIFY_TEST_DATABASE_URL` 可跳过 Docker 探测。
如需隔离的测试数据库，可使用此覆盖配置：测试套件会创建/删除 schema，并终止自己的监听后端。

```sh
PGNOTIFY_TEST_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:55433/postgres?sslmode=disable' \
  cargo test -p pgnotify --test postgres_real_table_e2e_tests --locked
```

监听器的非阻塞连接路径应使用数值地址，不能使用 `localhost`。
数据库角色必须有权限创建 schema/触发器并终止自己的监听后端。
每个场景使用独立 schema，在排空 ring 操作后删除；通道级 advisory lock 防止并发测试进程混入通知。
回调向量和同步 SQL 属于测试工具，不属于生产热路径。
GitHub Actions 通过相同环境变量提供位于 `127.0.0.1:5432` 的健康 `postgres:17` 服务。
工作区验证使用 `cargo xtask ci`；定向检查见上方命令。
