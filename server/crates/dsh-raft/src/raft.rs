//! Raft 装配助手（模块 03 §3/§4）。

use std::sync::Arc;

use openraft::rt::WatchReceiver;
use openraft::Config;
use openraft::Raft;

use crate::network::RaftHandle;
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, NodeInfo, TypeConfig};

pub type RaftConfig = Config;

/// 默认测试/联调配置（心跳 100ms、选举 300~600ms）。
///
/// AppendEntries 批量预算（P3a-C1，release 档 measurements，compat report §13）:
/// - msgpack wire ≈ stored-JSON bytes 1.00x（0.10 binary RPC，P2b1）；64 entries × 60 KiB
///   值 = 3.67 MiB wire、redb append+fsync ≈5ms（release）→ `max_payload_entries=64` 保持
///   (entry count 是 practical bound at current entry sizes；I/O never dominates);
/// - store 层 `MAX_BATCH_BYTES=6MiB`（`limited_get_log_entries` byte cap）chops
///   pathological fat-entry batches（e.g. 1 MiB-value entries → ~6 per RPC）;
/// - `enable_pre_vote=Some(true)`:leader 先 pre_vote RPC（0.10 default 关闭），fewer
///   wasted appends/network roundtrips when leadership moves。
///
/// 快照传输配套（follower 落后根治，P2b2 streaming，see raft_http_server.rs `raft_router`）:
/// - `install_snapshot_timeout=30s`：**0.9 时代的 knob，0.10 不生效**。0.10 的 full snapshot
///   走独立 snapshot client 的流式 RPC（`http_network.rs` 里 300s 总超时），本字段只被旧
///   per-chunk RPC 路径消费。安装端到端预算由压测断言（`--max-install-seconds`）而非此字段
///   兜底；保留 30_000 仅为配置兼容。P4 决策：要么用 `RPCOption::soft_ttl()` 真正约束流式
///   请求，要么改名/下线该字段并在运行手册里给出真实上限。
/// - `snapshot_max_chunk_size=1MiB`：网络层 clamps frame payload ≤1MiB（snapshot_stream
///   MAX_CHUNK_BYTES）；body 是 msgpack frames（旧 0.9 JSON 数字数组 ~3.1x 膨胀已 resolved
///   by streaming；/raft/* 32MiB body limit now only guards frame framing —
///   dev_docs/deployment-guide.md §6.5 updated accordingly）。
pub fn dev_config() -> RaftConfig {
    Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        max_payload_entries: 64,
        // (P3a-C1) pre_vote before append — openraft 0.10 default (None→false) wastes
        // appends to stale leaders; 60 KiB fat entries make the roundtrips expensive.
        enable_pre_vote: Some(true),
        enable_tick: true,
        install_snapshot_timeout: 30_000,
        snapshot_max_chunk_size: 1024 * 1024,
        // P3f（design §9.4「默认提升并可配置」）：快照已覆盖、但仍保留不 purge 的日志条数。
        // 值越大，略落后的 follower 越可能靠增量日志追上而不必整份快照（issue #1706）；
        // 代价是磁盘：60KB/entry 时 50k ≈ 3GB/节点。生产默认值留 P5 按写入速率压测确定，
        // 这里默认沿用 openraft 的 1000，用 env 覆盖即可压测不同档位：
        //   DSH_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP=50000
        max_in_snapshot_log_to_keep: std::env::var("DSH_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or_else(|| Config::default().max_in_snapshot_log_to_keep),
        ..Default::default()
    }
}

/// 创建 Raft 节点并注册到网络。
pub async fn new_raft_node<N>(
    id: NodeId,
    _node: NodeInfo,
    log_store: LogStore,
    sm_store: Arc<StateMachineStore>,
    network: &N,
    config: RaftConfig,
) -> Result<RaftHandle, Box<dyn std::error::Error + Send + Sync>>
where
    N: openraft::network::RaftNetworkFactory<TypeConfig> + Clone + Send + Sync + 'static,
    // 0.10：SnapshotData 已 moved 从 RaftTypeConfig 到 NetSnapshot——Raft::new 要求
    // N::Network:SnapshotData == SM:SnapshotData（P2a: PathBuf，file-backed）
    N::Network: openraft::network::NetSnapshot<TypeConfig, SnapshotData = std::path::PathBuf>,
{
    let raft = Raft::new(
        id,
        Arc::new(config),
        network.clone(),
        log_store,
        sm_store.as_ref().clone(),
    )
    .await?;
    Ok(raft)
}

/// 单节点初始化集群（bootstrap）。
pub async fn initialize_single(
    raft: &RaftHandle,
    node_id: NodeId,
    node: NodeInfo,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    raft.initialize(std::collections::BTreeMap::from([(node_id, node)]))
        .await?;
    Ok(())
}

/// 按静态成员表建群（seed map，多节点并行自举）。
///
/// 语义（openraft 文档 + 实测）：所有节点用【完全相同】的 map 并发调用 `initialize` 是安全的——
/// 先到者完成首写，其余节点若已收到同群竞选投票（vote 非 (0,0)）会返回 `NotAllowed` 这一
/// 【良性错误】（节点保持安全，随后经 leader 复制追平成员表成为 voter）。
///
/// 返回 `true` = 本节点完成了建群首写；`false` = 建群已由他节点完成（本节点等待复制追平）。
/// map 不一致（不同节点不同 map）是非法的 split-brain，调用方（dsh-cli）负责校验配置一致。
pub async fn initialize_cluster(
    raft: &RaftHandle,
    members: std::collections::BTreeMap<NodeId, NodeInfo>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match raft.initialize(members).await {
        Ok(()) => Ok(true),
        Err(openraft::error::RaftError::APIError(
            openraft::error::InitializeError::NotAllowed(_),
        )) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// 等待成为 leader。
pub async fn wait_for_leader(raft: &RaftHandle, timeout: std::time::Duration) -> Option<NodeId> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(leader) = raft.current_leader().await {
            return Some(leader);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// 等待某节点成为 leader。
pub async fn wait_until<F: Fn() -> bool>(cond: F, timeout: std::time::Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// 客户端写（重试直至成功/超时）。
/// 返回 `Ok(Ok(ack))` = 已生效（ack 含版本号与本命令 apply 产出的事件，F6）；
/// `Ok(Err(e))` = 状态机 apply 拒绝（带 ErrorKind，可映射 HTTP/gRPC 错误码）；
/// `Err(e)` = Raft 层失败（未提交/超时，可重试）。
pub async fn client_write(
    raft: &RaftHandle,
    cmd: dsh_core::command::Command,
    timeout: std::time::Duration,
) -> Result<Result<crate::types::WriteAck, dsh_core::Error>, Box<dyn std::error::Error + Send + Sync>>
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match raft.client_write(cmd.clone()).await {
            Ok(resp) => return Ok(resp.data),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("client_write timeout: {e}").into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// 客户端写失败（单次尝试）的归类。
#[derive(Debug)]
pub enum WriteError {
    /// 非 leader：携带 leader 的 HTTP 管理面地址（客户端应跟随转发）。
    ForwardToLeader {
        leader_id: Option<NodeId>,
        http_addr: Option<String>,
    },
    /// 其他 Raft 层错误（瞬时，可重试）。
    Other(String),
}

/// 单次客户端写（不重试）：`Ok(Ok(ack))` 已生效；`Ok(Err(e))` 状态机拒绝；
/// `Err(WriteError)` 为 Raft 层失败（含 leader 转发提示）。
pub async fn try_client_write(
    raft: &RaftHandle,
    cmd: dsh_core::command::Command,
) -> Result<Result<crate::types::WriteAck, dsh_core::Error>, WriteError> {
    match raft.client_write(cmd).await {
        Ok(resp) => Ok(resp.data),
        Err(openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(f),
        )) => Err(WriteError::ForwardToLeader {
            leader_id: f.leader_id,
            http_addr: f.leader_node.map(|n| n.http_addr),
        }),
        Err(e) => Err(WriteError::Other(e.to_string())),
    }
}

/// 从本节点 raft metrics 解析当前 leader 的 HTTP 管理面地址
/// （ForwardToLeader 缺 node 信息时兜底：learner 常不知道 leader 的 NodeInfo）。
pub fn leader_http_addr(raft: &RaftHandle) -> Option<String> {
    // 0.10：metrics() -> WatchReceiver（openraft:rt:WatchReceiver trait）—— 0.9 `.borrow()` gone
    //  -> borrow_watched()；StoredMembership.get_node direct（no .membership().get_node()）
    let m = raft.metrics().borrow_watched().clone();
    let leader_id = m.current_leader?;
    m.membership_config
        .get_node(&leader_id)
        .map(|n| n.http_addr.clone())
}

/// 写操作结果。
#[derive(Debug, Default)]
pub struct WriteOutcome {
    pub version: u64,
    pub events: Vec<dsh_core::model::PublishEvent>,
}

/// 通用写路径（模块 05 写面）：dev-single 直接 apply 状态机；集群模式经 Raft client_write。
/// - `events_tx`：dev-single 直发 watch 广播（集群模式由 raft apply 经 sm_store 转发，传 None）。
/// - 非 leader → `ErrorKind::LeaderRedirect`（携带 leader_hint，调用方跟随转发）。
pub async fn write_command(
    sm: &std::sync::RwLock<dsh_core::StateMachine>,
    raft: Option<&RaftHandle>,
    cmd: &dsh_core::command::Command,
    now_ms: i64,
    events_tx: Option<&tokio::sync::broadcast::Sender<dsh_core::model::PublishEvent>>,
) -> Result<WriteOutcome, dsh_core::Error> {
    match raft {
        None => {
            // perf 方案③：写锁作用域化——apply 后解锁再广播（锁外 send）
            let (events, version) = {
                let mut guard = sm
                    .write()
                    .map_err(|e| dsh_core::Error::internal(e.to_string()))?;
                let events = guard.apply(cmd, now_ms)?;
                let version = events.first().map(|e| e.version).unwrap_or(0);
                (events, version)
            };
            for e in &events {
                if let Some(tx) = events_tx {
                    let _ = tx.send(e.clone());
                }
            }
            Ok(WriteOutcome { version, events })
        }
        Some(raft) => {
            // 单次尝试；非 leader → 立即返回 ERR_LEADER_REDIRECT（携带 leader_hint），
            // 由调用方（login 转发 / SDK failover）跟随；瞬时错误重试至超时。
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match try_client_write(raft, cmd.clone()).await {
                    Ok(r) => {
                        // F6：ack 携带事件（changes/affected），dev-single 与集群行为一致
                        let ack = r?;
                        return Ok(WriteOutcome {
                            version: ack.version,
                            events: ack.events,
                        });
                    }
                    Err(WriteError::ForwardToLeader { http_addr, .. }) => {
                        // leader_node 可能为空（learner 不知 leader NodeInfo）→ 从本节点 metrics 兜底解析
                        let hint = http_addr
                            .or_else(|| leader_http_addr(raft))
                            .unwrap_or_default();
                        return Err(dsh_core::Error::new(
                            dsh_core::ErrorKind::LeaderRedirect,
                            "not leader, follow leader_hint",
                        )
                        .with_leader_hint(hint));
                    }
                    Err(WriteError::Other(_)) => {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(dsh_core::Error::internal("client_write timeout"));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}
