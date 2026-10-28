//! 真实 HTTP 传输 Raft 快照追平 e2e 回归（F: follower 永久落后根治）
//! 回归场景：leader 已有 >1MiB 快照并 purge 日志后，新增 learner（空 storage）
//! 【必须】经真实 HTTP `/raft/install-snapshot`（openraft full_snapshot 按
//! dev_config().snapshot_max_chunk_size 分块）追上。
//!
//! 红/绿：修复前 raft_router 无 body limit override → 1MiB raw 分块经
//! `InstallSnapshotRequest.data: Vec<u8>` JSON 数字数组 ~3.1x ≈3MB > axum 默认 2MB
//! → 恒 413 "length limit exceeded"，learner 无法追平（60s wait 失败）；修复后通过。
//! 锁定：raft_router 32MiB /raft/* body limit + dev_config
//! install_snapshot_timeout=30s（openraft 默认 200ms 单分块预算）+
//! snapshot_max_chunk_size=1MiB。
//!
//! 装配 MUST use HttpNetworkFactory + raft_router（reqwest↔axum）——
//! 进程内 NetworkFactory 测不到 body limit/RPC 传输。
//!
//! snapshot 路径强证据（红/绿 非假绿灯）：
//! 1) leader metrics.purged.index >= snapshot_index（日志经 snapshot 已删）;
//! 2) learner 日志上 1..=snapshot_index 无 entry（value 经 snapshot 传输）;
//! 3) learner metrics.purged.index >= snapshot_index（install_full_snapshot purge covered）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_core::command::{Command, DraftUpdateItem};
use dsh_core::model::{BranchName, GroupDef, ItemDef, PublishPolicy, Value, ValueType};
use dsh_core::StateMachine;
use dsh_raft::openraft::rt::WatchReceiver;
use dsh_raft::openraft::storage::RaftLogReader;
use dsh_raft::openraft::type_config::alias::LogIdOf;
use dsh_raft::openraft::RaftMetrics;
use dsh_raft::*;
use dsh_storage::RedbStorage;

static SEQ: AtomicU64 = AtomicU64::new(0);

const TOKEN: &str = "test-token";
// dev_config 配套三值（raft.rs dev_config / raft_http_server.rs raft_router 注释）：
const CHUNK_BYTES: usize = 1024 * 1024; // snapshot_max_chunk_size（raw）
const AXUM_LEGACY_LIMIT: usize = 2 * 1024 * 1024; // 修复前 axum Json default body limit
const RAFT_BODY_LIMIT: usize = 32 * 1024 * 1024; // 修复后 /raft/* DefaultBodyLimit::max
const ITEM_COUNT: usize = 40;
const VALUE_BYTES: usize = 60_000; // MAX_VALUE_BYTES=64KiB 单 item 值（40×60KB ≈ 2.4MB state）
const PID: &str = "p1";
const GROUP: &str = "g";

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("dsh-http-snap-{tag}-{}-{n}", std::process::id()))
}

struct Node {
    info: NodeInfo,
    raft: RaftHandle,
    sm: Arc<RwLock<StateMachine>>,
    // 独立 LogStore handle（Raft consume 其 own; 测试读日志经 this）
    log_store: LogStore,
    dir: std::path::PathBuf,
    // P3 voter 用例: 端口预绑定但 HTTP 服务可延迟启动（模拟「节点 down 追赶」voter）
    listener: Option<tokio::net::TcpListener>,
}

/// 装配一个节点：bind 127.0.0.1:0 真实端口（HttpNetworkFactory base=
/// `http://{node.raft_addr}` 与 axum::serve 同一 addr）→ fresh redb dir → raft → /raft/*.
/// P3 voter 用例：bind_node 预绑定端口+raft 但【延迟】HTTP serve（模拟 down 节点——
/// 地址在集群成员表而服务未 answering），serve_node 后「重」上线 → 经 full-snapshot 追平。
async fn bind_node(node_id: NodeId, tag: &str, network: &HttpNetworkFactory) -> Node {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let dir = tmpdir(tag);
    let _ = std::fs::remove_dir_all(&dir);
    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone(), dir.clone()));
    let info = NodeInfo {
        grpc_addr: "127.0.0.1:0".into(),
        http_addr: format!("http://{addr}"),
        raft_addr: addr,
    };

    let raft = new_raft_node(
        node_id,
        info.clone(),
        LogStore::new(db.clone()),
        sm_store.clone(),
        network,
        dev_config(),
    )
    .await
    .unwrap();

    Node {
        info,
        raft,
        sm,
        log_store: LogStore::new(db.clone()),
        dir,
        listener: Some(listener),
    }
}

/// 上线 /raft/* 服务（axum serve 同一 pre-bound listener）——「节点重上线」动作。
fn serve_node(n: &mut Node) {
    let listener = n.listener.take().expect("listener bound in bind_node");
    let state = RaftServerState::with_token(
        n.raft.clone(),
        Some(Arc::from(TOKEN)),
        n.dir.clone().join(store::SNAP_SUBDIR),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, raft_router(state)).await;
    });
}

async fn start_node(node_id: NodeId, tag: &str, network: &HttpNetworkFactory) -> Node {
    let mut n = bind_node(node_id, tag, network).await;
    serve_node(&mut n);
    n
}

// 0.10：RaftMetrics 泛型 <C>（0.9 <NID, N>）; metrics() -> WatchReceiver trait .borrow() gone
fn metrics_of(raft: &RaftHandle) -> RaftMetrics<TypeConfig> {
    raft.metrics().borrow_watched().clone()
}

// 0.10：LogId<NID>（0.9）gone —— LogId<CLID,NID>; TypeConfig 侧 alias LogIdOf（LeaderId<u64,u64>）
fn idx(v: Option<LogIdOf<TypeConfig>>) -> u64 {
    v.map(|l| l.index).unwrap_or(0)
}

async fn poll_metrics<F: Fn(RaftMetrics<TypeConfig>) -> bool>(
    raft: &RaftHandle,
    timeout: Duration,
    ok: F,
) -> RaftMetrics<TypeConfig> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let m = metrics_of(raft);
        if ok(m.clone()) {
            return m;
        }
        if tokio::time::Instant::now() >= deadline {
            return m;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn sm_has_project(sm: &Arc<RwLock<StateMachine>>, name: &str) -> bool {
    let g = sm.read().unwrap();
    g.list_projects().unwrap().iter().any(|p| p.name == name)
}

/// dev 分支 draft 的 group/key 草稿值（DraftUpdate 未 Publish → 值在 value_draft）。
fn sm_draft_value(sm: &Arc<RwLock<StateMachine>>, group: &str, key: &str) -> Option<Value> {
    let g = sm.read().unwrap();
    let st = g
        .get_branch_state(&(PID.into()), &(BranchName("dev".into())))
        .unwrap()
        .unwrap();
    st.value_draft
        .get(group)
        .and_then(|m| m.get(key))
        .map(|d| d.value.clone())
}

/// client_write double unwrap：Raft 层 Err（未提交/超时）+ 状态机层 Err（apply 拒绝）。
async fn must_write(raft: &RaftHandle, cmd: Command, ctx: String) {
    let outer = client_write(raft, cmd, Duration::from_secs(10)).await;
    let inner = outer.expect(ctx.as_str());
    inner.expect(ctx.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_catches_up_via_http_snapshot() {
    let network = HttpNetworkFactory::with_token(Some(TOKEN.into()));

    // 1) leader（真实 HTTP /raft/* 服务端）
    let n1 = start_node(1, "leader", &network).await;
    initialize_single(&n1.raft, 1, n1.info.clone())
        .await
        .unwrap();
    assert!(wait_for_leader(&n1.raft, Duration::from_secs(5))
        .await
        .is_some());

    // 2) 写 >1MiB 状态（40 item × 60KB draft value）
    must_write(
        &n1.raft,
        Command::ProjectCreate {
            name: PID.into(),
            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
        "project create".to_string(),
    )
    .await;
    let items: Vec<ItemDef> = (0..ITEM_COUNT)
        .map(|i| ItemDef {
            key: format!("k{}", i),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: None,
            shared: false,
        })
        .collect();
    must_write(
        &n1.raft,
        Command::StructureDraftSet {
            project: PID.into(),
            base_version: 1,
            groups: vec![GroupDef {
                name: GROUP.into(),
                items,
            }],
            operator: String::new(),
        },
        "structure draft set".to_string(),
    )
    .await;
    must_write(
        &n1.raft,
        Command::PublishStructure {
            project: PID.into(),
            comment: "init".into(),
            request_id: "s1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        "publish structure".to_string(),
    )
    .await;
    for i in 0..ITEM_COUNT {
        must_write(
            &n1.raft,
            Command::DraftUpdate {
                project: PID.into(),
                branch: BranchName("dev".into()),
                updates: vec![DraftUpdateItem {
                    group: GROUP.into(),
                    key: format!("k{}", i),
                    value: Value::String("x".repeat(VALUE_BYTES)),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            format!("draft update k{}", i),
        )
        .await;
    }
    let last_write_index = metrics_of(&n1.raft).last_log_index.expect("last_log_index");

    // 分块 math（snapshot data = serde_json(dump_all)）
    let pairs = n1.sm.read().unwrap().dump_all().unwrap();
    let state_bytes = pairs
        .iter()
        .fold(0usize, |acc, kv| acc + kv.0.len() + kv.1.len());
    let chunks = state_bytes.div_ceil(CHUNK_BYTES);
    assert!(
        chunks >= 2,
        "state ~{}B 需 >=2 分块（{}B）",
        state_bytes,
        CHUNK_BYTES
    );
    println!(
        "[diag] snapshot raw ~{}B -> {} chunk(s) of {}B; per-chunk JSON ~{}B vs legacy axum limit {}B / fixed limit {}B",
        state_bytes,
        chunks,
        CHUNK_BYTES,
        CHUNK_BYTES * 4,
        AXUM_LEGACY_LIMIT,
        RAFT_BODY_LIMIT,
    );
    // 3) force snapshot，then purge logs upto snapshot index
    n1.raft.trigger().snapshot().await.unwrap();
    let m1 = poll_metrics(&n1.raft, Duration::from_secs(10), |m| {
        idx(m.snapshot) >= last_write_index
    })
    .await;
    let snapshot_index = idx(m1.snapshot);
    assert!(
        snapshot_index >= last_write_index,
        "snapshot 未覆盖 last write: snapshot={} last_write={}",
        snapshot_index,
        last_write_index,
    );

    let upto = snapshot_index;
    n1.raft.trigger().purge_log(upto).await.unwrap();
    let m2 = poll_metrics(&n1.raft, Duration::from_secs(10), |m| idx(m.purged) >= upto).await;
    assert!(
        idx(m2.purged) >= upto,
        "leader purged 未 reach snapshot index（learner 无法经 snapshot 路径）: purged={:?} upto={}",
        m2.purged,
        upto,
    );
    println!(
        "[diag] leader last_write={} snapshot_index={} purged_index={}",
        last_write_index,
        snapshot_index,
        idx(m2.purged),
    );

    // 4) 新增 learner（空 storage），then add_learner -> 经真实 HTTP snapshot 追平
    let mut n2 = start_node(2, "learner", &network).await;
    let t0 = std::time::Instant::now();
    n1.raft
        .add_learner(2, n2.info.clone(), false)
        .await
        .unwrap();
    let caught = wait_until(|| sm_has_project(&n2.sm, PID), Duration::from_secs(60)).await;
    println!(
        "[diag] learner caught_up={} elapsed={:?}",
        caught,
        std::time::Instant::now() - t0,
    );
    assert!(
        caught,
        "learner 60s 未可见 p1（HTTP snapshot 分块追平失败）"
    );

    // 5) snapshot 内容可见：dev draft 60KB value + learner 上 log/metrics state
    let big = Value::String("x".repeat(VALUE_BYTES));
    let got = sm_draft_value(&n2.sm, GROUP, "k0");
    assert_eq!(got, Some(big), "learner 未 see snapshot dev draft value k0");
    let m3 = poll_metrics(&n2.raft, Duration::from_secs(10), |m| {
        idx(m.last_applied) >= snapshot_index && idx(m.purged) >= snapshot_index
    })
    .await;
    assert!(
        idx(m3.last_applied) >= snapshot_index,
        "learner last_applied={:?} < snapshot_index={}",
        m3.last_applied,
        snapshot_index,
    );
    assert!(
        idx(m3.purged) >= snapshot_index,
        "learner purged={:?} < snapshot_index={}（install_full_snapshot 应 purge covered logs）",
        m3.purged,
        snapshot_index,
    );

    // snapshot 路径强证据：learner 日志上 1..=snapshot_index 无 entry
    let covered = n2
        .log_store
        .try_get_log_entries(1..=snapshot_index)
        .await
        .unwrap();
    assert!(
        covered.is_empty(),
        "learner log 1..={} 应 empty（value 经 snapshot 传输）, got {}",
        snapshot_index,
        covered.len(),
    );

    // 6) cleanup
    for n in [n1, n2] {
        let _ = n.raft.shutdown().await;
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}
