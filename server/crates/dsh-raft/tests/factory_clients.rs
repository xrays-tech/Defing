//! P2b1: HttpNetworkFactory 持有三个独立 reqwest 连接池（data/heartbeat/snapshot）。
//! 验证（no network capture needed）:
//! - 三个 pool 的 builder 配置 pairwise 不同（reqwest Client Debug renders a
//!   client-level total timeout as `reqwest::config::TotalTimeout`);
//! - data pool 无 client-level total timeout（per-request RPCOption::soft_ttl 是
//!   authoritative in post_msgpack）; heartbeat(3s)/snapshot(300s) pools carry one.
use dsh_raft::openraft::network::RaftNetworkFactory;

use dsh_raft::types::NodeInfo;
use dsh_raft::HttpNetworkFactory;

#[tokio::test]
async fn three_client_pools_are_independent() {
    let f = HttpNetworkFactory::new();
    let data = f.data_client();
    let heartbeat = f.heartbeat_client();
    let snapshot = f.snapshot_client();

    let ds = format!("{data:?}");
    let hs = format!("{heartbeat:?}");
    let ss = format!("{snapshot:?}");

    // all three distinct configs
    assert_ne!(ds, hs, "data vs heartbeat pool must differ");
    assert_ne!(ds, ss, "data vs snapshot pool must differ");
    assert_ne!(hs, ss, "heartbeat(3s) vs snapshot(300s) pool must differ");

    // data pool carries no total-timeout cap (soft_ttl is authoritative);
    // heartbeat pool caps at 3s, snapshot pool at 300s.
    assert!(
        !ds.contains("TotalTimeout"),
        "data pool must not set a total timeout cap: {}",
        ds,
    );
    assert!(
        hs.contains("TotalTimeout") && hs.contains("3s"),
        "heartbeat pool: 3s cap"
    );
    assert!(
        ss.contains("TotalTimeout") && ss.contains("300s"),
        "snapshot pool: 300s cap"
    );

    // wiring smoke: all three factory entry points yield a client for the target
    // (pool wiring is internal choice; config independence asserted above)
    let info = NodeInfo {
        grpc_addr: "127.0.0.1:0".to_string(),
        http_addr: "http://127.0.0.1:0".to_string(),
        raft_addr: "127.0.0.1:0".to_string(),
    };
    let mut factory = HttpNetworkFactory::new();
    let _c1 = factory.new_client(1, &info).await;
    let _c2 = factory.new_heartbeat_client(1, &info).await;
    let _c3 = factory.new_snapshot_client(1, &info).await;
}
