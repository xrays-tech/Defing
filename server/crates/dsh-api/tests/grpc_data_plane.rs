//! gRPC 数据面集成测试（A1）：GetConfig / GetItem / Watch / ListMembers + 项目令牌鉴权。
//! 使用真实 TCP 监听 + 生成的客户端。

use std::sync::{Arc, RwLock};

use dsh_api::grpc::{
    config_service_client::ConfigServiceClient, config_service_server::ConfigServiceServer,
    ConfigGrpcService, GetConfigRequest, GetItemRequest, ListMembersRequest, WatchRequest,
};
use dsh_api::ApiState;
use dsh_core::command::Command;
use dsh_core::model::{BranchName, ProjectId, PublishPolicy, Value};
use dsh_core::InMemoryStore;
use dsh_core::StateMachine;
use dsh_crypto::Cipher;
use dsh_testkit::seed_demo_project;
use dsh_watch::WatchHub;

/// 项目 p 的测试令牌明文（apply 时只存 SHA-256）。
const RAW_TOKEN: &str = "raw-token-abc123";

fn seed_sm(sm: &RwLock<StateMachine>) {
    // testkit 播种：项目 + 结构(host/port/pass secret) + dev 草稿(host/port) + 发布(v2)
    seed_demo_project(sm, "p").unwrap();
    // 追加 secret 项值（明文不落库，测试直接写密文）
    let mut g = sm.write().unwrap();
    g.apply(
        &Command::DraftUpdate {
            project: "p".into(),
            branch: BranchName("dev".into()),
            updates: vec![
                dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("10.0.0.1".into()),
                },
                dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "pass".into(),
                    value: Value::Secret(Cipher::new([9u8; 32]).encrypt_secret(b"s3cret").unwrap()),
                },
            ],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        6,
    )
    .unwrap();
    g.apply(
        &Command::Publish {
            project: "p".into(),
            branch: BranchName("dev".into()),
            comment: "v3".into(),
            request_id: "r2".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        7,
    )
    .unwrap();
    // 项目访问令牌（project-token）：只存 SHA-256
    g.apply(
        &Command::ProjectTokenCreate {
            project: "p".into(),
            name: "test".into(),
            token_hash: dsh_core::token_hash(RAW_TOKEN),
            operator: "admin".into(),
            ts: 0,
        },
        8,
    )
    .unwrap();
}

async fn start_server() -> (String, ApiState) {
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(
        InMemoryStore::new(),
    ))));
    seed_sm(&sm);
    let hub = WatchHub::new();
    let state = ApiState::new(
        sm,
        hub,
        None,
        None,
        None,
        std::time::Duration::from_secs(86400),
        "pw".into(),
        None,
    );
    let svc = ConfigServiceServer::new(ConfigGrpcService {
        state: state.clone(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), state)
}

async fn client_at(url: &str) -> ConfigServiceClient<tonic::transport::Channel> {
    ConfigServiceClient::connect(url.to_string()).await.unwrap()
}

/// 带 metadata authorization Bearer 的客户端（interceptor 包装类型）。
type AuthedClient = ConfigServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        Box<
            dyn FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>
                + Send
                + Sync,
        >,
    >,
>;

async fn authed_client(url: &str, raw: &str) -> AuthedClient {
    let channel = tonic::transport::Channel::from_shared(url.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let raw = raw.to_string();
    ConfigServiceClient::with_interceptor(
        channel,
        Box::new(move |mut req: tonic::Request<()>| {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {raw}").parse().unwrap());
            Ok(req)
        }),
    )
}

fn get_req(project: &str) -> GetConfigRequest {
    GetConfigRequest {
        project: project.into(),
        branch: "dev".into(),
        version: 0,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_config_and_get_item() {
    let (url, _state) = start_server().await;
    let mut client = authed_client(&url, RAW_TOKEN).await;

    let snap = client.get_config(get_req("p")).await.unwrap().into_inner();
    assert_eq!(snap.version, 3); // testkit v2 + secret v3
    assert_eq!(snap.structure_version, 2); // 结构发布后版本=2（base_version=1 → published 2）
    let host = snap.groups.get("redis").unwrap().items.get("host").unwrap();
    assert!(!host.masked);
    assert_eq!(host.r#type, 1); // STRING

    // secret：脱敏 + masked 标记（数据面不解密）
    let pass = snap.groups.get("redis").unwrap().items.get("pass").unwrap();
    assert!(pass.masked);
    let masked_val = match &pass.data {
        Some(dsh_api::grpc::value::Data::StrValue(s)) => s.as_str(),
        _ => "",
    };
    assert!(!masked_val.contains("s3cret"));

    // GetItem 单值
    let item = client
        .get_item(GetItemRequest {
            project: "p".into(),
            branch: "dev".into(),
            group: "redis".into(),
            key: "host".into(),
            version: 0,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let got = item.value.unwrap();
    assert!(!got.masked);
    match got.data.unwrap() {
        dsh_api::grpc::value::Data::StrValue(s) => assert_eq!(s, "10.0.0.1"),
        other => panic!("expected str value, got {other:?}"),
    }

    // 不存在的 item → NotFound
    let err = client
        .get_item(GetItemRequest {
            project: "p".into(),
            branch: "dev".into(),
            group: "redis".into(),
            key: "nope".into(),
            version: 0,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_token_auth_matrix() {
    let (url, state) = start_server().await;

    // 无 token → Unauthenticated
    let mut plain = client_at(&url).await;
    let err = plain.get_config(get_req("p")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // 错误 token → Unauthenticated
    let mut wrong = authed_client(&url, "wrong-token").await;
    let err = wrong.get_config(get_req("p")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // 正确 token → 成功
    let mut ok = authed_client(&url, RAW_TOKEN).await;
    let snap = ok.get_config(get_req("p")).await.unwrap().into_inner();
    assert_eq!(snap.version, 3);

    // 吊销后 → Unauthenticated（即时生效）
    let id = {
        let sm = state.sm.read().unwrap();
        sm.get_data_token(&dsh_core::token_hash(RAW_TOKEN))
            .unwrap()
            .unwrap()
            .id
    };
    state
        .sm
        .write()
        .unwrap()
        .apply(
            &Command::ProjectTokenRevoke {
                project: "p".into(),
                token_id: id,
            },
            50,
        )
        .unwrap();
    let err = ok.get_config(get_req("p")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let _ = &mut plain;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_delivers_publish_events() {
    let (url, state) = start_server().await;
    let mut client = authed_client(&url, RAW_TOKEN).await;

    // 订阅（after_version=2 = 当前活动版本）→ 只收后续事件
    let mut stream = client
        .watch(WatchRequest {
            project: "p".into(),
            branch: "dev".into(),
            after_version: 2,
        })
        .await
        .unwrap()
        .into_inner();

    // 经 ApiState 发布 v3（写路径带 hub 广播）：先写草稿再发布
    state
        .publish
        .update_draft(
            &ProjectId("p".into()),
            &BranchName("dev".into()),
            vec![dsh_core::command::DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("10.0.0.2".into()),
            }],
            vec![],
            vec![],
            None,
            "test",
        )
        .await
        .unwrap();
    state
        .publish
        .publish(
            &ProjectId("p".into()),
            &BranchName("dev".into()),
            "v3",
            "r3",
            "test",
        )
        .await
        .unwrap();

    let ev = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("watch timeout")
        .unwrap()
        .expect("stream message");
    assert_eq!(ev.version, 3);
    assert!(!ev.changes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_members_requires_valid_token() {
    let (url, _state) = start_server().await;
    // 无 token → Unauthenticated
    let mut plain = client_at(&url).await;
    let err = plain.list_members(ListMembersRequest {}).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    // 任一有效项目 token → 通过鉴权；dev-single 无 raft → FailedPrecondition
    let mut authed = authed_client(&url, RAW_TOKEN).await;
    let err = authed
        .list_members(ListMembersRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

// ==================== G3 灰度数据面（design/g3-dataplane.md，D26/D27/D25） ====================

/// G3：gRPC get_config / get_item 按身份 resolve——命中读灰度快照、未命中/无身份读稳定（D26/D27/Q6）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gray_data_plane_resolves_by_identity() {
    let (url, state) = start_server().await;
    let mut client = authed_client(&url, RAW_TOKEN).await;

    // 直接写状态机：新草稿（host=gray-host）→ GrayPublish（规则 zone=cn-north-1）
    {
        let mut sm = state.sm.write().unwrap();
        sm.apply(
            &Command::DraftUpdate {
                project: "p".into(),
                branch: BranchName("dev".into()),
                updates: vec![dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("gray-host".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            100,
        )
        .unwrap();
        sm.apply(
            &Command::GrayPublish {
                project: "p".into(),
                branch: BranchName("dev".into()),
                rule: dsh_core::model::GrayRule {
                    match_labels: vec![dsh_core::model::LabelSelector {
                        key: "zone".into(),
                        value: "cn-north-1".into(),
                    }],
                    ip_cidrs: vec![],
                    percentage: None,
                },
                comment: "g".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            101,
        )
        .unwrap();
    }
    let north: std::collections::HashMap<String, String> =
        [("zone".to_string(), "cn-north-1".to_string())].into();
    let south: std::collections::HashMap<String, String> =
        [("zone".to_string(), "cn-south-1".to_string())].into();

    // ① 命中（instance_id + labels）→ 灰度内容 + gray=true + resolved_version=gray_seq
    let snap = client
        .get_config(GetConfigRequest {
            project: "p".into(),
            branch: "dev".into(),
            version: 0,
            instance_id: "web-1".into(),
            labels: north.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(snap.gray, "身份命中 → gray=true");
    assert_eq!(snap.resolved_version, 1, "resolved_version = gray_seq");
    let host = snap.groups.get("redis").unwrap().items.get("host").unwrap();
    match &host.data {
        Some(dsh_api::grpc::value::Data::StrValue(s)) => assert_eq!(s, "gray-host"),
        other => panic!("expected str, got {other:?}"),
    }

    // ② 未命中 → 稳定版 + gray=false（active=3：testkit v2 + secret v3）
    let snap2 = client
        .get_config(GetConfigRequest {
            project: "p".into(),
            branch: "dev".into(),
            version: 0,
            instance_id: "web-2".into(),
            labels: south.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!snap2.gray);
    assert_eq!(snap2.resolved_version, 3);
    let host2 = snap2
        .groups
        .get("redis")
        .unwrap()
        .items
        .get("host")
        .unwrap();
    match &host2.data {
        Some(dsh_api::grpc::value::Data::StrValue(s)) => assert_eq!(s, "10.0.0.1"),
        other => panic!("expected str, got {other:?}"),
    }

    // ③ 无身份（旧客户端）→ 稳定版（Q2 向后兼容）
    let snap3 = client
        .get_config(GetConfigRequest {
            project: "p".into(),
            branch: "dev".into(),
            version: 0,
            instance_id: String::new(),
            labels: std::collections::HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!snap3.gray, "无身份永不进灰度（Q2）");

    // ④ get_item 同分流（Q6）
    let item = client
        .get_item(GetItemRequest {
            project: "p".into(),
            branch: "dev".into(),
            group: "redis".into(),
            key: "host".into(),
            version: 0,
            instance_id: "web-1".into(),
            labels: north,
        })
        .await
        .unwrap()
        .into_inner();
    match &item.value.unwrap().data {
        Some(dsh_api::grpc::value::Data::StrValue(s)) => {
            assert_eq!(s, "gray-host", "get_item 必须同样 resolve")
        }
        other => panic!("expected str, got {other:?}"),
    }
}

/// G3/D25：gRPC watch 灰度事件永不按版本过滤——gray:true 且 version ≤ last（active 未变）
/// 的 GrayPublish 事件仍投递（Q4：promote/abort 补发不丢）；last 游标不因 gray 事件倒挂。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gray_watch_delivers_gray_events() {
    let (url, state) = start_server().await;
    let mut client = authed_client(&url, RAW_TOKEN).await;

    // 订阅：after_version=3（当前 active）→ last=3
    let mut stream = client
        .watch(WatchRequest {
            project: "p".into(),
            branch: "dev".into(),
            after_version: 3,
        })
        .await
        .unwrap()
        .into_inner();

    // 灰度发布（sm.apply + hub 手动广播，模拟写路径）——事件 gray=true、version=3（active 未变 ≤ last）
    let events = {
        let mut sm = state.sm.write().unwrap();
        sm.apply(
            &Command::DraftUpdate {
                project: "p".into(),
                branch: BranchName("dev".into()),
                updates: vec![dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("gray-host".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            100,
        )
        .unwrap();
        sm.apply(
            &Command::GrayPublish {
                project: "p".into(),
                branch: BranchName("dev".into()),
                rule: dsh_core::model::GrayRule {
                    match_labels: vec![dsh_core::model::LabelSelector {
                        key: "zone".into(),
                        value: "cn-north-1".into(),
                    }],
                    ip_cidrs: vec![],
                    percentage: None,
                },
                comment: "g".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            101,
        )
        .unwrap()
    };
    for e in &events {
        state.hub.publish(e);
    }

    // GrayPublish 事件必须投递（尽管 version=3 == last）
    let ev = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("watch timeout")
        .unwrap()
        .expect("stream message");
    assert!(ev.gray, "灰度事件 gray=true");
    assert_eq!(
        ev.version, 3,
        "GrayPublish 事件 version=active（未变）仍投递（D25）"
    );

    // 普通发布 v4 → 版本推进，正常投递（验证游标未因 gray 事件倒挂）
    state
        .publish
        .update_draft(
            &ProjectId("p".into()),
            &BranchName("dev".into()),
            vec![dsh_core::command::DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("10.0.0.3".into()),
            }],
            vec![],
            vec![],
            None,
            "test",
        )
        .await
        .unwrap();
    state
        .publish
        .publish(
            &ProjectId("p".into()),
            &BranchName("dev".into()),
            "v4",
            "r4",
            "test",
        )
        .await
        .unwrap();
    let ev = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("watch timeout")
        .unwrap()
        .expect("stream message");
    assert!(!ev.gray);
    assert_eq!(
        ev.version, 4,
        "last 游标未因 gray 事件倒挂（普通事件正常推进）"
    );
}
