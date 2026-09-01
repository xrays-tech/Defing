//! KV 键构造（对齐 design-v2 §3.2 前缀布局，模块 01 §4）。

use crate::model::{BranchName, ProjectId};

pub const K_PROJECT: &str = "p/";
pub const K_STRUCT: &str = "/struct";
pub const K_STRUCT_DRAFT: &str = "/struct-draft";
pub const K_BRANCH: &str = "/b/";
pub const K_STATE: &str = "/state";
pub const K_VERSION: &str = "/v/";
/// 共享引用选择在分支状态 BranchState.shared_bindings（设计 shared-ref-branch-scope）；
/// 结构仅声明 ItemDef.shared 标记，无独立引用键。
pub const K_SHARED: &str = "sh/";
pub const K_SHARED_DRAFT: &str = "sh-draft/";
pub const K_SESSION: &str = "sess/admin";
/// 管理员密码哈希（set-password 落状态机，集群一致；登录时优先于节点配置校验）。
pub const K_ADMIN_PW: &str = "sess/admin-pw";
pub const K_AUDIT: &str = "audit/";
/// 审计 seq 计数键（位于 audit/ 前缀内；get_prefix 扫描时按 20 位数字后缀区分条目）。
pub const K_AUDIT_SEQ: &str = "audit/seq";
pub const K_IDX_PNAME: &str = "idx/pname/";
/// 项目管理员账号前缀：adm/pa/{username} → ProjectAdminAccount。
pub const K_PA_ACCOUNT: &str = "adm/pa/";
/// 项目访问令牌键：tok/{hash}（扁平；数据面鉴权单次 KV 读）。
pub const K_DATA_TOKEN: &str = "tok/";

pub fn data_token_key(hash: &str) -> String {
    format!("{K_DATA_TOKEN}{hash}")
}
/// 项目管理员会话前缀：sess/pa/{username} → AdminSession（每账号单会话）。
pub const K_PA_SESSION: &str = "sess/pa/";

pub fn project_key(id: &ProjectId) -> String {
    format!("{K_PROJECT}{}", id.as_str())
}
pub fn struct_key(id: &ProjectId) -> String {
    format!("{K_PROJECT}{}{K_STRUCT}", id.as_str())
}
pub fn struct_draft_key(id: &ProjectId) -> String {
    format!("{K_PROJECT}{}{K_STRUCT_DRAFT}", id.as_str())
}
pub fn branch_state_key(id: &ProjectId, branch: &BranchName) -> String {
    format!(
        "{K_PROJECT}{}{K_BRANCH}{}{K_STATE}",
        id.as_str(),
        branch.as_str()
    )
}
pub fn version_key(id: &ProjectId, branch: &BranchName, no: u64) -> String {
    format!(
        "{K_PROJECT}{}{K_BRANCH}{}{K_VERSION}{no}",
        id.as_str(),
        branch.as_str()
    )
}
/// 版本值快照（M1：每版本存全量；M2 起按 checkpoint 规则存 diff）。
pub fn snapshot_key(id: &ProjectId, branch: &BranchName, no: u64) -> String {
    format!(
        "{K_PROJECT}{}{K_BRANCH}{}{K_VERSION}{no}/snap",
        id.as_str(),
        branch.as_str()
    )
}
/// 版本 diff（perf 方案② D3：非 checkpoint 版本存 diff 而非全量快照）。
/// 布局：p/{pid}/b/{branch}/v/{no}/diff —— 与 snapshot_key 同前缀不同后缀，
/// `version_history` 前缀扫描时按 `/snap` 后缀跳过逻辑需同步排除 `/diff`。
pub fn diff_key(id: &ProjectId, branch: &BranchName, no: u64) -> String {
    format!(
        "{K_PROJECT}{}{K_BRANCH}{}{K_VERSION}{no}/diff",
        id.as_str(),
        branch.as_str()
    )
}
/// 灰度快照前缀（G2/Q1：独立于 v/ 版本号空间，不与 active_version 冲突——双指针同号互覆盖问题）。
/// 布局：p/{pid}/b/{branch}/gray-snap/{seq}；gray_seq 为分支级独立单调递增序号。
pub const K_GRAY_SNAP: &str = "/gray-snap/";
/// 灰度快照键：p/{pid}/b/{branch}/gray-snap/{seq}（全量 SnapshotMap；仅存当前灰度，非历史链）。
pub fn gray_snap_key(id: &ProjectId, branch: &BranchName, seq: u64) -> String {
    format!(
        "{K_PROJECT}{}{K_BRANCH}{}{K_GRAY_SNAP}{seq}",
        id.as_str(),
        branch.as_str()
    )
}
pub fn branch_prefix(id: &ProjectId, branch: &BranchName) -> String {
    format!("{K_PROJECT}{}{K_BRANCH}{}", id.as_str(), branch.as_str())
}
/// 共享项键：sh/{key}（扁平库，无分组）。key 已由 validator::valid_key_name 约束
/// （1-128 位 [A-Za-z0-9._-]，无 `/` 与 HTML 特殊字符）。
pub fn shared_key(key: &str) -> String {
    format!("{K_SHARED}{key}")
}
pub fn shared_draft_key(key: &str) -> String {
    format!("{K_SHARED_DRAFT}{key}")
}
pub fn session_key() -> &'static str {
    K_SESSION
}
/// 管理员会话前缀（多会话：sess/admin/{session_id}；批量踢/审计用）。
pub const K_SESSION_PREFIX: &str = "sess/admin/";
/// 多会话管理员会话键：sess/admin/{session_id}（perf/multisession 改造）。
pub fn session_key_with(sid: &str) -> String {
    format!("{K_SESSION_PREFIX}{sid}")
}
/// 项目管理员账号键：adm/pa/{username}。
pub fn project_admin_key(username: &str) -> String {
    format!("{K_PA_ACCOUNT}{username}")
}
/// 项目管理员会话键：sess/pa/{username}（旧单会话，兼容）。
pub fn pa_session_key(username: &str) -> String {
    format!("{K_PA_SESSION}{username}")
}
/// 项目管理员会话前缀：sess/pa/{username}/（多会话批量操作）。
pub fn pa_session_prefix(username: &str) -> String {
    format!("{K_PA_SESSION}{username}/")
}
/// 多会话 PA 会话键：sess/pa/{username}/{session_id}。
pub fn pa_session_key_with(username: &str, sid: &str) -> String {
    format!("{K_PA_SESSION}{username}/{sid}")
}
pub fn audit_key(seq: u64) -> String {
    format!("{K_AUDIT}{seq:020}")
}
pub fn idx_pname(name: &str) -> String {
    format!("{K_IDX_PNAME}{name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_shapes() {
        let id: ProjectId = "order-service".into();
        let b: BranchName = "prod".into();
        assert_eq!(project_key(&id), "p/order-service");
        assert_eq!(struct_key(&id), "p/order-service/struct");
        assert_eq!(branch_state_key(&id, &b), "p/order-service/b/prod/state");
        assert_eq!(version_key(&id, &b, 12), "p/order-service/b/prod/v/12");
        assert_eq!(
            snapshot_key(&id, &b, 12),
            "p/order-service/b/prod/v/12/snap"
        );
        assert_eq!(diff_key(&id, &b, 12), "p/order-service/b/prod/v/12/diff");
        assert_eq!(
            gray_snap_key(&id, &b, 3),
            "p/order-service/b/prod/gray-snap/3"
        );
        assert_eq!(
            gray_snap_key(&id, &b, 12),
            "p/order-service/b/prod/gray-snap/12"
        );
        assert_eq!(data_token_key("ab12"), "tok/ab12");
        assert_eq!(shared_key("timeout"), "sh/timeout");
        assert_eq!(shared_draft_key("timeout"), "sh-draft/timeout");
        assert_eq!(idx_pname("order-service"), "idx/pname/order-service");
        assert_eq!(audit_key(7), "audit/00000000000000000007");
    }
}
