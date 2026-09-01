//! dsh-testkit —— 测试夹具（模块 13）：演示结构/值构造 + 项目播种。
//! 供集成测试复用（grpc_data_plane 等），避免各测试重复样板。

use std::sync::RwLock;

use dsh_core::command::Command;
use dsh_core::model::{BranchName, GroupDef, ItemDef, ProjectId, PublishPolicy, Value, ValueType};
use dsh_core::StateMachine;

/// 演示项目结构：redis 组（host 必填 string / port int / pass secret）。
pub fn demo_structure() -> Vec<GroupDef> {
    vec![GroupDef {
        name: "redis".into(),
        items: vec![
            ItemDef {
                key: "host".into(),
                ty: ValueType::String,
                required: true,
                secret: false,
                validate: None,
                description: None,
                shared: false,
            },
            ItemDef {
                key: "port".into(),
                ty: ValueType::Int,
                required: false,
                secret: false,
                validate: None,
                description: None,
                shared: false,
            },
            ItemDef {
                key: "pass".into(),
                ty: ValueType::Secret,
                required: false,
                secret: true,
                validate: None,
                description: None,
                shared: false,
            },
        ],
    }]
}

pub fn string_val(s: &str) -> Value {
    Value::String(s.into())
}

pub fn int_val(i: i64) -> Value {
    Value::Int(i)
}

/// 播种演示项目：建项目 → 结构草稿（demo_structure）→ 发布结构 →
/// dev 草稿（host/port）→ 发布值版本。返回 (项目, dev 分支)。
pub fn seed_demo_project(
    sm: &RwLock<StateMachine>,
    project: &str,
) -> Result<(ProjectId, BranchName), dsh_core::Error> {
    let pid = ProjectId(project.into());
    let dev = BranchName("dev".into());
    let mut g = sm.write().map_err(|_| dsh_core::Error::internal("lock"))?;
    g.apply(
        &Command::ProjectCreate {
            name: project.into(),

            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
        1,
    )?;
    g.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 1,
            groups: demo_structure(),

            operator: String::new(),
        },
        2,
    )?;
    g.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "init structure".into(),
            request_id: "s1".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        3,
    )?;
    g.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![
                dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: string_val("10.0.0.1"),
                },
                dsh_core::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "port".into(),
                    value: int_val(6379),
                },
            ],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        4,
    )?;
    g.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "dev v2".into(),
            request_id: "r1".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        5,
    )?;
    Ok((pid, dev))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::InMemoryStore;

    #[test]
    fn seed_produces_published_values() {
        let sm = RwLock::new(StateMachine::new(Box::new(InMemoryStore::new())));
        let (pid, dev) = seed_demo_project(&sm, "demo").unwrap();
        let snap = sm.read().unwrap().get_config(&pid, &dev, 0).unwrap();
        assert_eq!(snap.version, 2);
        assert_eq!(
            snap.groups["redis"]["host"],
            Value::String("10.0.0.1".into())
        );
        assert_eq!(snap.groups["redis"]["port"], Value::Int(6379));
    }

    #[test]
    fn fixture_structure_has_secret() {
        let groups = demo_structure();
        assert!(groups[0].items.iter().any(|i| i.ty == ValueType::Secret));
    }
}
