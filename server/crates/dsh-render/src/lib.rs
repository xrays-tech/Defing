//! 多格式渲染（模块 08）：物化快照 → 规范化 JSON 树 → YAML/TOML/JSON/ENV。
//! 输入为解密后的普通值（secret 已由上层解密为明文或掩码）。

use std::collections::BTreeMap;

use dsh_core::error::{Error, ErrorKind};
use dsh_core::model::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Yaml,
    Toml,
    Json,
    Env,
}

impl Format {
    pub fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "yaml" => Ok(Format::Yaml),
            "toml" => Ok(Format::Toml),
            "json" => Ok(Format::Json),
            "env" => Ok(Format::Env),
            other => Err(Error::validation(format!("unsupported format: {other}"))),
        }
    }
}

/// 渲染器。
pub struct Renderer;

impl Renderer {
    /// 将配置快照（group → key → Value）渲染为指定格式。
    /// 注意：secret 值由调用方预先解密；若仍有密文则输出掩码占位。
    pub fn render(
        &self,
        groups: &BTreeMap<String, BTreeMap<String, Value>>,
        format: Format,
    ) -> Result<String, Error> {
        let tree = plain_tree(groups);
        match format {
            Format::Yaml => serde_yaml::to_string(&tree)
                .map_err(|e| Error::new(ErrorKind::Validation, format!("yaml: {e}"))),
            Format::Json => serde_json::to_string_pretty(&tree)
                .map_err(|e| Error::new(ErrorKind::Validation, format!("json: {e}"))),
            Format::Toml => toml::to_string(&tree).map_err(|e| {
                Error::new(
                    ErrorKind::Validation,
                    format!("toml（键需为合法标识符）: {e}"),
                )
            }),
            Format::Env => Ok(render_env(&tree)),
        }
    }
}

/// .env 格式：`KEY=VALUE`（键转大写，无分组前缀——组仅组织语义，不进入 .env 输出）。
/// 注意：跨组同名 key 会输出重复行（dotenv 后写覆盖）；需区分时在键命名上体现。
/// 值：含空白/#/引号/反斜杠/换行的字符串加双引号转义；数组逗号连接；其余按字面输出。
fn render_env(tree: &BTreeMap<String, BTreeMap<String, serde_json::Value>>) -> String {
    let mut out = String::new();
    for items in tree.values() {
        for (k, v) in items {
            let key = k.to_uppercase();
            out.push_str(&format!("{key}={}\n", env_value(v)));
        }
    }
    out
}

fn env_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => {
            if s.is_empty() || s.contains([' ', '#', '"', '\\', '\n', '\r']) {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                s.clone()
            }
        }
        serde_json::Value::Array(items) => items
            .iter()
            .map(|x| x.as_str().unwrap_or(""))
            .collect::<Vec<_>>()
            .join(","),
        other => other.to_string(),
    }
}

/// Value → serde_json 普通值（去除 type 标签）。
fn plain_value(v: &Value) -> serde_json::Value {
    match v {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Json(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.clone())),
        Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
        Value::Secret(_) => serde_json::Value::String("***".into()),
    }
}

fn plain_tree(
    groups: &BTreeMap<String, BTreeMap<String, Value>>,
) -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
    groups
        .iter()
        .map(|(g, items)| {
            let m = items
                .iter()
                .map(|(k, v)| (k.clone(), plain_value(v)))
                .collect();
            (g.clone(), m)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::model::{Ciphertext, Value};

    fn sample() -> BTreeMap<String, BTreeMap<String, Value>> {
        let mut groups = BTreeMap::new();
        groups.insert(
            "redis".into(),
            BTreeMap::from([
                ("host".into(), Value::String("127.0.0.1".into())),
                ("port".into(), Value::Int(6379)),
                ("tls".into(), Value::Bool(true)),
            ]),
        );
        groups.insert(
            "db".into(),
            BTreeMap::from([(
                "password".into(),
                Value::Secret(Ciphertext {
                    enc: "aes-256-gcm".into(),
                    v: 1,
                    dek_v: 1,
                    nonce: "n".into(),
                    ct: "c".into(),
                    edek: "e".into(),
                    edek_nonce: "en".into(),
                }),
            )]),
        );
        groups
    }

    #[test]
    fn render_json() {
        let r = Renderer;
        let out = r.render(&sample(), Format::Json).unwrap();
        assert!(out.contains("\"redis\""));
        assert!(out.contains("127.0.0.1"));
        assert!(out.contains("***")); // secret 掩码
    }

    #[test]
    fn render_yaml() {
        let r = Renderer;
        let out = r.render(&sample(), Format::Yaml).unwrap();
        assert!(out.contains("host: 127.0.0.1"));
        assert!(out.contains("port: 6379"));
    }

    #[test]
    fn render_toml() {
        let r = Renderer;
        let out = r.render(&sample(), Format::Toml).unwrap();
        assert!(out.contains("[redis]"));
        assert!(out.contains("host = \"127.0.0.1\""));
        assert!(out.contains("port = 6379"));
    }

    #[test]
    fn render_env() {
        let r = Renderer;
        let out = r.render(&sample(), Format::Env).unwrap();
        assert!(out.contains("HOST=127.0.0.1"), "{out}");
        assert!(out.contains("PORT=6379"), "{out}");
        assert!(out.contains("TLS=true"), "{out}");
        assert!(out.contains("PASSWORD=***"), "{out}"); // secret 掩码
                                                        // 无分组前缀：输出仅含键（大写）
        assert!(!out.contains("__"), "group 前缀已去除: {out}");
    }

    #[test]
    fn render_env_quotes_special_values() {
        let mut groups = BTreeMap::new();
        groups.insert(
            "app".into(),
            BTreeMap::from([
                ("greeting".into(), Value::String("hello world".into())),
                ("flag".into(), Value::String("a#b".into())),
                ("tags".into(), Value::Array(vec!["x".into(), "y".into()])),
            ]),
        );
        let r = Renderer;
        let out = r.render(&groups, Format::Env).unwrap();
        assert!(out.contains("GREETING=\"hello world\""), "{out}");
        assert!(out.contains("FLAG=\"a#b\""), "{out}");
        assert!(out.contains("TAGS=x,y"), "{out}");
    }

    #[test]
    fn json_yaml_equivalence() {
        // 简单等价性：JSON 解析与 YAML 解析结果一致（浮点/整数规范化）
        let r = Renderer;
        let j = r.render(&sample(), Format::Json).unwrap();
        let y = r.render(&sample(), Format::Yaml).unwrap();
        let jv: serde_json::Value = serde_json::from_str(&j).unwrap();
        let yv: serde_json::Value = serde_yaml::from_str(&y).unwrap();
        assert_eq!(jv, yv);
    }
}
