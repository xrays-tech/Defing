---
layout: default
title: 08 构建脚本取值（curl）
prev: {title: 07 访问令牌与 SDK, url: /07-tokens/}
next: {title: 09 管理员与审计, url: /09-admin/}
---
# 08 构建脚本取值（curl）

编译 / 构建脚本无需引入 SDK，直接 `curl` 拉取指定分支的配置，输出任意格式 —— 特别适合在 CI / 编译脚本里预先获取参数。

## 8.1 接口

```text
GET /v1/projects/{project}/branches/{branch}/config?format={yaml|json|toml|env}&version={n}
鉴权：Authorization: Bearer <项目访问令牌>   （或 ?token=<令牌> 查询参数）
```

| 参数 | 说明 |
|---|---|
| `project` | 项目名 |
| `branch` | 分支名（dev / test / prod / 自定义） |
| `format` | `yaml`（默认）/ `json` / `toml` / `env` |
| `version` | 可选，指定版本（缺省 = 活动版本） |

## 8.2 用法示例

```bash
# YAML 输出
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=yaml" \
  -H "Authorization: Bearer <项目访问令牌>"

# JSON 输出（?token= 查询参数方式，适合无法带自定义头的环境）
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=json&token=<项目访问令牌>"

# 直接落盘 .env 文件
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=env" \
  -H "Authorization: Bearer <项目访问令牌>" > .env
```

输出示例（`format=env`，测试实例 `horizon-compile/dev` 实测）：

```text
CC=wefwefwefewe
CXX=g++
JOBS=8
OPTIMIZE=2
VERBOSE=true
HEALTHCHECK={"interval":30,"path":"/healthz"}
REGISTRY=registry.cn-north-1.example.com
REPLICAS=1
TAG=dev-2025.08
HOST=10.0.0.1
LOG_LEVEL=info
PORT=6379
TIMEOUT=60
```

> ENV 约定：`KEY=VALUE`，键转大写、**无分组前缀**（组仅组织语义，不进入 .env 输出）；含空格 / 特殊字符自动加引号转义；secret 对项目令牌请求**解密返回真值**（构建脚本取用，记录 `config_reveal` 审计），见 §8.4。

## 8.3 在 Admin UI 中获取命令

项目「访问令牌」页签顶部直接展示当前项目的 curl 命令（自动带入项目名与当前分支，格式可切换）：

![访问令牌与 curl]({{ site.baseurl }}/assets/images/07-tokens.png)

复制命令后把 `<项目访问令牌>` 替换为你的令牌即可使用。

## 8.4 编译脚本实战

```bash
# 编译前拉取 prod 分支配置并生成 .env
curl -sf "$DEFING_URL/v1/projects/$APP/branches/prod/config?format=env" \
  -H "Authorization: Bearer $DEFING_TOKEN" > .env
source .env
make build  # 编译时使用环境变量
```

- 用 `curl -f`（失败即报错），令牌过期 / 分支不存在时构建脚本直接失败而不是静默用旧值
- **secret 在 /config 渲染端点对项目 token 授权请求解密返回**（构建脚本可取真值，如 .env 落盘）；
  令牌即机器凭据，请妥善保管；SDK 快照（/snapshot、gRPC）仍恒掩码（proto masked 语义）

## 下一步

- [09 管理员与审计]({{ site.baseurl }}/09-admin/)：管理员账号与审计日志
