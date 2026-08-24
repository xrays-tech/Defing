---
layout: default
title: 07 访问令牌与 SDK
prev: {title: 06 共享库, url: /06-shared/}
next: {title: 08 构建脚本取值（curl）, url: /08-curl/}
---
# 07 访问令牌与 SDK

数据面（拉取配置）一律要求**项目访问令牌**：每项目独立、可吊销、轮换零中断。令牌在 Admin UI 管理（仅全局管理员）。

## 7.1 创建令牌

项目内「访问令牌」页签：

![访问令牌]({{ site.baseurl }}/assets/images/07-tokens.png)

1. 点「创建令牌」→ 输入名称（如 `build-svc`）
2. 响应弹窗**明文仅展示这一次**，立即复制保存（服务端只存 SHA-256，无法再次查看）
3. 列表显示：名称 / ID / 创建人 / 时间 / 状态，可随时「吊销」

> 令牌**永不过期**，管理靠主动吊销；泄露即吊销重建。
> 同一项目可建多个令牌：轮换 = 新令牌先分发到客户端，旧令牌下次发版后吊销，零中断。

同一页签顶部提供**构建脚本 curl 命令**（格式可切 YAML / JSON / TOML / ENV），详见 [08 构建脚本取值]({{ site.baseurl }}/08-curl/)。

## 7.2 三语言 SDK

令牌通过 SDK 的 `token` 参数携带，传输为 `Authorization: Bearer <token>`（gRPC metadata 同构）。

### TypeScript

```ts
import { ConfigClient } from './sdk/ts/src/index.ts';

const c = new ConfigClient([{ grpc: '127.0.0.1:8383', http: 'http://127.0.0.1:8384' }], {
  token: '<项目访问令牌>',
});

const snap = await c.get('my-app', 'dev');        // 读 dev 分支活动版本
const item = await c.getItem('my-app', 'dev', 'redis', 'host');
c.watch('my-app', 'dev', (e) => console.log(e));  // 订阅发布事件（断线 after_version 续传）
const members = await c.listMembers();            // 集群成员（端点池刷新）
```

### Go

```go
c := configclient.NewGrpc("127.0.0.1:8383", "<项目访问令牌>") // gRPC 数据面
// 或 HTTP 降级：
c2 := configclient.New([]string{"http://127.0.0.1:8384"}, "<项目访问令牌>")
snap, _ := c.Get(ctx, "my-app", "dev", 0)
```

### Python

```python
from config_client import ConfigClient

c = ConfigClient([{'grpc': '127.0.0.1:8383', 'http': 'http://127.0.0.1:8384'}],
                token='<项目访问令牌>')
snap = c.get('my-app', 'dev')
```

## 7.3 SDK 行为要点

| 能力 | 说明 |
|---|---|
| 端点池 failover | 多端点，连接失败指数退避切换 |
| gRPC 优先 / HTTP 降级 | 端点含 `grpc` 地址走 gRPC（:8383），否则 HTTP/SSE |
| watch 断线续传 | 重连携带 `after_version`，不丢事件 |
| secret 脱敏 | SDK 快照 / gRPC 恒返回 `***`；渲染端点（构建脚本）按项目令牌解密真值并审计，见 [08 §8.4]({{ site.baseurl }}/08-curl/) |
| 灰度身份 | 传 `instance` / `labels` 参与灰度解析（见 [05 灰度发布]({{ site.baseurl }}/05-gray/)） |

## 下一步

- [08 构建脚本取值（curl）]({{ site.baseurl }}/08-curl/)：编译脚本无需 SDK 直接拉配置
