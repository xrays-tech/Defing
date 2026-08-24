---
layout: default
title: 快速开始
prev: {title: 首页, url: /}
next: {title: 01 部署与启动, url: /01-install/}
---
# 快速开始

10 分钟走完核心链路：**登录 → 建项目 → 定义结构 → 填值发布 → 应用读取**。

## 1. 启动服务

```bash
defing --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:8384
# 管理面: http://127.0.0.1:8384/admin
# 启动时会打印一行「开发数据面 token」（SDK / curl 拉取配置用，全局有效）
```

> `--dev-single` 为单机内存模式（重启即清空），适合联调；生产部署见 [01 部署与启动]({{ site.baseurl }}/01-install/)。

## 2. 登录 Admin UI

浏览器打开 `http://127.0.0.1:8384/admin`：

![登录页]({{ site.baseurl }}/assets/images/01-login.png)

输入管理员密码（`--admin-password` 指定的值）点击「登 录」。**全局管理员：用户名留空**；项目管理员登录需填写自己的用户名（见 [09 管理员与审计]({{ site.baseurl }}/09-admin/)）。

> 未显式指定 `--admin-password` 时，服务启动会随机生成并打印。

## 3. 创建项目

登录后点击「新建项目」，输入项目名（小写字母 / 数字 / 连字符）：

![配置管理首页]({{ site.baseurl }}/assets/images/02-home.png)

项目创建后自动带 `dev / test / prod` 三个分支。

## 4. 定义结构并发布

进入项目后，切到「结构」页签 → 添加分组与配置项 → 「保存草稿」→「发布结构」：

![结构页]({{ site.baseurl }}/assets/images/04-structure.png)

结构定义了「有哪些配置项、什么类型、是否必填」，**发布后对所有分支同时生效**。

## 5. 填写草稿并发布版本

回到「草稿」页签，为各配置项填写值（无草稿时自动显示已发布版本的值），保存草稿后点「发布版本」：

![草稿页]({{ site.baseurl }}/assets/images/03-draft.png)

发布后，订阅该分支的客户端会收到变更事件。

## 6. 应用读取配置

**方式一（SDK）**：

```ts
import { ConfigClient } from './sdk/ts/src/index.ts';
const c = new ConfigClient([{ grpc: '127.0.0.1:8383', http: 'http://127.0.0.1:8384' }], {
  token: '<项目访问令牌>',
});
const snap = await c.get('my-app', 'dev');   // 读 dev 分支配置
```

**方式二（curl，构建脚本友好）**：

```bash
curl -s "http://127.0.0.1:8384/v1/projects/my-app/branches/dev/config?format=yaml" \
  -H "Authorization: Bearer <项目访问令牌>"
```

令牌在项目页「访问令牌」页签创建（详见 [07 访问令牌与 SDK]({{ site.baseurl }}/07-tokens/) 与 [08 构建脚本取值]({{ site.baseurl }}/08-curl/)）。

## 下一步

- [01 部署与启动]({{ site.baseurl }}/01-install/)：单机 / 集群 / 参数详解
- [02 项目与分支]({{ site.baseurl }}/02-project/)：项目与分支管理
- [04 草稿与发布]({{ site.baseurl }}/04-draft/)：编辑、校验、版本与回滚
- [05 灰度发布]({{ site.baseurl }}/05-gray/)：灰度上线与回滚
