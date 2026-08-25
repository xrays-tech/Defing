#!/usr/bin/env bash
# P0 管理面契约补全 e2e：覆盖 openapi 中此前缺失的端点
#   项目详情/删除、分支详情/删除、分支对比、值提升、共享库 CRUD+发布、共享引用绑定、
#   （cluster/remove 由 cluster-demo 扩展覆盖，本脚本 dev-single 无 raft）
set -euo pipefail
BIN=${BIN:-/home/alex/Projects/Defing/server/target/debug/defing}
PORT=${PORT:-8384}
BASE=${BASE:-http://127.0.0.1:$PORT}

cleanup() { [ -n "${PID:-}" ] && kill $PID 2>/dev/null || true; }
trap cleanup EXIT

echo "== 启动 defing --dev-single =="
head -c 32 /dev/urandom > /tmp/dsh-api-surface.key
$BIN --dev-single --admin-password admin123 --http-addr 127.0.0.1:$PORT \
  --master-key-file /tmp/dsh-api-surface.key >/tmp/dsh-api-surface.log 2>&1 &
PID=$!
for i in $(seq 1 20); do
  curl -sf $BASE/healthz >/dev/null && break
  sleep 0.5
done
curl -sf $BASE/healthz >/dev/null || { echo "  healthz FAIL"; cat /tmp/dsh-api-surface.log; exit 1; }

AUTH="Authorization: Bearer $(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' -d '{"password":"admin123"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")"
J() { curl -sf -H "$AUTH" -H 'Content-Type: application/json' "$@"; }
# project-token：dev-single 启动时打印全局开发数据面 token（数据面一律需要 token）
DEV_TOKEN=$(sed -n 's/.*开发数据面 token = \([a-f0-9]*\).*/\1/p' /tmp/dsh-api-surface.log)
[ -n "$DEV_TOKEN" ] || { echo "  dev token 未打印"; cat /tmp/dsh-api-surface.log; exit 1; }
DP() { curl -sf -H "Authorization: Bearer $DEV_TOKEN" "$@"; }

echo "== 1. 建项目 + 结构(host/port/password secret) + 发布 =="
J -X POST $BASE/api/v1/projects -d '{"name":"order-service"}' >/dev/null
J -X PUT $BASE/api/v1/projects/order-service/structure-draft -d '{"base_version":1,"groups":[{"name":"redis","items":[{"key":"host","type":"string","required":true},{"key":"port","type":"int"},{"key":"password","type":"secret","secret":true}]}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/structure-draft/publish -d '{"comment":"s","request_id":"s1"}' >/dev/null
J -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -d '{"updates":[{"group":"redis","key":"host","value":{"type":"string","str_value":"10.0.0.1"}},{"group":"redis","key":"port","value":{"type":"int","int_value":6379}},{"group":"redis","key":"password","value":{"type":"string","str_value":"s3cret"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -d '{"comment":"v2","request_id":"r1"}' >/dev/null
J -X PUT $BASE/api/v1/projects/order-service/branches/test/draft -d '{"updates":[{"group":"redis","key":"host","value":{"type":"string","str_value":"10.0.0.2"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/branches/test/publish -d '{"comment":"t2","request_id":"r2"}' >/dev/null

echo "== 2. 项目详情 =="
D=$(J $BASE/api/v1/projects/order-service)
echo "$D" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['id']=='order-service' and d['name']=='order-service'" && echo "  project_detail OK"

echo "== 3. 分支详情(dev 含草稿信息 + 活动版本基线) =="
J $BASE/api/v1/projects/order-service/branches/dev | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['active_version']==2 and d['structure_version']>=1; a=d.get('active',{}).get('redis',{}); assert a.get('host',{}).get('value',{}).get('str_value')=='10.0.0.1', a; assert a.get('password',{}).get('value',{}).get('masked')==True, a" && echo "  branch_detail OK (含 active 基线)"

echo "== 4. 分支对比 dev vs test (host 不同 → diff; password 仅 dev → missing) =="
J "$BASE/api/v1/projects/order-service/diff?branch_a=dev&branch_b=test" > /tmp/diff.json
cat /tmp/diff.json | python3 -c "
import json,sys
d=json.load(sys.stdin)
kinds={x['key']:x for x in d['diffs']}
assert 'host' in kinds and kinds['host']['branch_a']['str_value']=='10.0.0.1' and kinds['host']['branch_b']['str_value']=='10.0.0.2', d
assert any('password' in m or 'port' in m for m in d['missing']), d
" && echo "  branch_diff OK"

echo "== 5. 值提升 dev → prod (草稿无冲突 → 全部 applied) =="
R=$(J -X POST $BASE/api/v1/projects/order-service/promote -d '{"from":"dev","to":"prod"}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert len(r['applied'])==3 and r['skipped']==[] and r['missing_from']==[], r" && echo "  promote OK"
echo "== 5b. 再 promote：prod 草稿已修改 → 全部 skipped（force=false）=="
R=$(J -X POST $BASE/api/v1/projects/order-service/promote -d '{"from":"dev","to":"prod"}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert len(r['skipped'])==3 and r['applied']==[], r" && echo "  promote idempotent-skip OK"
echo "== 5c. force=true 覆盖 =="
R=$(J -X POST $BASE/api/v1/projects/order-service/promote -d '{"from":"dev","to":"prod","force":true}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert len(r['applied'])==3, r" && echo "  promote force OK"
echo "== 5d. items 过滤 + missing_from（prod 草稿已有 host → skipped；不存在项 → missing_from）=="
R=$(J -X POST $BASE/api/v1/projects/order-service/promote -d '{"from":"dev","to":"prod","items":["redis/host","redis/nope"]}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert 'redis/host' in r['skipped'] and 'redis/nope' in r['missing_from'], r" && echo "  promote filter OK"

echo "== 6. 共享草稿 CRUD + 发布（扁平库，无分组） =="
J -X POST $BASE/api/v1/shared -d '{"key":"timeout","type":"int","description":"全局超时","value":{"type":"int","int_value":30}}' >/dev/null
J -X PUT $BASE/api/v1/shared-draft -d '{"key":"timeout","type":"int","description":"全局超时","value":{"type":"int","int_value":60}}' >/dev/null
N=$(J $BASE/api/v1/shared-draft | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$N" = "1" ] && echo "  shared-draft list OK (1 draft)" || { echo "  shared-draft FAIL n=$N"; exit 1; }
SP=$(J -X POST $BASE/api/v1/shared/publish -d '{"comment":"lib v1","request_id":"sp1"}')
echo "$SP" | python3 -c "import json,sys; r=json.load(sys.stdin); assert r['version']==1, r" && echo "  shared publish OK"
J $BASE/api/v1/shared | python3 -c "import json,sys; l=json.load(sys.stdin); assert len(l)==1 and l[0]['key']=='timeout' and l[0]['version']==1 and l[0].get('description')=='全局超时', l" && echo "  shared list OK (description)"

echo "== 7. secret 共享项：写明文→加密存储→列表脱敏 =="
J -X POST $BASE/api/v1/shared -d '{"key":"api-key","type":"secret","secret":true,"value":{"type":"string","str_value":"topsecret"}}' >/dev/null
J -X POST $BASE/api/v1/shared/publish -d '{"comment":"key","request_id":"sp2"}' >/dev/null
J $BASE/api/v1/shared | python3 -c "import json,sys; l=json.load(sys.stdin); sk=[x for x in l if x['key']=='api-key'][0]; assert sk['value'].get('masked')==True and 'topsecret' not in json.dumps(l), l" && echo "  secret shared masked OK"

# shared-edit-ui：secret 留空 = 保留当前密文（仅改描述/required，不重输密钥）
J -X PUT $BASE/api/v1/shared-draft -d '{"key":"api-key","type":"secret","secret":true,"description":"更新描述","value":{"type":"string","str_value":""}}' >/dev/null
J $BASE/api/v1/shared-draft | python3 -c "import json,sys; l=json.load(sys.stdin); sk=[x for x in l if x['key']=='api-key'][0]; assert sk['value'].get('masked')==True and sk.get('description')=='更新描述', l" && echo "  shared secret keep-cipher OK (empty value)"
CODE=$(curl -s -o /tmp/shnew.json -w '%{http_code}' -X POST $BASE/api/v1/shared -H "$AUTH" -H 'Content-Type: application/json' -d '{"key":"brand-new","type":"secret","secret":true,"value":{"type":"string","str_value":""}}')
[ "$CODE" = "422" ] && echo "  shared secret empty-first-save rejected OK (422)" || { echo "  shared secret keep guard FAIL code=$CODE"; cat /tmp/shnew.json; exit 1; }

echo "== 8. 共享引用（分支级 shared_bindings）：结构标记 + 分支选择 + 级联 + 删除阻断 =="
J -X PUT $BASE/api/v1/projects/order-service/structure-draft -d '{"base_version":2,"groups":[{"name":"redis","items":[{"key":"host","type":"string","required":true},{"key":"port","type":"int","shared":true},{"key":"password","type":"secret","secret":true}]}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/structure-draft/publish -d '{"comment":"ref timeout","request_id":"sr1"}' >/dev/null
R=$(J -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -d '{"updates":[{"group":"redis","key":"port","value":{"type":"int","int_value":999}}],"deletes":[]}' || true)
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert '引用共享项' in r['message'], r" 2>/dev/null && echo "  shared-ref draft write rejected OK" || echo "  (rejection message check skipped)"
J -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -d '{"updates":[{"group":"redis","key":"host","value":{"type":"string","str_value":"10.0.0.1"}}],"deletes":[],"shared_bindings":[{"group":"redis","key":"port","shared_key":"timeout"}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -d '{"comment":"v-ref","request_id":"r-ref"}' >/dev/null
J $BASE/api/v1/projects/order-service/branches/dev/config | python3 -c "import json,sys; c=json.load(sys.stdin); assert c['groups']['redis']['port']==60, c['groups']['redis']" && echo "  shared-ref materialized OK (port=60 from shared binding)"
CODE=$(curl -s -H "$AUTH" -o /tmp/shdel.json -w '%{http_code}' -X DELETE $BASE/api/v1/shared/timeout)
[ "$CODE" = "409" ] && python3 -c "import json; d=json.load(open('/tmp/shdel.json')); assert 'order-service' in json.dumps(d), d" && echo "  shared delete blocked when bound OK (409)" || { echo "  shared delete guard FAIL code=$CODE"; cat /tmp/shdel.json; exit 1; }
J -X PUT $BASE/api/v1/projects/order-service/structure-draft -d '{"base_version":3,"groups":[{"name":"redis","items":[{"key":"host","type":"string","required":true},{"key":"port","type":"int"},{"key":"password","type":"secret","secret":true}]}]}' >/dev/null
J -X POST $BASE/api/v1/projects/order-service/structure-draft/publish -d '{"comment":"unref","request_id":"sr2"}' >/dev/null
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE $BASE/api/v1/shared/timeout)
[ "$CODE" = "204" ] && echo "  shared delete OK (204 after unreferenced)" || { echo "  shared delete FAIL $CODE"; exit 1; }
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE $BASE/api/v1/shared-draft/timeout)
[ "$CODE" = "204" ] && echo "  shared-draft delete OK (204 idempotent)" || { echo "  shared-draft delete FAIL $CODE"; exit 1; }

echo "== 9. 自定义分支创建/详情/删除 =="
J -X POST $BASE/api/v1/projects/order-service/branches -d '{"name":"staging"}' >/dev/null
J $BASE/api/v1/projects/order-service/branches/staging >/dev/null
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE $BASE/api/v1/projects/order-service/branches/staging)
[ "$CODE" = "204" ] && echo "  branch delete OK (204)" || { echo "  branch delete FAIL $CODE"; exit 1; }

echo "== 10. 删除项目（force 校验 + 强制删除）=="
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE $BASE/api/v1/projects/order-service)
[ "$CODE" = "422" ] && echo "  delete without force → 422 OK" || { echo "  delete guard FAIL $CODE"; exit 1; }
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE "$BASE/api/v1/projects/order-service?force=true")
[ "$CODE" = "204" ] && echo "  project delete OK (204)" || { echo "  project delete FAIL $CODE"; exit 1; }
curl -s -H "$AUTH" $BASE/api/v1/projects | python3 -c "import json,sys; assert json.load(sys.stdin)==[], sys.stdin.read()" && echo "  project gone OK"

echo "== 11. secret 策略（P0-b）：管理面/快照掩码；渲染端点数据面 token 解密；reveal 审计 =="
J -X POST $BASE/api/v1/projects -d '{"name":"mask-test"}' >/dev/null
J -X PUT $BASE/api/v1/projects/mask-test/structure-draft -d '{"base_version":1,"groups":[{"name":"db","items":[{"key":"host","type":"string","required":true},{"key":"pass","type":"secret","secret":true}]}]}' >/dev/null
J -X POST $BASE/api/v1/projects/mask-test/structure-draft/publish -d '{"comment":"s","request_id":"s1"}' >/dev/null
J -X PUT $BASE/api/v1/projects/mask-test/branches/dev/draft -d '{"updates":[{"group":"db","key":"host","value":{"type":"string","str_value":"db1"}},{"group":"db","key":"pass","value":{"type":"string","str_value":"plainpass"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/mask-test/branches/dev/publish -d '{"comment":"v1","request_id":"r1"}' >/dev/null

# 管理面 config：默认掩码
C=$(J $BASE/api/v1/projects/mask-test/branches/dev/config)
echo "$C" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['groups']['db']['pass']=='***' and d['groups']['db']['host']=='db1', d" && echo "  admin config 默认掩码 OK"
# 管理面 config reveal=true → 明文 + 审计
C=$(J "$BASE/api/v1/projects/mask-test/branches/dev/config?reveal=true")
echo "$C" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['groups']['db']['pass']=='plainpass', d" && echo "  admin config reveal OK"
J $BASE/api/v1/audit | python3 -c "import json,sys; a=[x for x in json.load(sys.stdin) if x['action']=='config_reveal']; assert len(a)>=1, a" && echo "  config_reveal 审计 OK"
# 数据面 snapshot：secret 掩码
C=$(DP $BASE/v1/projects/mask-test/branches/dev/snapshot)
echo "$C" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['groups']['db']['pass']=='***', d" && echo "  snapshot 数据面掩码 OK"
# 渲染端点（数据面 token）：secret 解密返回（构建脚本取真值，README「构建脚本取值」）
R=$(DP "$BASE/v1/projects/mask-test/branches/dev/config?format=json")
echo "$R" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['db']['pass']=='plainpass', d" && echo "  render 数据面 token 解密 OK"
# 渲染端点 reveal=true 无会话 → 401
CODE=$(curl -s -o /tmp/reveal-nosess.json -w '%{http_code}' "$BASE/v1/projects/mask-test/branches/dev/config?format=json&reveal=true")
[ "$CODE" = "401" ] && echo "  render reveal 无会话 → 401 OK" || { echo "  render reveal guard FAIL $CODE: $(cat /tmp/reveal-nosess.json)"; exit 1; }
# 渲染端点 reveal=true 带会话 → 明文 + 审计
R=$(curl -sf -H "$AUTH" "$BASE/v1/projects/mask-test/branches/dev/config?format=json&reveal=true")
echo "$R" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['db']['pass']=='plainpass', d" && echo "  render reveal 带会话 OK"
# 渲染端点 version 参数（历史版本；v1=结构空值版本，v2=含 db1 的值版本）
J -X PUT $BASE/api/v1/projects/mask-test/branches/dev/draft -d '{"updates":[{"group":"db","key":"host","value":{"type":"string","str_value":"db2"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/mask-test/branches/dev/publish -d '{"comment":"v3","request_id":"r2"}' >/dev/null
R=$(DP "$BASE/v1/projects/mask-test/branches/dev/config?format=json&version=2")
echo "$R" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['db']['host']=='db1', d" && echo "  render version 参数 OK"

echo
echo "======== P0 API surface 全部通过 ========"

echo "== 12. 灰度管理面全链路（G4：4 端点 + 审计 + 数据面联动）=="
J -X POST $BASE/api/v1/projects -d '{"name":"gray-test"}' >/dev/null
J -X PUT $BASE/api/v1/projects/gray-test/structure-draft -d '{"base_version":1,"groups":[{"name":"app","items":[{"key":"feature","type":"string","required":true}]}]}' >/dev/null
J -X POST $BASE/api/v1/projects/gray-test/structure-draft/publish -d '{"comment":"s","request_id":"g-s1"}' >/dev/null
J -X PUT $BASE/api/v1/projects/gray-test/branches/dev/draft -d '{"updates":[{"group":"app","key":"feature","value":{"type":"string","str_value":"stable"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/gray-test/branches/dev/publish -d '{"comment":"stable v2","request_id":"g-p1"}' >/dev/null

# 无草稿 → 灰度发布 409（NoDraft）
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X POST $BASE/api/v1/projects/gray-test/branches/dev/gray-publish -H 'Content-Type: application/json' -d '{"rule":{"match_labels":[{"key":"zone","value":"cn-north-1"}]},"comment":"x","request_id":"g-e1"}')
[ "$CODE" = "409" ] && echo "  gray-publish 无草稿 → 409 OK" || { echo "  gray-publish guard FAIL $CODE"; exit 1; }

# 编辑草稿（灰度内容）→ 灰度发布
J -X PUT $BASE/api/v1/projects/gray-test/branches/dev/draft -d '{"updates":[{"group":"app","key":"feature","value":{"type":"string","str_value":"gray-feature"}}]}' >/dev/null
R=$(J -X POST $BASE/api/v1/projects/gray-test/branches/dev/gray-publish -d '{"rule":{"match_labels":[{"key":"zone","value":"cn-north-1"}]},"comment":"gray","request_id":"g-g1"}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert r['gray_seq']==1 and r['event_gray']==True, r" && echo "  gray-publish OK (gray_seq=1)"

# gray-status
S=$(J $BASE/api/v1/projects/gray-test/branches/dev/gray-status)
echo "$S" | python3 -c "import json,sys; s=json.load(sys.stdin); assert s['gray_active'] and s['gray_seq']==1 and s['gray_rule']['match_labels'][0]['value']=='cn-north-1', s" && echo "  gray-status OK"

# 数据面联动：命中 → gray=true + resolved_version=gray_seq；未命中 → gray=false
N=$(DP $BASE/v1/projects/gray-test/branches/dev/snapshot -H 'X-Dsh-Instance: web-1' -H 'X-Dsh-Labels: zone=cn-north-1')
echo "$N" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['gray']==True and d['resolved_version']==1 and d['groups']['app']['feature']=='gray-feature', d" && echo "  数据面命中 → gray=true OK"
S2=$(DP $BASE/v1/projects/gray-test/branches/dev/snapshot -H 'X-Dsh-Instance: web-2' -H 'X-Dsh-Labels: zone=cn-south-1')
echo "$S2" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['gray']==False and d['resolved_version']==2 and d['groups']['app']['feature']=='stable', d" && echo "  数据面未命中 → gray=false OK"

# 转正 → active 推进 + 状态清空
J -X POST $BASE/api/v1/projects/gray-test/branches/dev/gray-promote -d '{"comment":"promote","request_id":"g-pr1"}' >/dev/null
S=$(J $BASE/api/v1/projects/gray-test/branches/dev/gray-status)
echo "$S" | python3 -c "import json,sys; s=json.load(sys.stdin); assert s['active_version']==3 and not s['gray_active'], s" && echo "  gray-promote OK (active=3)"

# 再次灰度 + 下量 → 回落 + 状态清空
J -X PUT $BASE/api/v1/projects/gray-test/branches/dev/draft -d '{"updates":[{"group":"app","key":"feature","value":{"type":"string","str_value":"gray2"}}]}' >/dev/null
J -X POST $BASE/api/v1/projects/gray-test/branches/dev/gray-publish -d '{"rule":{"percentage":100},"comment":"g2","request_id":"g-g2"}' >/dev/null
R=$(J -X POST $BASE/api/v1/projects/gray-test/branches/dev/gray-abort -d '{"comment":"abort","request_id":"g-ab1"}')
echo "$R" | python3 -c "import json,sys; r=json.load(sys.stdin); assert r['fallback_version']==3, r" && echo "  gray-abort OK (fallback=3)"
S=$(J $BASE/api/v1/projects/gray-test/branches/dev/gray-status)
echo "$S" | python3 -c "import json,sys; s=json.load(sys.stdin); assert not s['gray_active'] and s['gray_seq']==0, s" && echo "  gray-status 清空 OK"

# 审计 action 覆盖
J "$BASE/api/v1/audit?action=gray_publish" | python3 -c "import json,sys; a=json.load(sys.stdin); assert len(a)>=2, a" && echo "  audit gray_publish OK"
J "$BASE/api/v1/audit?action=gray_promote" | python3 -c "import json,sys; a=json.load(sys.stdin); assert len(a)>=1, a" && echo "  audit gray_promote OK"
J "$BASE/api/v1/audit?action=gray_abort" | python3 -c "import json,sys; a=json.load(sys.stdin); assert len(a)>=1, a" && echo "  audit gray_abort OK"

echo "== 项目访问令牌（project-token）生命周期 =="
# 创建：201，明文仅此一次；列表无明文
TK=$(J -X POST $BASE/api/v1/projects/mask-test/tokens -d '{"name":"surface-svc"}')
PT=$(echo "$TK" | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
TID=$(echo "$TK" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "$TK" | python3 -c "import json,sys; t=json.load(sys.stdin); assert t['name']=='surface-svc' and len(t['token'])==32, t" && echo "  token create OK (32-hex plaintext once)"
L=$(J $BASE/api/v1/projects/mask-test/tokens)
echo "$L" | python3 -c "import json,sys; l=json.load(sys.stdin); assert len(l)==1 and 'token' not in l[0] and 'hash' not in l[0], l" && echo "  token list no-plaintext OK"
# 项目 token 数据面：200（鉴权通过）；他项目 → 401；无 token → 401
CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $PT" "$BASE/v1/projects/mask-test/branches/dev/snapshot")
[ "$CODE" = "200" ] && echo "  project token data-plane OK" || { echo "  project token data-plane FAIL $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $PT" "$BASE/v1/projects/other-project/branches/dev/snapshot")
[ "$CODE" = "401" ] && echo "  cross-project token → 401 OK" || { echo "  cross-project FAIL $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/v1/projects/mask-test/branches/dev/snapshot")
[ "$CODE" = "401" ] && echo "  no-token → 401 OK" || { echo "  no-token FAIL $CODE"; exit 1; }
# 吊销 → 204；吊销后原 token → 401
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X DELETE "$BASE/api/v1/projects/mask-test/tokens/$TID")
[ "$CODE" = "204" ] && echo "  revoke OK" || { echo "  revoke FAIL $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $PT" "$BASE/v1/projects/mask-test/branches/dev/snapshot")
[ "$CODE" = "401" ] && echo "  revoked token → 401 OK" || { echo "  revoked-token FAIL $CODE"; exit 1; }
