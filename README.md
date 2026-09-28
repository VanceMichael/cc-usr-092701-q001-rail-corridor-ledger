# 中亚班列货运履约台账

南昌国际陆港 → 霍尔果斯口岸 → 比什凯克班列的货运履约服务。把一笔订单拆成可追踪的**货位（slot）**与**批次（batch）**，按**路线版本**记录装箱、换装、查验、放行、异常扣留、分拨拆并与最终交付；费用预占、放行保证金、异常赔付与节点变更同事务落账，支持崩溃恢复与任意时点审计。

数据均为演示用虚构内容。

## 参与方与权限

| 角色 | 权限 |
| --- | --- |
| 货代运营经理 forwarder | 建单、发起改线/拆并批提案 |
| 承运方 carrier | 只能推进**本人承运区段**的运输节点（按 `carrier_id` 校验） |
| 口岸查验人员 customs | 只能登记霍尔果斯的查验/放行、登记异常扣留 |
| 值班主管 supervisor | 解除异常与赔付、裁决挂起报文、批准改线与批次拆并 |
| 客户 customer | 只读脱敏视图（不暴露承运方编号、报文编号等内部信息） |

## 模块结构

```
src/rail_corridor_ledger/
  events.py   只追加 WAL：BEGIN/EVENT/COMMIT 事务，撕裂尾部恢复
  model.py    领域模型：apply 事件折叠 + decide_* 纯决策 + 资金守恒
  views.py    运营视图 / 客户脱敏视图 / 任意时点审计
  service.py  服务门面：命令幂等、单事务提交、提交前整流重放校验
  demo.py     南昌—霍尔果斯—比什凯克示例场景
```

## 核心规则（不变量）

1. **报文幂等**：每份报文有 `msg_id` 与内容指纹（节点+货位+重量+载荷）。
   - 编号与内容都相同的重复报文：零事件，不重复推进、不重复冻结/扣款；
   - **编号未变但货位构成或过磅重量变化**：不推进，生成挂起单 `MessageSuspended`，等值班主管裁决（确认改重并补推进，或作废）；裁决后该报文编号关闭。
2. **节点顺序**：必须沿当前路线版本逐点推进，不能跳点；处于异常扣留的货位，一切下游节点冻结，直到主管解除。
3. **路线版本**：改线必须先有主管批准的提案；激活时新版本号递增，已完成节点按 `carry_to_index` 迁移并记录来源版本与提案依据；旧版本上的迟到报文直接拒绝。
4. **批次拆并**：拆批必须不重不漏覆盖父批次；合批只允许发生在当前节点位置相同、无扣留货位的批次之间；均需主管批准。
5. **资金守恒**：金额为整数最小单位；建单时按重量把运费与预计保证金分摊到货位（尾差兜底，合计分毫不差）。
   - 建单：客户缴费 → `fee_hold` 费用预占；
   - 霍尔果斯放行：保证金 `cash → deposit_hold` 冻结；
   - 最终交付：`fee_hold → fee_income` 结转，`deposit_hold → cash` 退还；
   - 异常解除：赔付 `claim_expense / claim_payable`。
   - 每个事务**借贷相等**，且账户余额不得被反向击穿（例如不能退一笔从未冻结的保证金）。
6. **崩溃恢复**：节点事件与资金分录在同一 WAL 事务。进程在 COMMIT 前任一点退出，重启时未提交尾部被物理截断（容忍半行残片），重试不重不漏；COMMIT 后即使调用方未收到回执，数据已耐久，凭命令号 `cmd_id` 重试只返回首次结果。
7. **提交前整流校验**：服务在追加前把“历史事件 + 本批事件”整体重放，任何序号跳跃、资金不平、余额击穿都会整批拒绝，磁盘不写一个字节。
8. **任意时点审计**：状态全部由事件折叠得到，`audit_at(seq=…|ts=…)` 可还原任一时点采用的路线版本、货位状态；`timeline()` 列出每次变更及其依据（报文号/异常单/挂起单/提案号）。

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v

# 编译检查
python3 -m compileall -q src tests

# 端到端演示（重复报文、挂起核对、异常赔付、资金账户、时点审计）
python3 -m src.rail_corridor_ledger.demo /tmp/rcl-demo.wal

# 领域资料摘要
python3 -m src.rail_corridor_ledger.context fixtures/context.json
```

## 用法示例

```python
from src.rail_corridor_ledger.service import FulfillmentService
from src.rail_corridor_ledger import demo
from src.rail_corridor_ledger.model import Actor, Role

svc = FulfillmentService("/tmp/order.wal")
a = demo.actors()
svc.place_order(a["manager"], demo.order_cmd())

# 国内承运方只能推进自己区段
svc.report_checkpoint(a["carrier_cn"], {
    "cmd_id": "c1", "msg_id": "MSG-001",
    "node_id": "nanchang_load", "batch_id": "B1"})

svc.operations_view()      # 完整运营视图
svc.customer_view()        # 客户脱敏视图
svc.audit_at(seq=10)       # 第 10 个事件后的时点还原
```
