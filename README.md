# 中亚班列货运履约台账

面向南昌国际陆港—霍尔果斯—比什凯克直达班列的货运履约服务。货代、口岸和
承运方在不同时间回传装箱、换装、查验、放行消息，系统把订单拆成可追踪的
**货位/批次**，按**路线版本**推进节点，并与费用台账、审批和审计保持一致。

数据均为演示用虚构内容。

## 领域模型

- 订单（order）→ 批次（batch）→ 货位（slot，含箱号与毛重）
- 节点：`booked → loaded → departed → arrived_port → transship →
  inspection → released → departed_port → abroad_transit →
  arrived_bishkek → split_dispatch → delivered`
- 区段：境内段（承运方）、霍尔果斯口岸（口岸）、境外段与末端（承运方）

## 关键约束与实现方式

| 需求 | 实现 |
| --- | --- |
| 同一份口岸消息重复到达不能重复推进节点 | 消息以 `message_id` 登记结论（verified/suspended/detained/duplicate），重放命中直接丢弃，不产生事件 |
| 编号未变但货位或重量变化先挂起核对 | 比对箱号、毛重；不一致只写 `message_reviewed(suspended)` + 挂起，节点游标不动；核对解除后才能推进到下一个检查点 |
| 承运方只能维护自己负责的区段 | 角色—区段映射 `ROLE_SEGMENTS` + 节点—区段映射 `NODE_SEGMENTS`，每次回传双向校验 |
| 客户只看脱敏进度 | `customer_view` 裁掉费用、赔付、操作人姓名、消息编号与内部依据，只保留主链节点与 ETA 区间 |
| 值班主管才能批准改线/拆并批次 | 提案（pending）与批准分离，改线生成顺序递增的路线版本，拆并保留货位来源 |
| 费用预占、保证金、赔付与节点一致 | 运费随接单、保证金随放行节点在**同一事务**落盘；赔付绑定扣留挂起；预占只能结算/退还一次 |
| 写一半退出恢复后不多扣、不跳过检查点 | JSONL WAL：`begin → 事件（全局连续 seq）→ commit`，重开时截断无 commit 的尾部（含半行损坏） |
| 按任意时间点还原路线、货位状态与变更依据 | 全部状态由事件流回放；`replay(until_seq/until_ts)` + `audit_snapshot` 输出每个节点的依据、批准人与提案号 |

## 代码结构

- `src/rail_corridor_ledger/model.py` — 角色、区段、节点、路线、货位/批次模型
- `src/rail_corridor_ledger/store.py` — 事务型事件存储与崩溃恢复
- `src/rail_corridor_ledger/views.py` — 事件回放投影、运营/客户/审计视图、费用守恒检查
- `src/rail_corridor_ledger/service.py` — 应用服务（幂等、挂起、鉴权、审批、费用）
- `src/rail_corridor_ledger/demo.py` — 端到端场景演示
- `tests/test_fulfillment.py` — 20 个用例覆盖上述全部约束

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 编译

```bash
python3 -m compileall -q src tests
```

## 命令行检查

```bash
# 领域资料摘要
python3 -m src.rail_corridor_ledger.context fixtures/context.json

# 端到端履约场景（幂等、挂起、赔付、改线、脱敏、时间点审计）
python3 -m src.rail_corridor_ledger.demo
```

## 最小用法

```python
from src.rail_corridor_ledger.model import Role, Slot
from src.rail_corridor_ledger.service import Actor, FulfillmentService
from src.rail_corridor_ledger.store import EventStore
from src.rail_corridor_ledger.views import customer_view, replay

fwd = Actor("u1", Role.FORWARDER)
carrier = Actor("u2", Role.CARRIER)
svc = FulfillmentService(EventStore("ledger.jsonl"))
svc.open_order(fwd, "ORD-1", "比什凯克客户", freight_cents=486_000)
svc.create_batch(fwd, "ORD-1", "B-1",
                 [Slot("S-1", "CCLU-7001", 18200.0, "工程机电")],
                 basis="装箱计划")
svc.ingest(carrier, "ORD-1", "MSG-9", "B-1", "loaded",
           basis="南昌装箱回执",
           observed_slots=[{"slot_no": "S-1", "container_no": "CCLU-7001",
                            "weight_kg": 18200.0}])
print(customer_view(replay(svc.store.events()).order("ORD-1")))
```
