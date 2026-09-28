"""端到端场景演示：南昌—霍尔果斯—比什凯克班列的一笔订单。

运行：

    python3 -m src.rail_corridor_ledger.demo

演示内容：接单预占运费 → 装箱/发车 → 重复消息幂等丢弃 → 霍尔果斯
称重变化挂起核对 → 解除 → 查验异常扣留与赔付 → 主管批准改线 → 放行
保证金 → 客户脱敏视图 → 审计按时间点还原路线与节点依据。
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .model import Role, Slot
from .service import Actor, FulfillmentService
from .store import EventStore
from .views import audit_snapshot, customer_view, order_tracking, replay

FORWARDER = Actor("u-fwd", Role.FORWARDER, "李运营")
CARRIER = Actor("u-car", Role.CARRIER, "王承运")
PORT = Actor("u-port", Role.PORT, "霍尔果斯口岸")
SUP = Actor("u-sup", Role.SUPERVISOR, "赵值班")


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        svc = FulfillmentService(EventStore(Path(td) / "ledger.jsonl"))

        svc.open_order(FORWARDER, "ORD-NC-2601", "比什凯克客户甲",
                       freight_cents=486_000, basis="托运委托 SO-2601")
        svc.create_batch(
            FORWARDER, "ORD-NC-2601", "B-1",
            [Slot("S-1", "CCLU-7001", 18200.0, "工程机电"),
             Slot("S-2", "CCLU-7002", 9600.0, "汽配零件")],
            basis="装箱计划 PL-2601")

        svc.ingest(CARRIER, "ORD-NC-2601", "MSG-01", "B-1", "loaded",
                   basis="南昌国际陆港装箱回执",
                   observed_slots=[
                       {"slot_no": "S-1", "container_no": "CCLU-7001",
                        "weight_kg": 18200.0},
                       {"slot_no": "S-2", "container_no": "CCLU-7002",
                        "weight_kg": 9600.0}],
                   eta_range={"earliest": "2026-10-11", "latest": "2026-10-14"})
        svc.ingest(CARRIER, "ORD-NC-2601", "MSG-02", "B-1", "departed",
                   basis="南昌西发车")

        # 同一份消息因口岸系统重发再次到达：被幂等丢弃。
        dropped = svc.ingest(CARRIER, "ORD-NC-2601", "MSG-02", "B-1",
                             "departed", basis="重发")
        print("重复消息产生事件数：", len(dropped))

        # 抵霍尔果斯，报文里 S-2 重量与台账不符：挂起核对，节点不动。
        svc.ingest(PORT, "ORD-NC-2601", "MSG-03", "B-1", "arrived_port",
                   basis="抵达霍尔果斯",
                   observed_slots=[{"slot_no": "S-2",
                                    "container_no": "CCLU-7002",
                                    "weight_kg": 9950.0}])
        tracking = order_tracking(replay(svc.store.events()).order("ORD-NC-2601"))
        print("挂起中，当前节点：", tracking["batches"][0]["node"],
              "未决挂起：", tracking["open_holds"])

        hold_no = tracking["open_holds"][0]
        svc.resume_after_check(
            FORWARDER, "ORD-NC-2601", hold_no,
            resolution="口岸复称 9600kg，境外段申报误差",
            resume_node="arrived_port", basis="复称确认单 W-2601")

        svc.ingest(PORT, "ORD-NC-2601", "MSG-04", "B-1", "transship",
                   basis="准轨换宽轨换装")
        svc.ingest(PORT, "ORD-NC-2601", "MSG-05", "B-1", "inspection",
                   basis="哈方海关查验")

        # 查验异常扣留并赔付。
        svc.detain(PORT, "ORD-NC-2601", "B-1", "箱单与实物件数不符，滞留补证",
                   message_id="MSG-06")
        det_hold = order_tracking(
            replay(svc.store.events()).order("ORD-NC-2601"))["open_holds"][0]
        svc.book_compensation(SUP, "ORD-NC-2601", det_hold, 26_000,
                              basis="滞留补证赔付协议 CMP-2601")

        # 境外拥堵：承运方提案改线，值班主管批准（产生路线 v2）。
        new_nodes = ["booked", "loaded", "departed", "arrived_port",
                     "transship", "inspection", "released", "departed_port",
                     "abroad_transit", "arrived_almaty", "arrived_bishkek",
                     "split_dispatch", "delivered"]
        proposal = svc.propose_route_change(
            CARRIER, "ORD-NC-2601", new_nodes,
            note="改经阿拉木图中转", reason="哈方边境拥堵")
        svc.approve_route_change(SUP, "ORD-NC-2601",
                                 proposal["proposal_id"],
                                 basis="值班主管批准 RC-2601")

        svc.ingest(PORT, "ORD-NC-2601", "MSG-07", "B-1", "released",
                   basis="霍尔果斯放行出境",
                   eta_range={"earliest": "2026-10-15", "latest": "2026-10-17"})

        state = replay(svc.store.events())
        print("\n=== 运营台账 ===")
        print(json.dumps(order_tracking(state.order("ORD-NC-2601")),
                         ensure_ascii=False, indent=2))
        print("\n=== 客户脱敏视图 ===")
        print(json.dumps(customer_view(state.order("ORD-NC-2601")),
                         ensure_ascii=False, indent=2))

        # 审计：还原放行时刻采用的路线版本与每次变更依据。
        released_seq = next(
            e["seq"] for e in svc.store.events()
            if e["type"] == "node_advanced" and e["node"] == "released")
        snapshot = audit_snapshot(
            replay(svc.store.events(), until_seq=released_seq),
            "ORD-NC-2601")
        print("\n=== 放行时点审计还原 ===")
        print("当时路线版本：", snapshot["current_route"],
              "路线记录：", [(r["version"], r["basis"]) for r in snapshot["routes"]])
        print("费用台账：",
              [(f["fee_id"], f["fee_type"], f["amount_cents"], f["status"])
               for f in snapshot["fees"]])


if __name__ == "__main__":
    main()
