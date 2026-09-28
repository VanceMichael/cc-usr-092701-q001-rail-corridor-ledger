"""南昌国际陆港 → 霍尔果斯 → 比什凯克 演示场景与路线。

数据全部虚构，仅用于演示与测试。

命令行：
    python3 -m src.rail_corridor_ledger.demo <wal文件路径>
"""

from __future__ import annotations

import sys
from pathlib import Path

from .model import Actor, Role
from .service import FulfillmentService

# 承运方
CARRIER_CN = "CAR-CN-RAIL"   # 南昌—霍尔果斯国内段
CARRIER_KZ = "CAR-KZ-RAIL"   # 霍尔果斯—比什凯克境外段


def default_route() -> dict:
    """v1 路线：装箱发运 → 阿拉山口换装（霍尔果斯口岸作业）→ 查验 → 放行 → 比什凯克交付。"""
    return {
        "version": 1,
        "name": "南昌—霍尔果斯—比什凯克",
        "nodes": [
            {"id": "nanchang_load", "name": "南昌国际陆港装箱发运",
             "segment": "nanchang_khorgos", "kind": "load",
             "carrier_id": CARRIER_CN,
             "eta": {"earliest": "2026-09-01", "latest": "2026-09-02"}},
            {"id": "khorgos_transfer", "name": "霍尔果斯口岸换装",
             "segment": "nanchang_khorgos", "kind": "transfer",
             "carrier_id": CARRIER_CN,
             "eta": {"earliest": "2026-09-08", "latest": "2026-09-10"}},
            {"id": "khorgos_inspection", "name": "霍尔果斯海关查验",
             "segment": "khorgos", "kind": "inspection",
             "carrier_id": None,
             "eta": {"earliest": "2026-09-09", "latest": "2026-09-11"}},
            {"id": "khorgos_release", "name": "霍尔果斯放行出境",
             "segment": "khorgos", "kind": "release",
             "carrier_id": None,
             "eta": {"earliest": "2026-09-10", "latest": "2026-09-12"}},
            {"id": "bishkek_arrival", "name": "比什凯克到站分拨",
             "segment": "khorgos_bishkek", "kind": "transfer",
             "carrier_id": CARRIER_KZ,
             "eta": {"earliest": "2026-09-16", "latest": "2026-09-19"}},
            {"id": "bishkek_deliver", "name": "比什凯克客户交付",
             "segment": "khorgos_bishkek", "kind": "deliver",
             "carrier_id": CARRIER_KZ,
             "eta": {"earliest": "2026-09-18", "latest": "2026-09-21"}},
        ],
    }


def actors() -> dict[str, Actor]:
    return {
        "manager": Actor("u-manager", Role.FORWARDER),
        "carrier_cn": Actor("u-carrier-cn", Role.CARRIER, CARRIER_CN),
        "carrier_kz": Actor("u-carrier-kz", Role.CARRIER, CARRIER_KZ),
        "customs": Actor("u-customs", Role.CUSTOMS),
        "supervisor": Actor("u-supervisor", Role.SUPERVISOR),
        "customer": Actor("u-customer", Role.CUSTOMER),
    }


def order_cmd() -> dict:
    return {
        "cmd_id": "cmd-place-1",
        "order_id": "ORD-NC-BISH-0001",
        "customer_id": "CUST-88",
        "fee_total": 100000,          # 运费（分）
        "deposit_total": 20000,       # 预计放行保证金（分）
        "currency": "CNY",
        "route": default_route(),
        "slots": [
            {"slot_id": "S1", "weight": "1200"},
            {"slot_id": "S2", "weight": "800"},
            {"slot_id": "S3", "weight": "500"},
        ],
        "batches": [
            {"batch_id": "B1", "slot_ids": ["S1", "S2"], "carrier_id": CARRIER_CN},
            {"batch_id": "B2", "slot_ids": ["S3"], "carrier_id": CARRIER_CN},
        ],
    }


def run(path: str) -> None:
    """跑一遍典型履约：重复报文、挂起核对、异常赔付、拆并批、改线、交付。"""
    svc = FulfillmentService(path, clock=iter([
        "2026-09-01T08:00:00+00:00", "2026-09-02T08:00:00+00:00",
        "2026-09-08T08:00:00+00:00", "2026-09-10T08:00:00+00:00",
        "2026-09-11T08:00:00+00:00", "2026-09-12T08:00:00+00:00",
        "2026-09-13T08:00:00+00:00", "2026-09-14T08:00:00+00:00",
        "2026-09-15T08:00:00+00:00", "2026-09-16T08:00:00+00:00",
        "2026-09-17T08:00:00+00:00", "2026-09-18T08:00:00+00:00",
        "2026-09-19T08:00:00+00:00", "2026-09-20T08:00:00+00:00",
        "2026-09-21T08:00:00+00:00", "2026-09-22T08:00:00+00:00",
        "2026-09-23T08:00:00+00:00", "2026-09-24T08:00:00+00:00",
        "2026-09-25T08:00:00+00:00", "2026-09-26T08:00:00+00:00",
    ]).__next__)
    a = actors()

    svc.place_order(a["manager"], order_cmd())
    # B1(S1,S2) 装箱 → 换装 → 查验 → 放行（重复报文原样重发，无副作用）
    for node, actor, m in [
        ("nanchang_load", a["carrier_cn"], "D-LOAD"),
        ("khorgos_transfer", a["carrier_cn"], "D-TR"),
        ("khorgos_inspection", a["customs"], "D-INSP"),
        ("khorgos_release", a["customs"], "D-REL"),
    ]:
        svc.report_checkpoint(actor, {"msg_id": m, "node_id": node, "batch_id": "B1"})
        svc.report_checkpoint(actor, {"msg_id": m, "node_id": node, "batch_id": "B1"})

    # 比什凯克到站：S1 到站即被登记异常扣留，S2 正常到站；主管解除 S1 并赔付
    svc.report_exception(a["customs"], {"msg_id": "D-EXC", "exception_id": "EX-1",
                                        "node_id": "bishkek_arrival", "slot_ids": ["S1"],
                                        "reason": "箱体轻微破损"})
    svc.report_checkpoint(a["carrier_kz"], {"msg_id": "D-ARR", "node_id": "bishkek_arrival", "slot_ids": ["S2"]})
    svc.resolve_exception(a["supervisor"], {"cmd_id": "D-RXC", "exception_id": "EX-1",
                                            "claim_amount": "300"})
    svc.report_checkpoint(a["carrier_kz"], {"msg_id": "D-DEL-B1", "node_id": "bishkek_deliver", "slot_ids": ["S1", "S2"]})

    # B2(S3) 在霍尔果斯被发现过磅重量漂移，挂起 → 主管确认改重并补推进
    svc.report_checkpoint(a["carrier_cn"], {"msg_id": "D-LOAD2", "node_id": "nanchang_load", "batch_id": "B2"})
    svc.report_checkpoint(a["carrier_cn"], {"msg_id": "D-TR2", "node_id": "khorgos_transfer", "batch_id": "B2"})
    svc.report_checkpoint(a["customs"], {"msg_id": "D-INSP2", "node_id": "khorgos_inspection",
                                         "batch_id": "B2", "measured_weights": {"S3": "470"}})
    svc.resolve_suspension(a["supervisor"], {"cmd_id": "D-SUS", "suspension_id": "SUS-D-INSP2",
                                             "action": "confirmed", "corrected_weights": {"S3": "470"},
                                             "advance_node": True})
    svc.report_checkpoint(a["customs"], {"msg_id": "D-REL2", "node_id": "khorgos_release", "batch_id": "B2"})
    svc.report_checkpoint(a["carrier_kz"], {"msg_id": "D-ARR2", "node_id": "bishkek_arrival", "batch_id": "B2"})
    svc.report_checkpoint(a["carrier_kz"], {"msg_id": "D-DEL2", "node_id": "bishkek_deliver", "batch_id": "B2"})

    print("=== 运营视图（摘要）===")
    v = svc.operations_view()
    for s in v["slots"]:
        print(f"  {s['slot_id']} 批次={s['batch_id']} 重量={s['weight']} "
              f"状态={s['status']} 末节点={s['current_node']} "
              f"费用份额={s['fee_share']} 保证金份额={s['deposit_share']}")
    print("  账户余额：")
    for name, bal in v["money"]["accounts"].items():
        print(f"    {name}: 借 {bal['debit']} / 贷 {bal['credit']}")
    print(f"  订单完成：{v['completed']}")

    print("=== 客户脱敏视图（S1）===")
    c = svc.customer_view()
    print(" ", next(s for s in c["slots"] if s["slot_id"] == "S1"))

    print("=== 时点审计：霍尔果斯放行 S1/S2 时（seq 区间内）===")
    rel_seq = next(t["seq"] for t in svc.timeline()
                   if t["type"] == "SlotCheckpointReached" and t["node_id"] == "khorgos_release")
    at = svc.audit_at(seq=rel_seq)
    for s in at["slots"]:
        print(f"  {s['slot_id']} 当时末节点={s['current_node']}")
    print("  当时路线版本：", at["route_version"])


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/tmp/rcl-demo.wal"
    run(target)
