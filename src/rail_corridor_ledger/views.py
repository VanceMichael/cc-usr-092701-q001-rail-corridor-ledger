"""只读投影：完整运营视图、客户脱敏视图、任意时点审计还原。

投影全部由 State（事件折叠结果）派生，本身不保存任何状态，
因此审计人员指定任意时间点，只需重放到该时刻的事件再投影即可。
"""

from __future__ import annotations

from typing import Any, Iterable

from .model import SEGMENT_LABELS, rebuild


def _slot_progress(state, slot_id: str, route_version: int) -> dict:
    nodes = state.routes[route_version]["nodes"]
    trace = []
    for n in nodes:
        st = state.node_states.get((slot_id, route_version, n["id"]))
        if st and st["status"] == "reached":
            trace.append({
                "node_id": n["id"],
                "name": n["name"],
                "segment": n["segment"],
                "eta": n["eta"],
                "status": "reached",
                "ts": st.get("ts"),
                "basis": _basis(st),
            })
        elif st and st["status"] == "held":
            exc = state.exceptions.get(st.get("exception_id"), {})
            trace.append({
                "node_id": n["id"],
                "name": n["name"],
                "segment": n["segment"],
                "eta": n["eta"],
                "status": "held",
                "ts": st.get("ts"),
                "reason": exc.get("reason"),
            })
    return {"trace": trace}


def _basis(node_state: dict) -> dict:
    """这次推进的依据：报文 / 异常解除 / 挂起裁决 / 路线迁移。"""
    basis = {}
    if node_state.get("msg_id"):
        basis["msg_id"] = node_state["msg_id"]
    if node_state.get("exception_id"):
        basis["exception_id"] = node_state["exception_id"]
    if node_state.get("evidence"):
        ev = node_state["evidence"]
        if isinstance(ev, dict):
            if ev.get("suspension_id"):
                basis["suspension_id"] = ev["suspension_id"]
            if ev.get("proposal_id"):
                basis["proposal_id"] = ev["proposal_id"]
    if node_state.get("carried_from_version"):
        basis["carried_from_version"] = node_state["carried_from_version"]
        basis["proposal_id"] = node_state.get("proposal_id")
    if node_state.get("actor"):
        basis["actor"] = node_state["actor"]
    return basis


# =====================================================================
# 运营完整视图
# =====================================================================


def operations_view(state) -> dict[str, Any]:
    rv = state.route_version
    slots = []
    for sid in sorted(state.slots):
        slot = state.slots[sid]
        progress = _slot_progress(state, sid, rv)
        slots.append({
            "slot_id": sid,
            "batch_id": slot.batch_id,
            "weight": str(slot.weight),
            "status": slot.status,
            "fee_share": slot.fee_share,
            "deposit_share": slot.deposit_share,
            "current_node": progress["trace"][-1]["node_id"] if progress["trace"] else None,
            "trace": progress["trace"],
        })
    batches = [
        {
            "batch_id": b.batch_id,
            "active": b.active,
            "carrier_id": b.carrier_id,
            "slot_ids": sorted(b.slots),
            "parent": b.parent,
            "merged_into": b.merged_into,
        }
        for b in sorted(state.batches.values(), key=lambda b: b.batch_id)
    ]
    return {
        "order_id": state.order_id,
        "customer_id": state.customer_id,
        "completed": state.completed,
        "route_version": rv,
        "route": state.routes[rv],
        "route_history": [state.routes[v] for v in sorted(state.routes)],
        "slots": slots,
        "batches": batches,
        "exceptions": list(state.exceptions.values()),
        "suspensions": [
            {
                "suspension_id": s.suspension_id,
                "msg_id": s.msg_id,
                "kind": s.kind,
                "status": s.status,
                "slot_ids": s.slot_ids,
                "details": s.details,
            }
            for s in sorted(state.suspensions.values(), key=lambda s: s.suspension_id)
        ],
        "proposals": [
            {
                "proposal_id": p.proposal_id,
                "kind": p.kind,
                "status": p.status,
                "reason": p.reason,
                "proposer": p.proposer,
                "payload": p.payload,
                "decide_ts": p.decide_ts,
            }
            for p in sorted(state.proposals.values(), key=lambda p: p.proposal_id)
        ],
        "money": _money_view(state),
    }


def _money_view(state) -> dict:
    accounts = {name: {"debit": 0, "credit": 0}
                for name in ("cash", "fee_hold", "deposit_hold", "fee_income",
                             "claim_expense", "claim_payable")}
    accounts.update(state.balances)
    return {
        "currency": state.currency,
        "fee_total": state.fee_total,
        "deposit_total": state.deposit_total,
        "accounts": accounts,
        "entries": state.money,
    }


# =====================================================================
# 客户脱敏视图
# =====================================================================


def customer_view(state) -> dict[str, Any]:
    """客户可见：货向、当前区段名（不暴露承运方编号）、预计到达区间。"""
    rv = state.route_version
    route = state.routes[rv]
    slots = []
    for sid in sorted(state.slots):
        slot = state.slots[sid]
        last = state.last_reached(sid, rv)
        current = route["nodes"][last] if last >= 0 else None
        nxt = route["nodes"][last + 1] if last + 1 < len(route["nodes"]) else None
        held_at = [
            n["name"] for n in route["nodes"]
            if (state.node_states.get((sid, rv, n["id"])) or {}).get("status") == "held"
        ]
        slots.append({
            "slot_id": sid,  # 客户自己的货位编号可见
            "status": slot.status,
            "current_segment": SEGMENT_LABELS.get(current["segment"], current["segment"]) if current else "待发运",
            "current_node": current["name"] if current else None,
            "next_node": nxt["name"] if nxt else None,
            "eta_window": (nxt or current)["eta"] if (nxt or current) else None,
            "held": held_at[0] if held_at else None,
            "last_update_ts": (state.node_states[(sid, rv, current["id"])].get("ts")
                               if current else None),
        })
    open_claims = [
        e["ref"]["exception_id"] for e in state.money
        if e["account"] == "claim_payable"
    ]
    return {
        "order_id": state.order_id,
        "route_version": rv,
        "completed": state.completed,
        "slots": slots,
        "exceptions_open": sum(1 for e in state.exceptions.values() if e["status"] == "open"),
        "claims_pending": len(open_claims),
        "currency": state.currency,
        "fee_paid": state.fee_total,
        "deposit_total": state.deposit_total,
    }


# =====================================================================
# 审计：任意时间点还原
# =====================================================================


def state_at(events: Iterable[dict], ts: str | None = None, *, seq: int | None = None):
    """重放至指定时刻（ISO 字符串）或指定事件序号（含）的状态。"""
    chosen = []
    for env in events:
        if ts is not None and env["ts"] > ts:
            break
        if seq is not None and env["seq"] > seq:
            break
        chosen.append(env)
    return rebuild(chosen)


def audit_timeline(events: Iterable[dict]) -> list[dict]:
    """逐条变更依据：节点推进/异常/改线/拆并/资金，全部可按 seq 与时点定位。"""
    timeline = []
    for env in events:
        d = env["data"]
        rec = {
            "seq": env["seq"],
            "ts": env["ts"],
            "tx": env["tx"],
            "type": env["type"],
            "actor": env.get("actor"),
        }
        if env["type"] == "SlotCheckpointReached":
            rec.update({
                "slot_ids": d["slot_ids"],
                "route_version": d["route_version"],
                "node_id": d["node_id"],
                "msg_id": d.get("msg_id"),
                "evidence": d.get("evidence"),
            })
        elif env["type"] in ("SlotExceptionOpened", "SlotExceptionResolved"):
            rec.update({"exception_id": d.get("exception_id"),
                        "node_id": d.get("node_id"),
                        "reason": d.get("reason"),
                        "claim_amount": d.get("claim_amount")})
        elif env["type"] == "RouteActivated":
            rec.update({"from_version": d["from_version"],
                        "proposal_id": d.get("proposal_id"),
                        "new_route": d["route"]})
        elif env["type"] in ("BatchSplit", "BatchMerged"):
            rec.update({"proposal_id": d.get("proposal_id"),
                        "detail": {k: v for k, v in d.items()
                                   if k not in ("order_id", "proposal_id")}})
        elif env["type"] == "MessageSuspended":
            rec.update({"suspension_id": d["suspension_id"], "msg_id": d["msg_id"],
                        "kind": d["kind"], "details": d["details"]})
        elif env["type"] in ("ProposalOpened", "ProposalApproved", "ProposalRejected",
                             "SuspensionResolved", "SlotWeightAdjusted",
                             "MessageRecorded", "OrderPlaced", "SlotDelivered",
                             "OrderCompleted", "MoneyEntry"):
            rec["data"] = d
        timeline.append(rec)
    return timeline
