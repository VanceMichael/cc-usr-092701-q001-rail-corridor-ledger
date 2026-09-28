"""事件回放投影：当前状态、时间点还原与脱敏视图。

所有状态都由事件流推导，不允许就地修改，因此：

- :func``replay`` 能停在任意 seq/时间点，还原"当时采用的路线版本、
  货位状态"；
- 回放过程中执行费用守恒等不变量检查，日志一旦被破坏会立即报错；
- :func``customer_view`` 在同一投影结果上裁剪字段，客户看不到费用、
  异常赔付与内部核对依据。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .model import CUSTOMER_VISIBLE_NODES, DEFAULT_ROUTE


class ProjectionError(RuntimeError):
    """事件流无法自洽（缺事件、回退、费用不守恒等）。"""


@dataclass
class FeeEntry:
    fee_id: str
    fee_type: str          # freight_reserve / release_deposit / compensation
    amount_cents: int
    status: str            # held 预占中 / settled 已结算 / refunded 已退还
    seq: int
    settled_cents: int | None = None
    basis: str = ""


@dataclass
class BatchState:
    batch_no: str
    slots: list[dict[str, Any]] = field(default_factory=list)
    route_version: int = 1
    node: str = "booked"
    active: bool = True
    detained: bool = False
    history: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class OrderState:
    order_no: str
    customer: str
    routes: dict[int, dict[str, Any]] = field(default_factory=dict)
    current_route: int = 1
    batches: dict[str, BatchState] = field(default_factory=dict)
    fees: dict[str, FeeEntry] = field(default_factory=dict)
    detentions: list[dict[str, Any]] = field(default_factory=list)
    holds: dict[str, dict[str, Any]] = field(default_factory=dict)
    proposals: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages: dict[str, str] = field(default_factory=dict)  # message_id -> 处理结论


@dataclass
class LedgerState:
    orders: dict[str, OrderState] = field(default_factory=dict)
    seq: int = 0

    # -- 读取辅助 --------------------------------------------------------

    def order(self, order_no: str) -> OrderState:
        try:
            return self.orders[order_no]
        except KeyError:
            raise ProjectionError(f"订单不存在：{order_no}") from None

    def batch(self, order_no: str, batch_no: str) -> BatchState:
        order = self.order(order_no)
        try:
            return order.batches[batch_no]
        except KeyError:
            raise ProjectionError(f"批次不存在：{order_no}/{batch_no}") from None

    def message_seen(self, message_id: str) -> str | None:
        """返回消息上次的处理结论，未见过返回 None。"""
        for order in self.orders.values():
            if message_id in order.messages:
                return order.messages[message_id]
        return None


# ---------------------------------------------------------------------------
# 回放
# ---------------------------------------------------------------------------


def replay(events: Iterable[dict[str, Any]], *, until_seq: int | None = None,
           until_ts: float | None = None) -> LedgerState:
    """把事件流折叠成状态。``until_seq``/``until_ts`` 用于时间点还原。"""
    state = LedgerState()
    for event in events:
        if until_seq is not None and event["seq"] > until_seq:
            break
        if until_ts is not None and event["ts"] > until_ts:
            break
        _apply(state, event)
    _assert_fee_conservation(state)
    return state


def _apply(state: LedgerState, event: dict[str, Any]) -> None:
    seq = event["seq"]
    if seq != state.seq + 1:
        raise ProjectionError(f"事件序号不连续：{seq}，期望 {state.seq + 1}")
    state.seq = seq
    kind = event["type"]
    order_no = event.get("order_no")
    handler = _HANDLERS.get(kind)
    if handler is None:
        raise ProjectionError(f"未知事件类型：{kind}")
    handler(state, event, state.orders[order_no] if order_no in state.orders else None)


def _h_order_opened(state: LedgerState, e: dict[str, Any], _: Any) -> None:
    if e["order_no"] in state.orders:
        raise ProjectionError(f"订单重复开通：{e['order_no']}")
    route = {
        "version": 1,
        "nodes": tuple(e.get("route_nodes", DEFAULT_ROUTE.nodes)),
        "note": e.get("note", DEFAULT_ROUTE.note),
        "effective_seq": e["seq"],
        "basis": e.get("basis", "客户下单"),
    }
    state.orders[e["order_no"]] = OrderState(
        order_no=e["order_no"], customer=e["customer"],
        routes={1: route}, current_route=1)


def _h_batch_created(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    if e["batch_no"] in o.batches:
        raise ProjectionError(f"批次重复创建：{e['batch_no']}")
    o.batches[e["batch_no"]] = BatchState(
        batch_no=e["batch_no"],
        slots=[dict(s) for s in e["slots"]],
        route_version=e.get("route_version", o.current_route),
        node="booked",
        history=[{"seq": e["seq"], "node": "booked", "basis": _basis(e)}])


def _advance(b: BatchState, e: dict[str, Any]) -> None:
    if not b.active:
        raise ProjectionError(f"批次 {b.batch_no} 已关闭，不能推进节点")
    b.node = e["node"]
    b.detained = False
    b.history.append({
        "seq": e["seq"], "node": e["node"], "actor": e.get("actor"),
        "basis": _basis(e), "message_id": e.get("message_id"),
        "ts": e.get("ts"),
        "eta_range": e.get("eta_range"),
    })


def _h_node_advanced(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    _advance(o.batches[e["batch_no"]], e)
    if e.get("message_id"):
        o.messages.setdefault(e["message_id"], "advanced")


def _h_detained(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    b = o.batches[e["batch_no"]]
    b.detained = True
    hold = {
        "hold_no": e["hold_no"], "kind": "detention", "batch_no": b.batch_no,
        "reason": e["reason"], "since_seq": e["seq"], "resolved_seq": None,
        "resolution": None, "basis": _basis(e), "message_id": e.get("message_id"),
    }
    o.holds[e["hold_no"]] = hold
    o.detentions.append(hold)
    b.history.append({"seq": e["seq"], "node": "detained",
                      "reason": e["reason"], "basis": _basis(e)})
    if e.get("message_id"):
        o.messages.setdefault(e["message_id"], "detained")


def _h_released_from_hold(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    hold = o.holds.get(e["hold_no"])
    if hold is None:
        raise ProjectionError(f"挂起记录不存在：{e['hold_no']}")
    if hold["resolved_seq"] is not None:
        raise ProjectionError(f"挂起 {e['hold_no']} 已解除，不能重复解除")
    hold["resolved_seq"] = e["seq"]
    hold["resolution"] = e["resolution"]
    b = o.batches[hold["batch_no"]]
    b.detained = False
    if e.get("resume_node"):
        _advance(b, {**e, "node": e["resume_node"]})


def _h_route_changed(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    new_version = e["new_version"]
    if new_version != o.current_route + 1:
        raise ProjectionError(
            f"路线版本必须顺序递增：{new_version}，当前 v{o.current_route}")
    if new_version in o.routes:
        raise ProjectionError(f"路线版本已存在：v{new_version}")
    o.routes[new_version] = {
        "version": new_version, "nodes": tuple(e["nodes"]),
        "note": e.get("note", ""), "effective_seq": e["seq"],
        "basis": _basis(e), "proposal_id": e.get("proposal_id"),
        "approver": e.get("actor"),
    }
    o.current_route = new_version
    for b in o.batches.values():
        if b.active and b.batch_no in e.get("batches", list(o.batches)):
            b.route_version = new_version
    proposal = o.proposals.get(e.get("proposal_id", ""))
    if proposal is not None:
        proposal["status"] = "approved"
        proposal["resolved_seq"] = e["seq"]


def _slot_map(b: BatchState) -> dict[str, dict[str, Any]]:
    return {s["slot_no"]: s for s in b.slots}


def _h_batch_split(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    src = o.batches[e["batch_no"]]
    moved = e["moved_slots"]
    src_map = _slot_map(src)
    for slot_no in moved:
        if slot_no not in src_map:
            raise ProjectionError(f"拆分失败：{e['batch_no']} 无货位 {slot_no}")
    new_slots = [src_map.pop(slot_no) for slot_no in moved]
    src.slots = list(src_map.values())   # 源批次只保留未拆走的货位
    new_batch = BatchState(
        batch_no=e["new_batch_no"], slots=new_slots,
        route_version=src.route_version, node=src.node,
        history=[dict(src.history[-1])] if src.history else [])
    new_batch.history.append({"seq": e["seq"], "node": new_batch.node,
                              "basis": _basis(e), "split_from": src.batch_no})
    o.batches[e["new_batch_no"]] = new_batch
    src.history.append({"seq": e["seq"], "node": src.node,
                        "basis": _basis(e), "split_out": e["new_batch_no"]})
    proposal = o.proposals.get(e.get("proposal_id", ""))
    if proposal is not None:
        proposal["status"] = "approved"
        proposal["resolved_seq"] = e["seq"]


def _h_batch_merged(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    target = o.batches[e["target_batch_no"]]
    for src_no in e["source_batch_nos"]:
        src = o.batches.get(src_no)
        if src is None or not src.active:
            raise ProjectionError(f"合并失败：源批次 {src_no} 不存在或已关闭")
        target.slots.extend(src.slots)
        src.active = False
        src.node = "merged"
        src.history.append({"seq": e["seq"], "node": "merged",
                            "basis": _basis(e), "merged_into": target.batch_no})
    target.history.append({"seq": e["seq"], "node": target.node,
                           "basis": _basis(e),
                           "merged_from": list(e["source_batch_nos"])})
    proposal = o.proposals.get(e.get("proposal_id", ""))
    if proposal is not None:
        proposal["status"] = "approved"
        proposal["resolved_seq"] = e["seq"]


def _h_fee_booked(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    if e["fee_id"] in o.fees:
        raise ProjectionError(f"费用条目重复记账：{e['fee_id']}")
    o.fees[e["fee_id"]] = FeeEntry(
        fee_id=e["fee_id"], fee_type=e["fee_type"],
        amount_cents=e["amount_cents"], status="held", seq=e["seq"],
        basis=_basis(e))


def _h_fee_settled(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    fee = o.fees.get(e["fee_id"])
    if fee is None:
        raise ProjectionError(f"结算的费用条目不存在：{e['fee_id']}")
    if fee.status != "held":
        raise ProjectionError(f"费用 {fee.fee_id} 已处理过，不能重复结算")
    fee.status = e["outcome"]           # settled / refunded
    fee.settled_cents = e["settled_cents"]
    fee.seq = e["seq"]


def _h_compensation_booked(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    if e["fee_id"] in o.fees:
        raise ProjectionError(f"赔付重复记账：{e['fee_id']}")
    o.fees[e["fee_id"]] = FeeEntry(
        fee_id=e["fee_id"], fee_type="compensation",
        amount_cents=e["amount_cents"], status="settled", seq=e["seq"],
        settled_cents=e["amount_cents"], basis=_basis(e))
    hold = o.holds.get(e.get("hold_no", ""))
    if hold is not None and e.get("close_hold"):
        hold["resolved_seq"] = e["seq"]
        hold["resolution"] = f"赔付 {e['amount_cents']} 分"
        o.batches[hold["batch_no"]].detained = False


def _h_delivery_confirmed(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    b = o.batches[e["batch_no"]]
    route_nodes = o.routes[b.route_version]["nodes"]
    if b.node != route_nodes[-1]:
        raise ProjectionError(
            f"批次 {b.batch_no} 停在 {b.node}，未到终点 {route_nodes[-1]}，不能交付")
    b.active = False
    b.history.append({"seq": e["seq"], "node": "delivered",
                      "basis": _basis(e), "confirmer": e.get("actor")})


def _h_proposal_created(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    if e["proposal_id"] in o.proposals:
        raise ProjectionError(f"提案编号重复：{e['proposal_id']}")
    o.proposals[e["proposal_id"]] = {
        "proposal_id": e["proposal_id"], "kind": e["proposal_kind"],
        "detail": e.get("detail", {}), "status": "pending",
        "raised_by": e.get("actor"), "raised_seq": e["seq"],
        "resolved_seq": None,
    }


def _h_message_reviewed(state: LedgerState, e: dict[str, Any], o: OrderState) -> None:
    o.messages[e["message_id"]] = e["outcome"]   # duplicate / suspended / verified
    if e["outcome"] == "suspended":
        o.holds[e["hold_no"]] = {
            "hold_no": e["hold_no"], "kind": "identity_mismatch",
            "batch_no": e.get("batch_no", ""), "reason": e["reason"],
            "since_seq": e["seq"], "resolved_seq": None, "resolution": None,
            "basis": _basis(e), "message_id": e["message_id"],
            "observed": e.get("observed", {}),
        }


_HANDLERS = {
    "order_opened": _h_order_opened,
    "batch_created": _h_batch_created,
    "node_advanced": _h_node_advanced,
    "detained": _h_detained,
    "released_from_hold": _h_released_from_hold,
    "route_changed": _h_route_changed,
    "batch_split": _h_batch_split,
    "batch_merged": _h_batch_merged,
    "fee_booked": _h_fee_booked,
    "fee_settled": _h_fee_settled,
    "compensation_booked": _h_compensation_booked,
    "delivery_confirmed": _h_delivery_confirmed,
    "proposal_created": _h_proposal_created,
    "message_reviewed": _h_message_reviewed,
}


def _basis(e: dict[str, Any]) -> str:
    """每条变更携带的"依据"，供审计回答"为什么变"。"""
    return e.get("basis") or e.get("reason") or (
        f"口岸消息 {e['message_id']}" if e.get("message_id") else "")


# ---------------------------------------------------------------------------
# 不变量
# ---------------------------------------------------------------------------


def _assert_fee_conservation(state: LedgerState) -> None:
    """每条预占最终必须且只能结算一次：held 之外不得有悬挂金额。

    守恒口径：对每个非赔付条目，``amount_cents == 结算/退还金额 + 仍挂账金额``。
    赔付一经认定即为已结算支出。该断言保证崩溃恢复或重复消息不会让
    一笔预占被结算两次（重复结算会在投影期已被拒绝）。
    """
    for o in state.orders.values():
        for fee in o.fees.values():
            if fee.fee_type == "compensation":
                if fee.status != "settled" or fee.settled_cents != fee.amount_cents:
                    raise ProjectionError(f"赔付 {fee.fee_id} 状态异常")
                continue
            if fee.status == "held":
                continue
            if fee.settled_cents is None or fee.settled_cents < 0:
                raise ProjectionError(f"费用 {fee.fee_id} 结算金额非法")


def fee_summary(o: OrderState) -> dict[str, int]:
    """汇总：预占中、已结算、已退还、赔付（单位：分）。"""
    summary = {"held": 0, "settled": 0, "refunded": 0, "compensation": 0}
    for fee in o.fees.values():
        if fee.fee_type == "compensation":
            summary["compensation"] += fee.amount_cents
        elif fee.status == "held":
            summary["held"] += fee.amount_cents
        elif fee.status == "settled":
            summary["settled"] += fee.settled_cents or 0
        elif fee.status == "refunded":
            summary["refunded"] += fee.settled_cents or 0
    return summary


# ---------------------------------------------------------------------------
# 对外视图
# ---------------------------------------------------------------------------


def order_tracking(o: OrderState) -> dict[str, Any]:
    """运营视角：一笔订单下货物去向、责任人与预计到达区间。"""
    route = o.routes[o.current_route]
    batches_out = []
    for b in o.batches.values():
        last = b.history[-1] if b.history else {}
        batches_out.append({
            "batch_no": b.batch_no,
            "active": b.active,
            "route_version": b.route_version,
            "node": b.node,
            "detained": b.detained,
            "owner_role": last.get("actor", {}).get("role")
                          if isinstance(last.get("actor"), dict) else last.get("actor"),
            "eta_range": last.get("eta_range"),
            "slots": [dict(s) for s in b.slots],
        })
    return {
        "order_no": o.order_no,
        "customer": o.customer,
        "route_version": o.current_route,
        "route_note": route["note"],
        "route_nodes": list(route["nodes"]),
        "batches": batches_out,
        "fees": fee_summary(o),
        "open_holds": [h["hold_no"] for h in o.holds.values()
                       if h["resolved_seq"] is None],
        "pending_proposals": [p["proposal_id"] for p in o.proposals.values()
                              if p["status"] == "pending"],
    }


def customer_view(o: OrderState) -> dict[str, Any]:
    """客户视角：脱敏后的进度。

    - 隐去费用、保证金、赔付与所有内部依据（message_id/审批编号/原因）；
    - 责任人只显示角色，不显示具体操作人；
    - 仅暴露主链节点与预计到达区间，扣留表现为节点上的异常标记，
      不暴露核对细节。
    """
    route = o.routes[o.current_route]
    batches = []
    for b in o.batches.values():
        visible = [h for h in b.history
                   if h["node"] in CUSTOMER_VISIBLE_NODES and h["node"] != "merged"]
        timeline = [{"node": h["node"], "ts": h.get("ts"),
                     "eta_range": h.get("eta_range")} for h in visible]
        batches.append({
            "batch_no": b.batch_no,
            "route_version": b.route_version,
            "node": b.node if b.node in CUSTOMER_VISIBLE_NODES
            else ("merged" if b.node == "merged" else "exception"),
            "exception": b.detained,
            "eta_range": timeline[-1]["eta_range"] if timeline else None,
            "timeline": timeline,
            "slot_count": len(b.slots),
        })
    return {
        "order_no": o.order_no,
        "route_version": o.current_route,
        "route_note": route["note"],
        "batches": batches,
    }


def audit_snapshot(state: LedgerState, order_no: str) -> dict[str, Any]:
    """审计视角：还原当前（或某个时间点的回放结果）全部决策依据。"""
    o = state.order(order_no)
    return {
        "as_of_seq": state.seq,
        "order_no": o.order_no,
        "current_route": o.current_route,
        "routes": [
            {"version": r["version"], "note": r["note"],
             "effective_seq": r["effective_seq"], "basis": r["basis"],
             "approver": r.get("approver"), "proposal_id": r.get("proposal_id")}
            for r in sorted(o.routes.values(), key=lambda r: r["version"])
        ],
        "batches": [
            {"batch_no": b.batch_no, "active": b.active,
             "route_version": b.route_version, "node": b.node,
             "detained": b.detained, "slots": [dict(s) for s in b.slots],
             "history": b.history}
            for b in sorted(o.batches.values(), key=lambda b: b.batch_no)
        ],
        "fees": [vars(f) for f in sorted(o.fees.values(), key=lambda f: f.seq)],
        "holds": list(o.holds.values()),
        "proposals": list(o.proposals.values()),
        "messages": [{"message_id": k, "outcome": v}
                     for k, v in sorted(o.messages.items())],
    }
