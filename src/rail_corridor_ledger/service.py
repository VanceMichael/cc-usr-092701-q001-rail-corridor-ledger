"""货运履约应用服务。

服务把外部动作（接单、口岸/承运方回传消息、异常处置、改线与拆并审批、
费用记账）翻译成一个原子事务里的若干事件，交给 :class:`EventStore`
落盘。所有判定都在"重放到当前时刻"的状态上进行：

幂等与挂起
    同一份口岸消息（message_id）重复到达，直接返回上次结论，不产生事件、
    不重复推进节点；消息编号未变但货位箱号或毛重变化，先挂起核对，
    节点保持不动。

区段权限
    承运方只能推进自己负责区段的节点，口岸节点只有口岸角色可写；
    改线与拆并必须先提案、由值班主管批准；客户只能读脱敏视图。

费用一致
    运费预占随接单、放行保证金随放行节点在**同一事务**内记账；
    异常赔付与挂起解除绑定；任何一步写一半崩溃，整笔事务回滚，
    不会多扣也不会跳过检查点。
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable

from .model import (
    DEFAULT_ROUTE,
    NODE_SEGMENTS,
    ROLE_SEGMENTS,
    FEE_COMPENSATION,
    FEE_DEPOSIT,
    FEE_FREIGHT,
    Role,
    Route,
    Slot,
)
from .store import EventStore
from .views import LedgerState, OrderState, ProjectionError, replay


class ServiceError(RuntimeError):
    """业务规则被拒绝（权限、顺序、挂起等）。"""


@dataclass(frozen=True)
class Actor:
    actor_id: str
    role: Role
    name: str = ""

    def as_event(self) -> dict[str, str]:
        return {"id": self.actor_id, "role": self.role.value, "name": self.name}


# 放行保证金的默认金额（分），演示用；真实实现应来自费率表。
DEFAULT_DEPOSIT_CENTS = 200_000


class FulfillmentService:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        # 公开方法在"回放最新状态 → 检查 → 原子追加"期间持锁，
        # 使并发回传的口岸消息不会同时通过幂等/顺序检查。
        self._guard = RLock()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _next_serial(prefix: str, used: set[str]) -> int:
        """从已落盘编号里推导下一个序号，避免进程重启后撞号。"""
        highest = 0
        for ident in used:
            # 形如 ORD-H3 / ORD-P2 / ORD-C1
            match = re.match(rf"^{re.escape(prefix)}(\d+)$", ident)
            if match:
                highest = max(highest, int(match.group(1)))
        return highest + 1

    def _next_hold_no(self, o: OrderState) -> str:
        return f"{o.order_no}-H{self._next_serial(f'{o.order_no}-H', set(o.holds))}"

    def _next_proposal_id(self, o: OrderState) -> str:
        return f"{o.order_no}-P{self._next_serial(f'{o.order_no}-P', set(o.proposals))}"

    def _state(self) -> LedgerState:
        return replay(self.store.events())

    def _commit(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.store.append(events)

    def _order(self, state: LedgerState, order_no: str) -> OrderState:
        try:
            return state.order(order_no)
        except ProjectionError as exc:
            raise ServiceError(str(exc)) from None

    def _require_role(self, actor: Actor, roles: set[Role], action: str) -> None:
        if actor.role not in roles:
            raise ServiceError(f"{actor.role.value} 无权{action}")

    def _require_segment(self, actor: Actor, node: str, action: str) -> None:
        """节点所在区段必须在该角色可写区段内。"""
        segment = NODE_SEGMENTS[node]
        allowed = ROLE_SEGMENTS.get(actor.role, frozenset())
        if segment not in allowed:
            raise ServiceError(
                f"{actor.role.value} 无权操作{segment.value}区段的节点 {node}（{action}）")

    def _open_holds(self, o: OrderState, batch_no: str | None = None) -> list[dict]:
        return [h for h in o.holds.values()
                if h["resolved_seq"] is None
                and (batch_no is None or h.get("batch_no") == batch_no)]

    # ------------------------------------------------------------------
    # 接单与批次
    # ------------------------------------------------------------------

    def open_order(self, actor: Actor, order_no: str, customer: str,
                   freight_cents: int, basis: str = "客户托运委托",
                   route: Route | None = None) -> list[dict[str, Any]]:
        """货代接单：开通订单并在同一事务预占运费。"""
        self._require_role(actor, {Role.FORWARDER}, "开通订单")
        state = self._state()
        if order_no in state.orders:
            raise ServiceError(f"订单已存在：{order_no}")
        route = route or DEFAULT_ROUTE
        if freight_cents <= 0:
            raise ServiceError("运费预占金额必须为正")
        events = [
            {"type": "order_opened", "order_no": order_no, "customer": customer,
             "route_nodes": list(route.nodes), "note": route.note,
             "actor": actor.as_event(), "basis": basis},
            {"type": "fee_booked", "order_no": order_no, "fee_id": f"{order_no}-F01",
             "fee_type": FEE_FREIGHT, "amount_cents": freight_cents,
             "actor": actor.as_event(), "basis": f"接单预占运费：{basis}"},
        ]
        return self._commit(events)

    def create_batch(self, actor: Actor, order_no: str, batch_no: str,
                     slots: list[Slot], basis: str) -> list[dict[str, Any]]:
        self._require_role(actor, {Role.FORWARDER}, "建立批次")
        state = self._state()
        self._order(state, order_no)
        if not slots:
            raise ServiceError("批次至少包含一个货位")
        if any(s.weight_kg <= 0 for s in slots):
            raise ServiceError("货位毛重必须为正")
        event = {"type": "batch_created", "order_no": order_no,
                 "batch_no": batch_no,
                 "slots": [{"slot_no": s.slot_no, "container_no": s.container_no,
                            "weight_kg": s.weight_kg, "goods": s.goods} for s in slots],
                 "actor": actor.as_event(), "basis": basis}
        return self._commit([event])

    # ------------------------------------------------------------------
    # 外部消息（装箱/换装/查验/放行…）：幂等 + 挂起核对 + 区段权限
    # ------------------------------------------------------------------

    def ingest(self, actor: Actor, order_no: str, message_id: str,
               batch_no: str, node: str, *, basis: str,
               observed_slots: list[dict[str, Any]] | None = None,
               eta_range: dict[str, str] | None = None,
               ) -> list[dict[str, Any]]:
        """处理一条货代/口岸/承运方回传消息。

        ``observed_slots`` 携带消息里实际观察到的货位
        ``{slot_no, container_no, weight_kg}``。编号未变而箱号或重量
        变化时，消息挂起（suspended），不推进任何节点。

        返回值为空列表表示消息是重复件，被幂等丢弃。
        """
        state = self._state()
        prior = state.message_seen(message_id)
        if prior is not None:
            # 同一份消息重复到达：什么都不写，节点绝不重复推进。
            return []

        o = self._order(state, order_no)
        batch = self._active_batch(o, batch_no)
        self._require_segment(actor, node, "回传节点")
        if self._open_holds(o, batch_no):
            raise ServiceError(f"批次 {batch_no} 存在未解除的挂起，先核对再推进")

        # 节点必须沿当前路线向前，不允许跳点或回退。
        route_nodes = list(o.routes[batch.route_version]["nodes"])
        cur_idx = route_nodes.index(batch.node) if batch.node in route_nodes else -1
        try:
            next_idx = route_nodes.index(node)
        except ValueError:
            raise ServiceError(f"节点 {node} 不在批次当前路线 v{batch.route_version} 上")
        if next_idx != cur_idx + 1:
            raise ServiceError(
                f"节点不能从 {batch.node} 跳到 {node}（只能推进到下一个检查点）")

        mismatch = self._detect_slot_mismatch(batch, observed_slots or [])
        if mismatch is not None:
            hold_no = self._next_hold_no(o)
            event = {
                "type": "message_reviewed", "order_no": order_no,
                "message_id": message_id, "outcome": "suspended",
                "hold_no": hold_no, "batch_no": batch_no,
                "reason": mismatch, "observed": {"slots": observed_slots},
                "actor": actor.as_event(),
                "basis": f"编号未变但货位/重量变化，挂起核对：{basis}",
            }
            return self._commit([event])

        events = [{
            "type": "message_reviewed", "order_no": order_no,
            "message_id": message_id, "outcome": "verified",
            "actor": actor.as_event(), "basis": basis,
        }]
        # 放行保证金先于节点事件入同一事务：审计停在放行 seq 时，
        # 保证金必然已在账，体现"费用与节点变更一致"。
        if node == "released":
            events.append({
                "type": "fee_booked", "order_no": order_no,
                "fee_id": f"{order_no}-D{len([f for f in o.fees.values() if f.fee_type == FEE_DEPOSIT]) + 1}",
                "fee_type": FEE_DEPOSIT, "amount_cents": DEFAULT_DEPOSIT_CENTS,
                "actor": actor.as_event(),
                "basis": f"霍尔果斯放行预存保证金（消息 {message_id}）",
            })
        advance = {
            "type": "node_advanced", "order_no": order_no, "batch_no": batch_no,
            "node": node, "message_id": message_id,
            "actor": actor.as_event(), "basis": basis,
        }
        if eta_range:
            advance["eta_range"] = eta_range
        events.append(advance)
        return self._commit(events)

    def _active_batch(self, o: OrderState, batch_no: str) -> Any:
        b = o.batches.get(batch_no)
        if b is None:
            raise ServiceError(f"批次不存在：{batch_no}")
        if not b.active:
            raise ServiceError(f"批次 {batch_no} 已关闭")
        return b

    def _detect_slot_mismatch(self, batch: Any, observed: list[dict[str, Any]]) -> str | None:
        """比对消息观察值与台账货位；返回差异描述，无差异返回 None。"""
        for obs in observed:
            current = next((s for s in batch.slots if s["slot_no"] == obs["slot_no"]), None)
            if current is None:
                # 新货位出现在已有批次里也属于需要核对的异常。
                return f"出现台账外货位 {obs['slot_no']}"
            if obs.get("container_no") and obs["container_no"] != current["container_no"]:
                return (f"货位 {obs['slot_no']} 箱号变化："
                        f"{current['container_no']} → {obs['container_no']}")
            if obs.get("weight_kg") is not None and \
                    float(obs["weight_kg"]) != float(current["weight_kg"]):
                return (f"货位 {obs['slot_no']} 毛重变化："
                        f"{current['weight_kg']}kg → {obs['weight_kg']}kg")
        return None

    # ------------------------------------------------------------------
    # 异常扣留、挂起核对与赔付
    # ------------------------------------------------------------------

    def detain(self, actor: Actor, order_no: str, batch_no: str,
               reason: str, *, message_id: str, hold_no: str | None = None
               ) -> list[dict[str, Any]]:
        """口岸/承运方上报异常扣留（查验异常、边境滞留等）。"""
        state = self._state()
        if state.message_seen(message_id) is not None:
            return []
        o = self._order(state, order_no)
        self._active_batch(o, batch_no)
        if actor.role not in (Role.PORT, Role.CARRIER):
            raise ServiceError("只有口岸或承运方可以上报扣留")
        hold_no = hold_no or self._next_hold_no(o)
        events = [{
            "type": "message_reviewed", "order_no": order_no,
            "message_id": message_id, "outcome": "detained",
            "actor": actor.as_event(), "basis": reason,
        }, {
            "type": "detained", "order_no": order_no, "batch_no": batch_no,
            "hold_no": hold_no, "reason": reason, "message_id": message_id,
            "actor": actor.as_event(), "basis": reason,
        }]
        return self._commit(events)

    def resume_after_check(self, actor: Actor, order_no: str, hold_no: str,
                           resolution: str, resume_node: str,
                           basis: str) -> list[dict[str, Any]]:
        """核对完成后解除挂起并恢复到指定节点。

        货位/重量差异类挂起由货代运营经理核对解除；口岸扣留由口岸解除。
        """
        state = self._state()
        o = self._order(state, order_no)
        hold = o.holds.get(hold_no)
        if hold is None or hold["resolved_seq"] is not None:
            raise ServiceError("挂起不存在或已解除")
        if hold["kind"] == "identity_mismatch":
            self._require_role(actor, {Role.FORWARDER, Role.SUPERVISOR}, "解除核对挂起")
        else:
            self._require_role(actor, {Role.PORT, Role.SUPERVISOR}, "解除扣留")
        batch = o.batches[hold["batch_no"]]
        route_nodes = list(o.routes[batch.route_version]["nodes"])
        if resume_node not in route_nodes:
            raise ServiceError(f"恢复节点 {resume_node} 不在当前路线上")
        cur_idx = route_nodes.index(batch.node) if batch.node in route_nodes else -1
        if route_nodes.index(resume_node) != cur_idx + 1:
            raise ServiceError(
                f"恢复节点必须是 {batch.node} 的下一个检查点，不能跳到 {resume_node}")
        event = {
            "type": "released_from_hold", "order_no": order_no, "hold_no": hold_no,
            "resolution": resolution, "resume_node": resume_node,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    def book_compensation(self, actor: Actor, order_no: str, hold_no: str,
                          amount_cents: int, basis: str, *,
                          close_hold: bool = True) -> list[dict[str, Any]]:
        """异常赔付与扣留挂起在同一事务处理，值班主管批准。"""
        self._require_role(actor, {Role.SUPERVISOR, Role.FORWARDER}, "登记异常赔付")
        if amount_cents <= 0:
            raise ServiceError("赔付金额必须为正")
        state = self._state()
        o = self._order(state, order_no)
        if hold_no not in o.holds:
            raise ServiceError(f"挂起不存在：{hold_no}")
        comp_no = f"{order_no}-C{sum(1 for f in o.fees.values() if f.fee_type == FEE_COMPENSATION) + 1}"
        event = {
            "type": "compensation_booked", "order_no": order_no,
            "fee_id": comp_no, "hold_no": hold_no,
            "amount_cents": amount_cents, "close_hold": close_hold,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    # ------------------------------------------------------------------
    # 改线、拆批、合批：提案 + 主管批准
    # ------------------------------------------------------------------

    def propose_route_change(self, actor: Actor, order_no: str,
                             new_nodes: list[str], note: str,
                             reason: str) -> dict[str, Any]:
        self._require_role(actor, {Role.FORWARDER, Role.CARRIER}, "提出改线")
        state = self._state()
        o = self._order(state, order_no)
        unknown = [n for n in new_nodes if n not in NODE_SEGMENTS]
        if unknown:
            raise ServiceError(f"路线包含未知节点：{unknown}")
        proposal_id = self._next_proposal_id(o)
        self._commit([{
            "type": "proposal_created", "order_no": order_no,
            "proposal_id": proposal_id, "proposal_kind": "route_change",
            "detail": {"nodes": new_nodes, "note": note},
            "actor": actor.as_event(), "basis": reason,
        }])
        return {"proposal_id": proposal_id, "new_version": o.current_route + 1}

    def approve_route_change(self, actor: Actor, order_no: str,
                             proposal_id: str, basis: str) -> list[dict[str, Any]]:
        self._require_role(actor, {Role.SUPERVISOR}, "批准改线")
        state = self._state()
        o = self._order(state, order_no)
        proposal = self._pending_proposal(o, proposal_id, "route_change")
        new_nodes = proposal["detail"]["nodes"]
        # 已到达的节点不能被路线改动抹掉：所有活跃批次的当前节点
        # 必须仍出现在新路线上，且不能回退。
        for b in o.batches.values():
            if not b.active:
                continue
            old_nodes = list(o.routes[b.route_version]["nodes"])
            if b.node not in new_nodes:
                raise ServiceError(
                    f"新路线缺少批次 {b.batch_no} 当前节点 {b.node}，无法改线")
            if new_nodes.index(b.node) < old_nodes.index(b.node):
                raise ServiceError(
                    f"新路线会让批次 {b.batch_no} 的节点回退，拒绝改线")
        new_version = o.current_route + 1
        event = {
            "type": "route_changed", "order_no": order_no,
            "new_version": new_version, "nodes": new_nodes,
            "note": proposal["detail"].get("note", ""),
            "proposal_id": proposal_id,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    def propose_split(self, actor: Actor, order_no: str, batch_no: str,
                      moved_slots: list[str], reason: str) -> str:
        self._require_role(actor, {Role.FORWARDER, Role.CARRIER}, "提出拆批")
        state = self._state()
        o = self._order(state, order_no)
        batch = self._active_batch(o, batch_no)
        if not moved_slots or any(s not in {x["slot_no"] for x in batch.slots}
                                  for s in moved_slots):
            raise ServiceError("拆出货位必须都在源批次中")
        if len(moved_slots) == len(batch.slots):
            raise ServiceError("不能把批次全部货位拆走")
        proposal_id = self._next_proposal_id(o)
        self._commit([{
            "type": "proposal_created", "order_no": order_no,
            "proposal_id": proposal_id, "proposal_kind": "batch_split",
            "detail": {"batch_no": batch_no, "moved_slots": moved_slots},
            "actor": actor.as_event(), "basis": reason,
        }])
        return proposal_id

    def approve_split(self, actor: Actor, order_no: str, proposal_id: str,
                      new_batch_no: str, basis: str) -> list[dict[str, Any]]:
        self._require_role(actor, {Role.SUPERVISOR}, "批准拆批")
        state = self._state()
        o = self._order(state, order_no)
        proposal = self._pending_proposal(o, proposal_id, "batch_split")
        if new_batch_no in o.batches:
            raise ServiceError(f"新批次号已存在：{new_batch_no}")
        event = {
            "type": "batch_split", "order_no": order_no,
            "batch_no": proposal["detail"]["batch_no"],
            "moved_slots": proposal["detail"]["moved_slots"],
            "new_batch_no": new_batch_no,
            "proposal_id": proposal_id,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    def propose_merge(self, actor: Actor, order_no: str, target_batch_no: str,
                      source_batch_nos: list[str], reason: str) -> str:
        self._require_role(actor, {Role.FORWARDER, Role.CARRIER}, "提出合批")
        state = self._state()
        o = self._order(state, order_no)
        if target_batch_no in source_batch_nos or not source_batch_nos:
            raise ServiceError("合批源批次不能包含目标批次且不能为空")
        for no in [target_batch_no, *source_batch_nos]:
            self._active_batch(o, no)
        proposal_id = self._next_proposal_id(o)
        self._commit([{
            "type": "proposal_created", "order_no": order_no,
            "proposal_id": proposal_id, "proposal_kind": "batch_merge",
            "detail": {"target_batch_no": target_batch_no,
                       "source_batch_nos": list(source_batch_nos)},
            "actor": actor.as_event(), "basis": reason,
        }])
        return proposal_id

    def approve_merge(self, actor: Actor, order_no: str, proposal_id: str,
                      basis: str) -> list[dict[str, Any]]:
        self._require_role(actor, {Role.SUPERVISOR}, "批准合批")
        state = self._state()
        o = self._order(state, order_no)
        proposal = self._pending_proposal(o, proposal_id, "batch_merge")
        event = {
            "type": "batch_merged", "order_no": order_no,
            "target_batch_no": proposal["detail"]["target_batch_no"],
            "source_batch_nos": proposal["detail"]["source_batch_nos"],
            "proposal_id": proposal_id,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    def _pending_proposal(self, o: OrderState, proposal_id: str, kind: str) -> dict:
        proposal = o.proposals.get(proposal_id)
        if proposal is None:
            raise ServiceError(f"提案不存在：{proposal_id}")
        if proposal["kind"] != kind:
            raise ServiceError(f"提案 {proposal_id} 类型不是 {kind}")
        if proposal["status"] != "pending":
            raise ServiceError(f"提案 {proposal_id} 已{proposal['status']}")
        return proposal

    # ------------------------------------------------------------------
    # 费用结算与交付
    # ------------------------------------------------------------------

    def settle_fee(self, actor: Actor, order_no: str, fee_id: str,
                   outcome: str, settled_cents: int, basis: str
                   ) -> list[dict[str, Any]]:
        """预占转实收或保证金退还；同一笔费用只能结算一次。"""
        self._require_role(actor, {Role.FORWARDER, Role.SUPERVISOR}, "结算费用")
        if outcome not in ("settled", "refunded"):
            raise ServiceError("结算结果只能是 settled 或 refunded")
        if settled_cents < 0:
            raise ServiceError("结算金额不能为负")
        state = self._state()
        o = self._order(state, order_no)
        fee = o.fees.get(fee_id)
        if fee is None:
            raise ServiceError(f"费用条目不存在：{fee_id}")
        if fee.status != "held":
            raise ServiceError(f"费用 {fee_id} 已{fee.status}，禁止重复结算")
        event = {
            "type": "fee_settled", "order_no": order_no, "fee_id": fee_id,
            "outcome": outcome, "settled_cents": settled_cents,
            "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])

    def confirm_delivery(self, actor: Actor, order_no: str, batch_no: str,
                         basis: str) -> list[dict[str, Any]]:
        self._require_role(actor, {Role.FORWARDER, Role.CARRIER}, "确认交付")
        state = self._state()
        o = self._order(state, order_no)
        batch = self._active_batch(o, batch_no)
        if self._open_holds(o, batch_no):
            raise ServiceError("批次还有未关闭的挂起，不能交付")
        route_nodes = list(o.routes[batch.route_version]["nodes"])
        if batch.node != route_nodes[-1]:
            raise ServiceError(
                f"批次 {batch_no} 停在 {batch.node}，未到终点 {route_nodes[-1]}，不能交付")
        event = {
            "type": "delivery_confirmed", "order_no": order_no,
            "batch_no": batch_no, "actor": actor.as_event(), "basis": basis,
        }
        return self._commit([event])


def _synchronized(method: Callable) -> Callable:
    @functools.wraps(method)
    def wrapped(self, *args: Any, **kwargs: Any) -> Any:
        with self._guard:
            return method(self, *args, **kwargs)
    return wrapped


# 所有对外方法串行化"检查—提交"区间；内部 _ 开头的辅助方法不重复加锁，
# 配合可重入锁避免自死锁。
for _name, _method in list(vars(FulfillmentService).items()):
    if not _name.startswith("_") and callable(_method):
        setattr(FulfillmentService, _name, _synchronized(_method))
