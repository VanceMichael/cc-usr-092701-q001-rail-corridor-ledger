"""领域模型：纯函数决策 + 事件折叠，不依赖存储与时间来源。

角色：
- forwarder  货代运营经理：建单、发起改线/拆并提案
- carrier    承运方：只能推进自己负责区段的节点
- customs    口岸查验人员：霍尔果斯查验、放行、异常登记
- customer   客户：只读，视图脱敏
- supervisor 值班主管：解除异常、批准改线与批次拆并、裁决挂起报文

不变量（每次折叠都校验，违反即拒绝整个事务）：
1. 同一报文编号 + 同一内容指纹：只生效一次；编号不变但货位/重量变化：挂起核对；
2. 节点必须按路线顺序推进，处于异常扣留的货位不能继续下行；
3. 承运方只能操作本人承运区段的节点；
4. 改线、拆批、合批必须存在主管批准的提案；
5. 每个资金事务借贷相等，费用预占/保证金/赔付随同一节点事务落账；
6. 全局事件 seq 连续（由 events.EventStore 保证），任何时点可重放还原。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable

# --------------------------------------------------------------------- 角色


class Role:
    FORWARDER = "forwarder"
    CARRIER = "carrier"
    CUSTOMS = "customs"
    CUSTOMER = "customer"
    SUPERVISOR = "supervisor"


@dataclass(frozen=True)
class Actor:
    actor_id: str
    role: str
    carrier_id: str | None = None  # 仅承运方需要


# 客户视图里承运方编号统一替换为区段名
SEGMENT_LABELS = {
    "nanchang_khorgos": "南昌—霍尔果斯承运段",
    "khorgos": "霍尔果斯口岸",
    "khorgos_bishkek": "霍尔果斯—比什凯克承运段",
}

CUSTOMS_KINDS = frozenset({"inspection", "release"})


# --------------------------------------------------------------------- 异常


class DomainError(Exception):
    """所有业务规则违反的基类。"""


class AuthError(DomainError):
    """角色或区段越权。"""


class OrderError(DomainError):
    """订单/货位/批次状态不允许该操作。"""


class RoutingError(DomainError):
    """节点不属于当前路线版本或违反顺序。"""


class ProposalError(DomainError):
    """提案状态或审批人不符合要求。"""


class InvariantError(DomainError):
    """折叠出的状态违反系统不变量（资金不平、序号跳跃等）。"""


# --------------------------------------------------------------------- 工具


def _w(value: str | Decimal) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


# 各账户的正常余额方向：反方向过账后余额不得为负
# （例如 cash 借增贷减，保证金退款的贷方不能超过历史借方）
ACCOUNT_NORMAL = {
    "cash": "debit",
    "fee_hold": "credit",
    "deposit_hold": "credit",
    "fee_income": "credit",
    "claim_expense": "debit",
    "claim_payable": "credit",
}


def _ev(etype: str, **data: Any) -> dict:
    return {"type": etype, "data": data}


# --------------------------------------------------------------------- 状态


@dataclass
class Slot:
    slot_id: str
    weight: Decimal
    batch_id: str
    fee_share: int = 0
    deposit_share: int = 0
    status: str = "planned"  # planned/in_transit/delivered
    history: list[dict] = field(default_factory=list)


@dataclass
class Batch:
    batch_id: str
    slots: set[str] = field(default_factory=set)
    carrier_id: str | None = None
    active: bool = True
    parent: str | None = None
    merged_into: str | None = None


@dataclass
class Suspension:
    suspension_id: str
    msg_id: str
    kind: str  # weight_mismatch / identity_conflict
    details: dict
    slot_ids: list[str]
    status: str = "pending"  # pending/confirmed/discarded


@dataclass
class Proposal:
    proposal_id: str
    kind: str  # reroute/split/merge
    payload: dict
    reason: str
    proposer: str
    status: str = "open"  # open/approved/rejected
    decide_ts: str | None = None


@dataclass
class State:
    order_id: str | None = None
    customer_id: str | None = None
    fee_total: int = 0
    deposit_total: int = 0
    currency: str = "CNY"
    route_version: int = 0
    routes: dict[int, dict] = field(default_factory=dict)
    slots: dict[str, Slot] = field(default_factory=dict)
    batches: dict[str, Batch] = field(default_factory=dict)
    # (slot_id, route_version, node_id) -> {"status": reached/held, ...}
    node_states: dict[tuple[str, int, str], dict] = field(default_factory=dict)
    exceptions: dict[str, dict] = field(default_factory=dict)
    suspensions: dict[str, Suspension] = field(default_factory=dict)
    proposals: dict[str, Proposal] = field(default_factory=dict)
    money: list[dict] = field(default_factory=list)
    # account -> {"debit": int, "credit": int}，任何分录过账后都不得击穿余额下限
    balances: dict[str, dict] = field(default_factory=dict)
    # msg_id -> {"fingerprint": str, "result": str, "suspension_id": str|None}
    messages: dict[str, dict] = field(default_factory=dict)
    completed: bool = False

    # ---------------------------------------------------------- 路线辅助
    def route(self, version: int | None = None) -> dict:
        return self.routes[version if version is not None else self.route_version]

    def node(self, node_id: str, version: int | None = None) -> dict:
        route = self.route(version)
        for n in route["nodes"]:
            if n["id"] == node_id:
                return n
        raise RoutingError(f"节点 {node_id} 不属于路线 v{route['version']}")

    def node_index(self, node_id: str, version: int | None = None) -> int:
        route = self.route(version)
        return next(i for i, n in enumerate(route["nodes"]) if n["id"] == node_id)

    def last_reached(self, slot_id: str, version: int | None = None) -> int:
        """已到达节点的最大下标，-1 表示尚未出发。"""
        version = self.route_version if version is None else version
        last = -1
        for i, n in enumerate(self.routes[version]["nodes"]):
            st = self.node_states.get((slot_id, version, n["id"]))
            if st and st["status"] == "reached":
                last = i
        return last

    def active_slots_of(self, batch_id: str) -> list[str]:
        batch = self.batches.get(batch_id)
        if batch is None or not batch.active:
            raise OrderError(f"批次 {batch_id} 不存在或已失效")
        return sorted(batch.slots)

    def slot_or_raise(self, slot_id: str) -> Slot:
        slot = self.slots.get(slot_id)
        if slot is None:
            raise OrderError(f"未知货位 {slot_id}")
        return slot


# =====================================================================
# apply：事件折叠（唯一的状态变更入口）
# =====================================================================


def apply(state: State, event: dict) -> None:
    etype = event["type"]
    d = event["data"]

    if etype == "OrderPlaced":
        state.order_id = d["order_id"]
        state.customer_id = d["customer_id"]
        state.fee_total = d["fee_total"]
        state.deposit_total = d.get("deposit_total", 0)
        state.currency = d.get("currency", "CNY")
        state.route_version = d["route"]["version"]
        state.routes[state.route_version] = d["route"]
        for s in d["slots"]:
            state.slots[s["slot_id"]] = Slot(
                slot_id=s["slot_id"], weight=_w(s["weight"]), batch_id=s["batch_id"],
                fee_share=s.get("fee_share", 0), deposit_share=s.get("deposit_share", 0),
            )
        for b in d["batches"]:
            state.batches[b["batch_id"]] = Batch(
                batch_id=b["batch_id"],
                slots=set(b["slot_ids"]),
                carrier_id=b.get("carrier_id"),
            )

    elif etype == "SlotCheckpointReached":
        rv, node_id = d["route_version"], d["node_id"]
        for sid in d["slot_ids"]:
            key = (sid, rv, node_id)
            assert state.node_states.get(key) is None, "检查点被重复推进"
            state.node_states[key] = {
                "status": "reached",
                "msg_id": d.get("msg_id"),
                "ts": event["ts"],
                "actor": event.get("actor"),
                "evidence": d.get("evidence", {}),
            }
            state.slots[sid].history.append(
                {"node_id": node_id, "route_version": rv, "ts": event["ts"],
                 "msg_id": d.get("msg_id")}
            )
            if state.node(node_id, rv)["kind"] == "deliver":
                state.slots[sid].status = "delivered"
            elif state.slots[sid].status == "planned":
                state.slots[sid].status = "in_transit"

    elif etype == "SlotExceptionOpened":
        for sid in d["slot_ids"]:
            key = (sid, d["route_version"], d["node_id"])
            state.node_states[key] = {
                "status": "held",
                "exception_id": d["exception_id"],
                "ts": event["ts"],
                "msg_id": d.get("msg_id"),
            }
        state.exceptions[d["exception_id"]] = {
            "exception_id": d["exception_id"],
            "slot_ids": list(d["slot_ids"]),
            "route_version": d["route_version"],
            "node_id": d["node_id"],
            "reason": d["reason"],
            "status": "open",
            "opened_ts": event["ts"],
        }

    elif etype == "SlotExceptionResolved":
        exc = state.exceptions[d["exception_id"]]
        exc["status"] = "resolved"
        exc["resolved_ts"] = event["ts"]
        exc["claim_amount"] = d.get("claim_amount", 0)
        for sid in exc["slot_ids"]:
            key = (sid, exc["route_version"], exc["node_id"])
            state.node_states[key] = {
                "status": "reached",
                "exception_id": exc["exception_id"],
                "resolved_ts": event["ts"],
                "actor": event.get("actor"),
            }

    elif etype == "MessageRecorded":
        # 已生效报文的幂等指纹：必须来自事件，重开后仍可去重
        state.messages[d["msg_id"]] = {
            "fingerprint": d["fingerprint"],
            "result": d["result"],
            "suspension_id": d.get("suspension_id"),
        }

    elif etype == "MessageSuspended":
        state.suspensions[d["suspension_id"]] = Suspension(
            suspension_id=d["suspension_id"],
            msg_id=d["msg_id"],
            kind=d["kind"],
            details=d["details"],
            slot_ids=list(d["slot_ids"]),
        )
        rec = state.messages.setdefault(
            d["msg_id"], {"fingerprint": d["details"].get("fingerprint"),
                          "result": "suspended", "suspension_id": None}
        )
        rec["result"] = "suspended"
        rec["suspension_id"] = d["suspension_id"]

    elif etype == "SuspensionResolved":
        sus = state.suspensions[d["suspension_id"]]
        sus.status = d["action"]  # confirmed / discarded

    elif etype == "SlotWeightAdjusted":
        for sid, w in d["weights"].items():
            state.slots[sid].weight = _w(w)

    elif etype == "ProposalOpened":
        state.proposals[d["proposal_id"]] = Proposal(
            proposal_id=d["proposal_id"],
            kind=d["kind"],
            payload=d["payload"],
            reason=d["reason"],
            proposer=d["proposer"],
        )

    elif etype == "ProposalApproved":
        p = state.proposals[d["proposal_id"]]
        p.status = "approved"
        p.decide_ts = event["ts"]
        p.decider = event.get("actor")

    elif etype == "ProposalRejected":
        p = state.proposals[d["proposal_id"]]
        p.status = "rejected"
        p.decide_ts = event["ts"]
        p.decider = event.get("actor")

    elif etype == "RouteActivated":
        new_version = d["route"]["version"]
        assert new_version == state.route_version + 1, "路线版本必须递增"
        state.routes[new_version] = d["route"]
        state.route_version = new_version
        # 已走完的旧节点迁移到新路线前 carry_to_index 个节点，依据即本提案。
        carry = d.get("carry_to_index", 0)
        for sid in state.slots:
            for i, n in enumerate(d["route"]["nodes"]):
                if i < carry:
                    state.node_states[(sid, new_version, n["id"])] = {
                        "status": "reached",
                        "carried_from_version": d["from_version"],
                        "proposal_id": d.get("proposal_id"),
                        "ts": event["ts"],
                    }

    elif etype == "BatchSplit":
        parent = state.batches[d["parent_batch"]]
        parent.active = False
        for child in d["children"]:
            for sid in child["slot_ids"]:
                state.slots[sid].batch_id = child["batch_id"]
                parent.slots.discard(sid)
            state.batches[child["batch_id"]] = Batch(
                batch_id=child["batch_id"],
                slots=set(child["slot_ids"]),
                carrier_id=child.get("carrier_id"),
                parent=parent.batch_id,
            )

    elif etype == "BatchMerged":
        merged_slots: set[str] = set()
        for bid in d["batches"]:
            b = state.batches[bid]
            b.active = False
            b.merged_into = d["result_batch_id"]
            merged_slots |= b.slots
        result = Batch(
            batch_id=d["result_batch_id"],
            slots=set(merged_slots),
            carrier_id=d.get("carrier_id"),
        )
        state.batches[d["result_batch_id"]] = result
        for sid in merged_slots:
            state.slots[sid].batch_id = d["result_batch_id"]

    elif etype == "SlotDelivered":
        for sid in d["slot_ids"]:
            state.slots[sid].status = "delivered"

    elif etype == "OrderCompleted":
        state.completed = True

    elif etype == "MoneyEntry":
        state.money.append(d)

    else:
        raise InvariantError(f"未知事件类型：{etype}")


def rebuild(envelopes: Iterable[dict]) -> State:
    """从事件信封流重建状态，按事务分组断言资金守恒。"""
    state = State()
    current_tx = None
    tx_entries: list[dict] = []

    def _check_tx() -> None:
        debit = sum(e["amount"] for e in tx_entries if e["direction"] == "debit")
        credit = sum(e["amount"] for e in tx_entries if e["direction"] == "credit")
        if debit != credit:
            raise InvariantError(
                f"事务 {current_tx} 资金不平：借 {debit} ≠ 贷 {credit}"
            )
        # 把本事务的净额并入账户，校验各账户不被反向击穿
        delta: dict[str, dict] = {}
        for e in tx_entries:
            d = delta.setdefault(e["account"], {"debit": 0, "credit": 0})
            d[e["direction"]] += e["amount"]
        for account, dd in delta.items():
            bal = state.balances.setdefault(account, {"debit": 0, "credit": 0})
            bal["debit"] += dd["debit"]
            bal["credit"] += dd["credit"]
            normal = ACCOUNT_NORMAL.get(account)
            if normal == "debit" and bal["debit"] < bal["credit"]:
                raise InvariantError(
                    f"账户 {account} 余额被反向击穿：借 {bal['debit']} < 贷 {bal['credit']}")
            if normal == "credit" and bal["credit"] < bal["debit"]:
                raise InvariantError(
                    f"账户 {account} 余额被反向击穿：贷 {bal['credit']} < 借 {bal['debit']}")

    expected_seq = 1
    for env in envelopes:
        if env["seq"] != expected_seq:
            raise InvariantError(
                f"事件序号跳跃：期望 {expected_seq}，实际 {env['seq']}"
            )
        expected_seq += 1
        if current_tx != env["tx"]:
            if current_tx is not None:
                _check_tx()
            current_tx = env["tx"]
            tx_entries = []
        if env["type"] == "MoneyEntry":
            tx_entries.append(env["data"])
        apply(state, env)

    if current_tx is not None:
        _check_tx()
    return state


# =====================================================================
# 资金分录：金额为整数（最小货币单位），每批必须借贷相等
# =====================================================================


def _money(entry_id: str, account: str, amount: int, direction: str,
           reason: str, ref: dict) -> dict:
    if amount < 0:
        raise InvariantError("金额不允许为负")
    return _ev(
        "MoneyEntry",
        entry_id=entry_id, account=account, amount=amount,
        direction=direction, reason=reason, ref=ref,
    )


def _weight_share(slot_ids: list[str], weights: dict[str, Decimal],
                  total: int) -> dict[str, int]:
    """按重量把总额摊到货位；尾差归最后一个货位，保证合计分毫不差。"""
    grand = sum(weights[sid] for sid in slot_ids)
    if grand == 0:
        raise InvariantError("货位总重量为零，无法分摊金额")
    out: dict[str, int] = {}
    running = 0
    for i, sid in enumerate(slot_ids):
        if i == len(slot_ids) - 1:
            out[sid] = total - running
        else:
            part = int(total * weights[sid] / grand)  # 整除向下
            running += part
            out[sid] = part
    return out


# =====================================================================
# decide：命令 → 事件（纯函数，不做 IO）
# =====================================================================


def _require_roles(actor: Actor, roles: set[str]) -> None:
    if actor.role not in roles:
        raise AuthError(f"角色 {actor.role} 无权执行该操作（需要 {'/'.join(sorted(roles))}）")


def decide_place_order(state: State | None, actor: Actor, cmd: dict) -> list[dict]:
    """货代建单：登记路线 v1、货位、初始批次，同时预占全程费用。

    费用与（预计的）放行保证金按重量预先分摊到每个货位并写进建单事件，
    之后无论怎样拆批、分批放行与交付，都只引用既定份额，保证总额守恒。
    """
    _require_roles(actor, {Role.FORWARDER})
    if state is not None and state.order_id is not None:
        raise OrderError(f"台账中已存在订单 {state.order_id}，不能重复建单")
    slots = cmd["slots"]
    if not slots:
        raise OrderError("订单至少要有一个货位")
    ids = [s["slot_id"] for s in slots]
    if len(set(ids)) != len(ids):
        raise OrderError("货位编号重复")
    total_weight = sum((_w(s["weight"]) for s in slots), Decimal(0))
    if total_weight <= 0:
        raise OrderError("货位总重量必须为正")
    fee_total = int(cmd["fee_total"])
    if fee_total <= 0:
        raise OrderError("费用预占额必须为正")
    deposit_total = int(cmd.get("deposit_total", 0))
    if deposit_total < 0:
        raise OrderError("放行保证金不能为负")

    batches = cmd.get("batches")
    if batches is None:
        batches = [
            {"batch_id": f"B-{s['slot_id']}", "slot_ids": [s["slot_id"]],
             "carrier_id": cmd.get("carrier_id")}
            for s in slots
        ]
    covered = [sid for b in batches for sid in b["slot_ids"]]
    if sorted(covered) != sorted(ids):
        raise OrderError("初始批次必须不重不漏覆盖全部货位")

    route = dict(cmd["route"])
    route["version"] = 1
    _validate_route(route)

    weights = {s["slot_id"]: _w(s["weight"]) for s in slots}
    fee_parts = _weight_share(ids, weights, fee_total)
    dep_parts = _weight_share(ids, weights, deposit_total) if deposit_total else {
        sid: 0 for sid in ids
    }
    batch_of = {sid: b["batch_id"] for b in batches for sid in b["slot_ids"]}
    slot_payload = [
        {"slot_id": sid, "weight": str(weights[sid]),
         "batch_id": batch_of[sid],
         "fee_share": fee_parts[sid], "deposit_share": dep_parts[sid]}
        for sid in ids
    ]

    events = [
        _ev(
            "OrderPlaced",
            order_id=cmd["order_id"],
            customer_id=cmd["customer_id"],
            fee_total=fee_total,
            deposit_total=deposit_total,
            currency=cmd.get("currency", "CNY"),
            route=route,
            slots=slot_payload,
            batches=batches,
        )
    ]
    events.append(
        _money("m-booking-fee", "cash", fee_total, "debit", "客户缴纳运费、费用预占",
               {"order_id": cmd["order_id"]})
    )
    events.append(
        _money("m-booking-fee", "fee_hold", fee_total, "credit", "费用预占",
               {"order_id": cmd["order_id"]})
    )
    return events


def _validate_route(route: dict) -> None:
    nodes = route.get("nodes")
    if not nodes:
        raise RoutingError("路线至少包含一个节点")
    seen: set[str] = set()
    for n in nodes:
        if n["id"] in seen:
            raise RoutingError(f"路线内节点编号重复：{n['id']}")
        seen.add(n["id"])
        for k in ("id", "name", "segment", "kind", "carrier_id", "eta"):
            if k not in n:
                raise RoutingError(f"节点 {n.get('id')} 缺少字段 {k}")
        eta = n["eta"]
        if eta["earliest"] > eta["latest"]:
            raise RoutingError(f"节点 {n['id']} 预计区间倒挂")


def _authorize_node(actor: Actor, node: dict) -> None:
    if actor.role == Role.CUSTOMS:
        if node["kind"] not in CUSTOMS_KINDS or node["segment"] != "khorgos":
            raise AuthError("口岸人员只能登记霍尔果斯的查验/放行节点")
        return
    if actor.role == Role.CARRIER:
        if node["kind"] in CUSTOMS_KINDS:
            raise AuthError("查验/放行只能由口岸人员登记")
        if actor.carrier_id != node["carrier_id"]:
            raise AuthError(
                f"承运方 {actor.carrier_id} 无权维护区段（责任人 {node['carrier_id']}）"
            )
        return
    raise AuthError(f"角色 {actor.role} 不能登记节点")


def _target_slots(state: State, cmd: dict) -> list[str]:
    if cmd.get("batch_id"):
        return state.active_slots_of(cmd["batch_id"])
    sids = cmd.get("slot_ids")
    if not sids:
        raise OrderError("必须指定批次或货位")
    for sid in sids:
        state.slot_or_raise(sid)
    return sorted(sids)


def _fingerprint(cmd: dict, slot_ids: list[str], weights: dict[str, str]) -> str:
    payload = cmd.get("payload", {})
    norm = {
        "node_id": cmd["node_id"],
        "slots": {sid: weights[sid] for sid in slot_ids},
        "payload": payload,
    }
    import hashlib
    import json
    return hashlib.sha256(
        json.dumps(norm, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


def decide_report_checkpoint(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """承运方/口岸回传节点到达。重复报文不重放；重量/货位漂移先挂起。"""
    msg_id = cmd.get("msg_id")
    if not msg_id:
        raise DomainError("口岸/承运报文必须带报文编号 msg_id")
    node = state.node(cmd["node_id"])  # 若非当前版本直接抛错（基于旧版本的迟到报文）
    _authorize_node(actor, node)
    slot_ids = _target_slots(state, cmd)
    if not slot_ids:
        raise OrderError("报文不包含任何有效货位")

    weights = {sid: str(state.slots[sid].weight) for sid in slot_ids}
    fp = _fingerprint(cmd, slot_ids, weights)
    prior = state.messages.get(msg_id)

    # 挂起核对：编号未变但货位构成或过磅重量变化
    measured = cmd.get("measured_weights") or {}
    drift = {
        sid: str(_w(v))
        for sid, v in measured.items()
        if sid in state.slots and _w(v) != state.slots[sid].weight
    }
    if prior is not None:
        if prior["result"].startswith("resolved_"):
            # 该编号曾挂起并经主管裁决关闭：后续同编号一律不受理，避免二次挂起
            return []
        if prior["fingerprint"] != fp or drift:
            if prior["result"] == "suspended":
                return []  # 已有挂起单，等主管裁决；绝不重复推进
            return [_suspension_event(
                state, msg_id, "identity_conflict", slot_ids,
                {"fingerprint": fp, "prior_fingerprint": prior["fingerprint"],
                 "node_id": node["id"], "measured": drift,
                 "note": "同一报文编号但货位/重量发生变化"},
            )]
        # 内容完全一致的重复报文：不产生任何事件（幂等）
        return []
    if drift:
        return [_suspension_event(
            state, msg_id, "weight_mismatch", slot_ids,
            {"fingerprint": fp, "node_id": node["id"],
             "declared": weights, "measured": drift,
             "note": "过磅重量与登记重量不一致，挂起核对"},
        )]

    rv = state.route_version
    idx = state.node_index(node["id"], rv)
    for sid in slot_ids:
        cur = state.node_states.get((sid, rv, node["id"]))
        if cur and cur["status"] == "reached":
            # 同节点重复推进（不同 msg_id 也算重复到达，不重复推进、不重复收钱）
            return []
        # 任何未解除的扣留（在本节点或其之前）都冻结后续一切推进
        held_nodes = [
            n["id"] for n in state.routes[rv]["nodes"]
            if (state.node_states.get((sid, rv, n["id"])) or {}).get("status") == "held"
        ]
        if held_nodes:
            raise OrderError(
                f"货位 {sid} 在 {held_nodes[0]} 处于扣留状态，需先解除异常")
        last = state.last_reached(sid, rv)
        if idx != last + 1:
            raise RoutingError(
                f"货位 {sid} 节点顺序违规：已到第 {last} 个节点，不能跳到第 {idx} 个"
            )

    events: list[dict] = _checkpoint_events(
        state, slot_ids, node, msg_id,
        {"actor": actor.actor_id, "payload": cmd.get("payload", {})},
    )
    events.append(_message_recorded(msg_id, fp, "applied"))
    return events


def _checkpoint_events(state: State, slot_ids: list[str], node: dict,
                       msg_id: str | None, evidence: dict) -> list[dict]:
    """构造节点推进事件及其同事务资金分录（放行保证金 / 交付结转）。"""
    rv = state.route_version
    events: list[dict] = [
        _ev(
            "SlotCheckpointReached",
            order_id=state.order_id,
            slot_ids=slot_ids,
            route_version=rv,
            node_id=node["id"],
            msg_id=msg_id,
            evidence=evidence,
        )
    ]

    # 放行节点：同步冻结/缴纳放行保证金（与节点推进同一事务，份额建单时已定）
    if node["kind"] == "release":
        for sid in slot_ids:
            amount = state.slots[sid].deposit_share
            if amount <= 0:
                continue
            events.append(_money(
                f"dep-{msg_id or 'sus'}-{sid}", "cash", amount, "debit",
                "霍尔果斯放行保证金", {"msg_id": msg_id, "slot_id": sid}))
            events.append(_money(
                f"dep-{msg_id or 'sus'}-{sid}", "deposit_hold", amount, "credit",
                "放行保证金冻结", {"msg_id": msg_id, "slot_id": sid}))

    # 最终交付节点：确认收入、退还保证金、标记交付
    if node["kind"] == "deliver":
        for sid in slot_ids:
            amount = state.slots[sid].fee_share
            events.append(_money(
                f"fee-{msg_id or 'sus'}-{sid}", "fee_hold", amount, "debit",
                "交付完成，预占费用结转收入", {"slot_id": sid}))
            events.append(_money(
                f"fee-{msg_id or 'sus'}-{sid}", "fee_income", amount, "credit",
                "运费收入确认", {"slot_id": sid}))
            dep = state.slots[sid].deposit_share
            if dep > 0:
                events.append(_money(
                    f"dep-ret-{msg_id or 'sus'}-{sid}", "deposit_hold", dep, "debit",
                    "交付完成，放行保证金释放", {"slot_id": sid}))
                events.append(_money(
                    f"dep-ret-{msg_id or 'sus'}-{sid}", "cash", dep, "credit",
                    "保证金退还客户", {"slot_id": sid}))
        events.append(_ev("SlotDelivered", order_id=state.order_id, slot_ids=slot_ids))
        if all(state.slots[s].status == "delivered" or s in slot_ids
               for s in state.slots):
            events.append(_ev("OrderCompleted", order_id=state.order_id))
    return events


def _message_recorded(msg_id: str, fingerprint: str, result: str,
                      suspension_id: str | None = None) -> dict:
    return _ev("MessageRecorded", order_id=None, msg_id=msg_id,
               fingerprint=fingerprint, result=result,
               suspension_id=suspension_id)


def _suspension_event(state: State, msg_id: str, kind: str, slot_ids: list[str],
                      details: dict) -> dict:
    sus_id = f"SUS-{msg_id}"
    return _ev(
        "MessageSuspended",
        order_id=state.order_id,
        suspension_id=sus_id,
        msg_id=msg_id,
        kind=kind,
        slot_ids=slot_ids,
        details=details,
    )


def decide_report_exception(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """口岸登记异常扣留（如查验扣货）。扣留期间下游节点一律冻结。"""
    if actor.role not in (Role.CUSTOMS, Role.SUPERVISOR):
        raise AuthError("只有口岸人员或值班主管可以登记异常")
    msg_id = cmd.get("msg_id")
    if not msg_id:
        raise DomainError("异常报文必须带 msg_id")
    if msg_id in state.messages:
        return []  # 重复异常报文
    node = state.node(cmd["node_id"])
    slot_ids = _target_slots(state, cmd)
    rv = state.route_version
    idx = state.node_index(node["id"], rv)
    for sid in slot_ids:
        key = (sid, rv, node["id"])
        if key in state.node_states:
            return []  # 该节点已有状态，重复登记忽略
        if state.last_reached(sid, rv) != idx - 1:
            raise RoutingError(f"货位 {sid} 尚未到达 {node['id']}，不能在该处登记扣留")
    return [
        _ev(
            "SlotExceptionOpened",
            order_id=state.order_id,
            exception_id=cmd["exception_id"],
            slot_ids=slot_ids,
            route_version=rv,
            node_id=node["id"],
            reason=cmd["reason"],
            msg_id=msg_id,
        ),
        _message_recorded(msg_id, f"exc-{msg_id}", "exception"),
    ]


def decide_resolve_exception(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """主管解除扣留：节点放行，异常赔付同事务入账（借贷相等）。"""
    _require_roles(actor, {Role.SUPERVISOR})
    exc = state.exceptions.get(cmd["exception_id"])
    if exc is None:
        raise OrderError(f"未知异常单 {cmd['exception_id']}")
    if exc["status"] != "open":
        return []  # 重复解除
    claim = int(cmd.get("claim_amount", 0))
    events: list[dict] = [
        _ev(
            "SlotExceptionResolved",
            order_id=state.order_id,
            exception_id=exc["exception_id"],
            note=cmd.get("note", ""),
            claim_amount=claim,
        )
    ]
    if claim:
        events.append(_money(
            f"clm-{exc['exception_id']}", "claim_expense", claim, "debit",
            f"异常赔付：{exc['reason']}",
            {"exception_id": exc["exception_id"], "node_id": exc["node_id"]}))
        events.append(_money(
            f"clm-{exc['exception_id']}", "claim_payable", claim, "credit",
            "应付客户赔款", {"exception_id": exc["exception_id"]}))
    return events


# ----------------------------------------------------------------- 提案审批


def decide_propose(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """货代/承运方发起改线或拆并批提案；主管审批后才生效。"""
    _require_roles(actor, {Role.FORWARDER, Role.CARRIER, Role.SUPERVISOR})
    if state.completed:
        raise OrderError("订单已完成交付，不能再改线或拆并批次")
    kind = cmd["kind"]
    if kind not in ("reroute", "split", "merge"):
        raise ProposalError(f"未知提案类型 {kind}")
    if kind == "reroute":
        new_route = dict(cmd["route"])
        new_route["version"] = state.route_version + 1
        _validate_route(new_route)
        payload = {"route": new_route, "carry_to_index": int(cmd.get("carry_to_index", 0))}
        carry = payload["carry_to_index"]
        if not (0 <= carry <= len(state.route()["nodes"])):
            raise ProposalError("carry_to_index 超出当前路线范围")
    elif kind == "split":
        payload = _validate_split(state, cmd["payload"])
    else:
        payload = _validate_merge(state, cmd["payload"])
    return [
        _ev(
            "ProposalOpened",
            order_id=state.order_id,
            proposal_id=cmd["proposal_id"],
            kind=kind,
            payload=payload,
            reason=cmd.get("reason", ""),
            proposer=actor.actor_id,
        )
    ]


def _validate_split(state: State, p: dict) -> dict:
    parent = state.batches.get(p["parent_batch"])
    if parent is None or not parent.active:
        raise ProposalError("待拆分批次不存在或已失效")
    children = p["children"]
    if len(children) < 2:
        raise ProposalError("拆批至少产生两个子批次")
    alloc: list[str] = []
    for c in children:
        if not c["slot_ids"]:
            raise ProposalError("子批次不能为空")
        alloc.extend(c["slot_ids"])
    if sorted(alloc) != sorted(parent.slots):
        raise ProposalError("拆批必须不重不漏覆盖父批次全部货位")
    return {"parent_batch": parent.batch_id,
            "children": [{"batch_id": c["batch_id"],
                          "slot_ids": c["slot_ids"],
                          "carrier_id": c.get("carrier_id")} for c in children]}


def _validate_merge(state: State, p: dict) -> dict:
    ids = p["batches"]
    if len(ids) < 2:
        raise ProposalError("合批至少需要两个批次")
    positions = set()
    seen_batches: set[str] = set()
    for bid in ids:
        if bid in seen_batches:
            raise ProposalError(f"批次 {bid} 在合批名单中重复")
        seen_batches.add(bid)
        b = state.batches.get(bid)
        if b is None or not b.active:
            raise ProposalError(f"批次 {bid} 不存在或已失效")
        for sid in b.slots:
            for (s, _rv, _n), st in state.node_states.items():
                if s == sid and st["status"] == "held":
                    raise ProposalError(f"批次 {bid} 存在扣留货位 {sid}，不能合批")
            positions.add(state.last_reached(sid))
    if len(positions) != 1:
        raise ProposalError("只能合并当前节点位置相同的批次（同地分拨合并）")
    result_id = p["result_batch_id"]
    if result_id in state.batches:
        raise ProposalError(f"结果批次编号 {result_id} 已存在")
    return {"batches": list(ids), "result_batch_id": result_id,
            "carrier_id": p.get("carrier_id")}


def decide_review_proposal(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """只有值班主管能批准/驳回；批准时同事务让改线/拆并立即生效。"""
    _require_roles(actor, {Role.SUPERVISOR})
    proposal = state.proposals.get(cmd["proposal_id"])
    if proposal is None:
        raise ProposalError(f"未知提案 {cmd['proposal_id']}")
    if proposal.status != "open":
        raise ProposalError("提案已裁决，不能重复审批")
    decision = cmd["decision"]  # approved / rejected
    if decision not in ("approved", "rejected"):
        raise ProposalError("decision 必须是 approved 或 rejected")

    events: list[dict] = [
        _ev("Proposal" + decision.capitalize(),
            order_id=state.order_id, proposal_id=proposal.proposal_id,
            comment=cmd.get("comment", ""))
    ]
    if decision == "rejected":
        return events

    if proposal.kind == "reroute":
        route = proposal.payload["route"]
        events.append(_ev(
            "RouteActivated",
            order_id=state.order_id,
            from_version=state.route_version,
            route=route,
            carry_to_index=proposal.payload["carry_to_index"],
            proposal_id=proposal.proposal_id,
        ))
    elif proposal.kind == "split":
        events.append(_ev(
            "BatchSplit",
            order_id=state.order_id,
            parent_batch=proposal.payload["parent_batch"],
            children=proposal.payload["children"],
            proposal_id=proposal.proposal_id,
        ))
    else:
        events.append(_ev(
            "BatchMerged",
            order_id=state.order_id,
            batches=proposal.payload["batches"],
            result_batch_id=proposal.payload["result_batch_id"],
            carrier_id=proposal.payload.get("carrier_id"),
            proposal_id=proposal.proposal_id,
        ))
    return events


def decide_resolve_suspension(state: State, actor: Actor, cmd: dict) -> list[dict]:
    """主管裁决挂起报文：confirmed 可修正重量并补推进；discarded 作废。"""
    _require_roles(actor, {Role.SUPERVISOR})
    sus = state.suspensions.get(cmd["suspension_id"])
    if sus is None:
        raise OrderError(f"未知挂起单 {cmd['suspension_id']}")
    if sus.status != "pending":
        return []
    action = cmd["action"]  # confirmed / discarded
    if action not in ("confirmed", "discarded"):
        raise ProposalError("action 必须是 confirmed 或 discarded")

    events: list[dict] = [
        _ev("SuspensionResolved", order_id=state.order_id,
            suspension_id=sus.suspension_id, action=action,
            note=cmd.get("note", "")),
        # 关闭该报文编号：裁决结论进入幂等表，后续同编号报文一律忽略
        _message_recorded(sus.msg_id,
                          sus.details.get("fingerprint", sus.msg_id),
                          f"resolved_{action}", sus.suspension_id),
    ]
    if action == "confirmed":
        corrected = cmd.get("corrected_weights") or {}
        weights = {sid: str(_w(w)) for sid, w in corrected.items() if sid in state.slots}
        if weights:
            events.append(_ev("SlotWeightAdjusted",
                              order_id=state.order_id, weights=weights,
                              suspension_id=sus.suspension_id))
        if cmd.get("advance_node"):
            node_id = sus.details["node_id"]
            node = state.node(node_id)
            rv = state.route_version
            idx = state.node_index(node_id)
            target = [s for s in sus.slot_ids if state.last_reached(s, rv) == idx - 1]
            if target:
                events.extend(_checkpoint_events(
                    state, target, node, None,
                    {"suspension_id": sus.suspension_id,
                     "approved_by": actor.actor_id,
                     "note": "挂起核对后主管确认补推进"},
                ))
    return events
