"""货运履约服务门面。

职责：
- 命令 → model.decide_* 纯决策 → 事件信封 → EventStore 单事务追加；
- 提交前把“已提交事件 + 本批候选事件”整体重放一遍（含资金守恒/余额/序号校验），
  任何不变量不满足则整批拒绝，磁盘一字节不写；
- 命令号 cmd_id 幂等：调用方超时重试同一命令不会重复执行（重开后仍可识别）；
- 崩溃恢复由 EventStore 负责：未提交事务在重启时被截断，已提交事务不丢。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import model
from .events import EventStore
from .model import Actor, DomainError, Role
from .views import audit_timeline, customer_view, operations_view, state_at

CommandHandler = Callable


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class FulfillmentService:
    def __init__(self, path: str | Path, clock: Callable[[], str] = _utcnow):
        self.store = EventStore(path)
        self.clock = clock

    # ------------------------------------------------------------------ 内部
    def _state(self):
        return model.rebuild(self.store.read())

    def _handled_cmd_ids(self) -> dict[str, list[int]]:
        """已处理命令号 → 产生的事件 seq（用于重试幂等，含重启后）。"""
        out: dict[str, list[int]] = {}
        for env in self.store.read():
            cid = env.get("cmd_id")
            if cid:
                out.setdefault(cid, []).append(env["seq"])
        return out

    def _submit(self, actor: Actor, cmd: dict, events: list[dict],
                crash: str | None = None) -> list[dict]:
        if not events:
            return []
        ts = self.clock()
        cmd_id = cmd.get("cmd_id")
        stamped = [
            {**ev, "actor": actor.actor_id, "cmd_id": cmd_id}
            for ev in events
        ]
        # 先分配临时序号整体重放校验：任何不变量失败都不落盘。
        base = len(self.store)
        tentative = [
            {"seq": base + i + 1, "tx": "__trial__", "ts": ts,
             "type": ev["type"], "data": ev.get("data", {}),
             "actor": actor.actor_id, "cmd_id": cmd_id}
            for i, ev in enumerate(stamped)
        ]
        model.rebuild(self.store.read() + tentative)  # 抛错即拒绝
        return self.store.append(stamped, ts=ts, crash=crash)

    # ------------------------------------------------------------------ 命令
    def place_order(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_place_order, **kw)

    def report_checkpoint(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_report_checkpoint, **kw)

    def report_exception(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_report_exception, **kw)

    def resolve_exception(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_resolve_exception, **kw)

    def propose(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_propose, **kw)

    def review_proposal(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_review_proposal, **kw)

    def resolve_suspension(self, actor: Actor, cmd: dict, **kw) -> list[dict]:
        return self._run(actor, cmd, model.decide_resolve_suspension, **kw)

    def _run(self, actor: Actor, cmd: dict, handler: CommandHandler,
             crash: str | None = None) -> list[dict]:
        cmd_id = cmd.get("cmd_id")
        if cmd_id is not None:
            done = self._handled_cmd_ids()
            if cmd_id in done:
                # 命令号幂等：重试只返回首次结果的定位，不重新执行。
                return [env for env in self.store.read() if env.get("cmd_id") == cmd_id]
        state = self._state()
        events = handler(state, actor, cmd)
        return self._submit(actor, cmd, events, crash=crash)

    # ------------------------------------------------------------------ 视图
    def operations_view(self) -> dict:
        return operations_view(self._state())

    def customer_view(self) -> dict:
        return customer_view(self._state())

    def view_for(self, actor: Actor) -> dict:
        if actor.role == Role.CUSTOMER:
            return self.customer_view()
        return self.operations_view()

    def audit_at(self, *, ts: str | None = None, seq: int | None = None) -> dict:
        state = state_at(self.store.read(), ts=ts, seq=seq)
        return operations_view(state)

    def timeline(self) -> list[dict]:
        return audit_timeline(self.store.read())

    def raw_events(self) -> list[dict]:
        return self.store.read()
