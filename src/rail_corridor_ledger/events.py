"""只追加的 WAL 事件存储。

磁盘布局（每行一个 JSON 记录，事务成组提交）：

    {"t":"B","x":<事务号>,"n":<事件数>}
    {"t":"E","x":<事务号>,"i":0,"e":<事件信封>}
    ...
    {"t":"C","x":<事务号>}

只有读到 COMMIT 的事务才会生效；末尾残缺行（写入一半退出）与
未提交事务在打开时一并截断，保证：
- 节点事件和资金分录同事务 → 要么都可见，要么都不可见；
- 已提交事件带全局连续 seq，恢复后不重不漏。
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from pathlib import Path
from typing import Callable, Iterable


class CrashError(RuntimeError):
    """测试注入的“进程在写入一半退出”。"""


class CorruptLogError(RuntimeError):
    """日志中段出现无法解释的损坏（末尾撕裂不属于此列）。"""


_CRASH_STAGES = frozenset(
    {"after_begin", "after_event", "partial_tail", "after_commit"}
)


class EventStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._events: list[dict] = []
        self._committed_bytes = 0
        self._recover()

    # ------------------------------------------------------------------ 恢复
    def _recover(self) -> None:
        if not self.path.exists():
            self.path.touch()
            return
        raw = self.path.read_bytes()
        chunks = raw.splitlines(keepends=True)
        committed: list[dict] = []
        good_end = 0
        pending: tuple[str, list[dict]] | None = None
        consumed = 0
        for chunk in chunks:
            complete = chunk.endswith(b"\n")
            line = chunk[:-1] if complete else chunk
            consumed += len(chunk)
            if not line.strip():
                # 空行只允许出现在末尾（撕裂的空白），中段空行视为损坏。
                if consumed != len(raw):
                    raise CorruptLogError("日志中段存在空行")
                break
            try:
                rec = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                # 末行无换行结尾：写入一半退出，丢弃整个未提交尾部；
                # 以换行结尾却无法解析的完整记录属于损坏，必须拒绝。
                if complete or consumed != len(raw):
                    raise CorruptLogError("日志中段存在无法解析的完整记录")
                break
            kind = rec.get("t")
            txid = rec.get("x")
            if kind == "B":
                if pending is not None:
                    raise CorruptLogError("未结束的事务又出现 BEGIN")
                pending = (txid, [])
            elif kind == "E":
                if pending is None or pending[0] != txid:
                    raise CorruptLogError("事件缺少匹配的 BEGIN")
                env = rec.get("e")
                if not isinstance(env, dict) or env.get("seq") != len(pending[1]) + len(committed) + 1:
                    raise CorruptLogError("事件序号不连续")
                pending[1].append(env)
            elif kind == "C":
                if pending is None or pending[0] != txid:
                    raise CorruptLogError("COMMIT 缺少匹配的 BEGIN")
                committed.extend(pending[1])
                pending = None
                good_end = consumed
            else:
                raise CorruptLogError(f"未知记录类型：{kind!r}")
        # 未提交或撕裂的尾部物理截断，后续追加从干净位置开始。
        if good_end != len(raw):
            with self.path.open("ab") as fh:
                fh.truncate(good_end)
                fh.flush()
                os.fsync(fh.fileno())
        self._events = committed
        self._committed_bytes = good_end

    # ------------------------------------------------------------------ 读取
    def read(self) -> list[dict]:
        with self._lock:
            return list(self._events)

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    # ------------------------------------------------------------------ 写入
    def append(
        self,
        events: Iterable[dict],
        *,
        ts: str,
        crash: str | None = None,
    ) -> list[dict]:
        """原子追加一批事件信封（type/data 已就绪，seq 在此分配）。

        crash 用于演练半途退出：
        - after_begin：BEGIN 落盘后退出；
        - after_event：首条 EVENT 落盘后退出；
        - partial_tail：用半行残片替代 COMMIT 后退出；
        - after_commit：COMMIT 已落盘（数据耐久）但调用方看到异常。
        """
        if crash is not None and crash not in _CRASH_STAGES:
            raise ValueError(f"未知崩溃注入点：{crash}")
        events = list(events)
        if not events:
            return []
        with self._lock:
            try:
                return self._append_locked(events, ts, crash)
            except CrashError:
                # 模拟进程重启：按磁盘内容重新恢复，未提交尾部被截断丢弃。
                self._recover()
                raise

    def _append_locked(self, events: list[dict], ts: str,
                       crash: str | None) -> list[dict]:
        with self.path.open("ab") as fh:
            txid = uuid.uuid4().hex
            base = len(self._events)
            envelopes = []
            for i, ev in enumerate(events):
                envelopes.append(
                    {
                        "seq": base + i + 1,
                        "tx": txid,
                        "ts": ts,
                        "type": ev["type"],
                        "data": ev.get("data", {}),
                        "actor": ev.get("actor"),
                        "cmd_id": ev.get("cmd_id"),
                    }
                )

            def _write(obj: dict) -> None:
                fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                fh.write(b"\n")
                fh.flush()
                os.fsync(fh.fileno())

            _write({"t": "B", "x": txid, "n": len(envelopes)})
            if crash == "after_begin":
                raise CrashError("BEGIN 后进程退出")
            for i, env in enumerate(envelopes):
                _write({"t": "E", "x": txid, "i": i, "e": env})
                if crash == "after_event" and i == 0:
                    raise CrashError("首条事件后进程退出")
            if crash == "partial_tail":
                fh.write(b'{"t":"C","x":"' + txid[:8].encode())  # 半行，无换行
                fh.flush()
                os.fsync(fh.fileno())
                raise CrashError("COMMIT 写到一半进程退出")
            _write({"t": "C", "x": txid})
            self._events.extend(envelopes)
            self._committed_bytes = fh.tell()
            if crash == "after_commit":
                raise CrashError("提交完成但调用方未收到回执")
            return envelopes
