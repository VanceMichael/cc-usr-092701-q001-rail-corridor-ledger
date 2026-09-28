"""只追加（append-only）的事件存储，带崩溃恢复。

磁盘布局是 JSONL，一次业务事务按下述顺序落盘：

    {"tx": 7, "stage": "begin", "event_count": 2, "ts": ...}
    {"tx": 7, "seq": 13, "event": { ... }}
    {"tx": 7, "seq": 14, "event": { ... }}
    {"tx": 7, "stage": "commit"}

进程在写入一半退出时，最后一个事务没有 ``commit`` 行。打开存储时
:meth:`EventStore.recover` 用 begin/commit 状态机识别它、把未提交的半截
事务整段截掉，于是：

- 半截事务里的费用/节点事件整体不生效，不会"多扣"；
- 已提交事务的 ``seq`` 严格连续，回放时不会"跳过检查点"。

事件序号 ``seq`` 在全存储范围内单调递增且不空洞，审计按 seq 即可定位
任意时间点。
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Iterator


class CorruptLogError(RuntimeError):
    """日志无法按 begin/事件/commit 协议解释。"""


def _now() -> float:
    return time.time()


class EventStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._tx_seq = 0
        self._next_seq = 1
        self.recover()

    # ------------------------------------------------------------------
    # 恢复与回放
    # ------------------------------------------------------------------

    def recover(self) -> int:
        """重放日志并截断未提交的尾部，返回截掉的未提交事件数。"""
        with self._lock:
            if not self.path.exists():
                self.path.touch()
                self._next_seq = 1
                return 0

            raw = self.path.read_bytes()
            lines = raw.splitlines(keepends=True)
            committed_end = 0       # 最后一个 commit 行之后的字节偏移
            offset = 0
            in_tx = False
            expected_seq = 1
            dangling_events = 0
            max_tx = 0
            torn_tail = False

            for idx, raw_line in enumerate(lines):
                line_end = offset + len(raw_line)
                try:
                    record = json.loads(raw_line.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # 崩溃在半行写入上：只有当它位于文件尾部时才能截断
                    # （未提交事务的半截事件，或写坏的半截 begin/commit 行）；
                    # 已提交区域中间的损坏必须报错，避免静默丢掉数据。
                    if idx == len(lines) - 1 and (in_tx or committed_end == offset):
                        torn_tail = True
                        break
                    raise CorruptLogError(f"偏移 {offset} 处日志行损坏") from None
                stage = record.get("stage")
                if stage == "begin":
                    if in_tx:
                        raise CorruptLogError(
                            f"偏移 {offset} 处出现嵌套 begin")
                    in_tx = True
                    dangling_events = 0
                    max_tx = max(max_tx, int(record.get("tx") or 0))
                elif stage == "commit":
                    if not in_tx:
                        raise CorruptLogError(
                            f"偏移 {offset} 处出现无 begin 的 commit")
                    in_tx = False
                    committed_end = line_end
                    dangling_events = 0
                    max_tx = max(max_tx, int(record.get("tx") or 0))
                elif "event" in record:
                    if not in_tx:
                        raise CorruptLogError(
                            f"偏移 {offset} 的事件不在任何事务内")
                    if record.get("seq") != expected_seq:
                        raise CorruptLogError(
                            f"事件序号不连续：{record.get('seq')}，"
                            f"期望 {expected_seq}")
                    expected_seq += 1
                    if in_tx:
                        dangling_events += 1
                else:
                    raise CorruptLogError(f"无法识别的日志行：{record}")
                offset = line_end

            if in_tx or torn_tail:
                # 尾部事务缺少 commit（或写坏了半截）：整段回滚。
                expected_seq -= dangling_events
                with self.path.open("r+b") as fh:
                    fh.truncate(committed_end)
                os.sync()
                self._next_seq = expected_seq
                self._tx_seq = max_tx
                return dangling_events

            self._next_seq = expected_seq
            self._tx_seq = max_tx
            return 0

    def events(self) -> Iterator[dict[str, Any]]:
        """按 seq 顺序产出所有已提交事件。"""
        with self._lock:
            with self.path.open("r", encoding="utf-8") as fh:
                for raw in fh:
                    record = json.loads(raw)
                    if "event" in record:
                        yield record["event"]

    def committed_count(self) -> int:
        return sum(1 for _ in self.events())

    # ------------------------------------------------------------------
    # 原子追加
    # ------------------------------------------------------------------

    def append(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """把一批事件作为单个事务提交；要么全部可见，要么全部不可见。

        返回带全局 ``seq`` 与服务端时间戳的事件副本。调用方可以用这些
        seq 做审计定位（同一事务的事件共享 ``tx``）。
        """
        if not events:
            return []
        with self._lock:
            self._tx_seq += 1
            tx_id = self._tx_seq
            stamped: list[dict[str, Any]] = []
            for event in events:
                event = dict(event)
                event["seq"] = self._next_seq
                event["tx"] = tx_id
                event.setdefault("ts", _now())
                self._next_seq += 1
                stamped.append(event)

            # begin 与事件先落盘并 fsync，然后才写 commit，
            # 使"有无 commit 行"成为明确的提交判定点。
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(
                    {"tx": tx_id, "stage": "begin",
                     "event_count": len(stamped), "ts": _now()},
                    ensure_ascii=False) + "\n")
                for event in stamped:
                    fh.write(json.dumps(
                        {"tx": tx_id, "seq": event["seq"], "event": event},
                        ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
                fh.write(json.dumps({"tx": tx_id, "stage": "commit"}) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            return stamped
