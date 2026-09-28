import json
import os
import tempfile
import unittest

from src.rail_corridor_ledger.events import CorruptLogError, CrashError, EventStore


class EventStoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "wal")

    def test_append_and_reopen(self):
        store = EventStore(self.path)
        evs = store.append([{"type": "A", "data": {"x": 1}},
                            {"type": "B", "data": {}}], ts="2026-09-01T00:00:00+00:00")
        self.assertEqual([e["seq"] for e in evs], [1, 2])
        self.assertTrue(all(e["tx"] == evs[0]["tx"] for e in evs))
        reopened = EventStore(self.path)
        self.assertEqual(len(reopened), 2)
        self.assertEqual([e["seq"] for e in reopened.read()], [1, 2])

    def test_seq_continuous_across_tx(self):
        store = EventStore(self.path)
        store.append([{"type": "A"}], ts="t1")
        store.append([{"type": "B"}, {"type": "C"}], ts="t2")
        self.assertEqual([e["seq"] for e in EventStore(self.path).read()], [1, 2, 3])

    def test_crash_stages_rollback_or_commit(self):
        for stage, committed in [
            ("after_begin", 0),
            ("after_event", 0),
            ("partial_tail", 0),
            ("after_commit", 2),
        ]:
            with self.subTest(stage=stage):
                store = EventStore(self.path + "-" + stage)
                store.append([{"type": "Seed"}], ts="t0")
                with self.assertRaises(CrashError):
                    store.append([{"type": "X"}, {"type": "Y"}],
                                 ts="t1", crash=stage)
                # 同进程恢复（模拟重启）后，未提交尾部已被截断
                store._recover()
                self.assertEqual(len(store), 1 + committed)
                again = EventStore(self.path + "-" + stage)
                self.assertEqual(len(again), 1 + committed)

    def test_retry_after_crash_does_not_duplicate_or_skip(self):
        store = EventStore(self.path)
        store.append([{"type": "Seed"}], ts="t0")
        with self.assertRaises(CrashError):
            store.append([{"type": "X"}], ts="t1", crash="after_event")
        store2 = EventStore(self.path)
        evs = store2.append([{"type": "X"}], ts="t1")
        self.assertEqual(evs[0]["seq"], 2)  # 不跳号
        self.assertEqual([e["seq"] for e in EventStore(self.path).read()], [1, 2])

    def test_torn_tail_truncated(self):
        store = EventStore(self.path)
        store.append([{"type": "Seed"}], ts="t0")
        with open(self.path, "ab") as fh:
            fh.write(b'{"t":"B","x":"abc"}\n{"t":"E",')  # 写到一半，无换行
        reopened = EventStore(self.path)
        self.assertEqual([e["type"] for e in reopened.read()], ["Seed"])
        # 截断后可正常追加
        reopened.append([{"type": "Next"}], ts="t2")
        self.assertEqual([e["seq"] for e in EventStore(self.path).read()], [1, 2])

    def test_corruption_in_middle_rejected(self):
        store = EventStore(self.path)
        store.append([{"type": "Seed"}], ts="t0")
        with open(self.path, "rb") as fh:
            raw = fh.read()
        with open(self.path, "wb") as fh:
            fh.write(raw + b"not-a-json-line\n")  # 中段（有换行）损坏必须报错
        with self.assertRaises(CorruptLogError):
            EventStore(self.path)

    def test_records_are_json_lines(self):
        store = EventStore(self.path)
        store.append([{"type": "A"}], ts="t0")
        with open(self.path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        self.assertEqual([json.loads(x)["t"] for x in lines], ["B", "E", "C"])


if __name__ == "__main__":
    unittest.main()
