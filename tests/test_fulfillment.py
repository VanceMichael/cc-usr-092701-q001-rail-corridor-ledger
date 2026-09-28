"""货运履约服务的端到端测试。

覆盖需求里的硬约束：
- 口岸消息幂等、同编号货位/重量变化挂起核对；
- 承运方区段权限、跳点拒绝、改线/拆并主管审批；
- 费用预占/保证金/赔付与节点同事务、结算只能一次；
- 写一半崩溃后恢复：不多扣、不跳过检查点；
- 客户脱敏视图与审计任意时间点还原。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.rail_corridor_ledger.model import Role, Route, Slot
from src.rail_corridor_ledger.service import (
    DEFAULT_DEPOSIT_CENTS,
    Actor,
    FulfillmentService,
    ServiceError,
)
from src.rail_corridor_ledger.store import CorruptLogError, EventStore
from src.rail_corridor_ledger.views import (
    audit_snapshot,
    customer_view,
    order_tracking,
    replay,
)

FORWARDER = Actor("u-fwd", Role.FORWARDER, "李运营")
CARRIER = Actor("u-car", Role.CARRIER, "王承运")
CARRIER2 = Actor("u-car2", Role.CARRIER, "境外承运方")
PORT = Actor("u-port", Role.PORT, "霍尔果斯口岸")
SUPERVISOR = Actor("u-sup", Role.SUPERVISOR, "赵值班")
CUSTOMER = Actor("u-cus", Role.CUSTOMER, "客户")

SLOTS = [
    Slot("S-1", "CCLU-1001", 18000.0, "机电设备"),
    Slot("S-2", "CCLU-1002", 9500.0, "汽配零件"),
]


class ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = EventStore(Path(self._tmp.name) / "ledger.jsonl")
        self.svc = FulfillmentService(self.store)
        self.svc.open_order(FORWARDER, "ORD-1", "比什凯克客户甲",
                            freight_cents=500_000)
        self.svc.create_batch(FORWARDER, "ORD-1", "B-1", SLOTS,
                              basis="装箱计划单 PL-001")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def state(self):
        return replay(self.store.events())

    def batch(self, no="B-1"):
        return self.state().order("ORD-1").batches[no]

    # ------------------------------------------------------------------
    # 正常履约 + 费用同事务
    # ------------------------------------------------------------------

    def test_happy_path_and_fee_ledger(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded",
                        basis="南昌装箱回执",
                        observed_slots=[{"slot_no": "S-1", "container_no": "CCLU-1001",
                                         "weight_kg": 18000.0},
                                        {"slot_no": "S-2", "container_no": "CCLU-1002",
                                         "weight_kg": 9500.0}])
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed",
                        basis="南昌西发车")
        self.svc.ingest(PORT, "ORD-1", "M-ARR", "B-1", "arrived_port",
                        basis="抵达霍尔果斯", eta_range={"earliest": "2026-10-02",
                                                        "latest": "2026-10-03"})
        self.svc.ingest(PORT, "ORD-1", "M-TRANS", "B-1", "transship", basis="准轨换宽轨")
        self.svc.ingest(PORT, "ORD-1", "M-INSP", "B-1", "inspection", basis="海关查验")
        self.svc.ingest(PORT, "ORD-1", "M-REL", "B-1", "released", basis="放行出境")

        self.assertEqual(self.batch().node, "released")

        fees = order_tracking(self.state().order("ORD-1"))["fees"]
        self.assertEqual(fees["held"], 500_000 + DEFAULT_DEPOSIT_CENTS)
        self.assertEqual(fees["settled"], 0)

        # 放行节点与保证金必须在同一个事务里落盘。
        wrappers = [json.loads(line) for line in self.store.path.read_text().splitlines()]
        rel_tx = next(w["tx"] for w in wrappers
                      if w.get("event", {}).get("type") == "node_advanced"
                      and w["event"]["node"] == "released")
        same_tx = [w["event"]["type"] for w in wrappers
                   if w.get("tx") == rel_tx and "event" in w]
        self.assertIn("fee_booked", same_tx)
        self.assertTrue(any(w.get("stage") == "commit" and w["tx"] == rel_tx
                            for w in wrappers))

    # ------------------------------------------------------------------
    # 幂等
    # ------------------------------------------------------------------

    def test_duplicate_message_is_idempotent(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded",
                        basis="南昌装箱回执")
        before = self.store.committed_count()
        again = self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded",
                                basis="重复回传")
        self.assertEqual(again, [])                      # 不产生任何事件
        self.assertEqual(self.store.committed_count(), before)
        nodes = [h["node"] for h in self.batch().history]
        self.assertEqual(nodes.count("loaded"), 1)       # 节点只推进一次

        # 扣留消息重复同样不推进、不重复挂起。
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        self.svc.detain(PORT, "ORD-1", "B-1", "查验异常待处理",
                        message_id="M-HOLD")
        again_detain = self.svc.detain(PORT, "ORD-1", "B-1", "查验异常待处理",
                                       message_id="M-HOLD")
        self.assertEqual(again_detain, [])
        self.assertEqual(len(self.state().order("ORD-1").holds), 1)

    # ------------------------------------------------------------------
    # 同编号货位/重量变化 → 挂起核对，节点不动
    # ------------------------------------------------------------------

    def test_slot_weight_change_suspends_node(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        self.svc.ingest(PORT, "ORD-1", "M-ARR", "B-1", "arrived_port",
                        basis="抵岸",
                        observed_slots=[{"slot_no": "S-2", "container_no": "CCLU-1002",
                                         "weight_kg": 9800.0}])  # 9500 → 9800
        self.assertEqual(self.batch().node, "departed")          # 没有推进
        hold = next(h for h in self.state().order("ORD-1").holds.values())
        self.assertIsNone(hold["resolved_seq"])
        self.assertIn("毛重变化", hold["reason"])

        # 挂起未解除前，任何新消息都不能推进。
        with self.assertRaisesRegex(ServiceError, "挂起"):
            self.svc.ingest(PORT, "ORD-1", "M-ARR2", "B-1", "arrived_port",
                            basis="补传")

        # 货代核对（复称无误/已更正）后解除，节点恢复到下一个检查点。
        self.svc.resume_after_check(
            FORWARDER, "ORD-1", hold["hold_no"],
            resolution="口岸复称 9500kg，原报文称重误差",
            resume_node="arrived_port", basis="复称确认单 W-9")
        self.assertEqual(self.batch().node, "arrived_port")
        self.svc.ingest(PORT, "ORD-1", "M-TRANS", "B-1", "transship", basis="换装")

    def test_container_number_change_suspends(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        self.svc.ingest(PORT, "ORD-1", "M-ARR", "B-1", "arrived_port",
                        basis="抵岸",
                        observed_slots=[{"slot_no": "S-1",
                                         "container_no": "CCLU-9999",
                                         "weight_kg": 18000.0}])
        hold = next(iter(self.state().order("ORD-1").holds.values()))
        self.assertEqual(self.batch().node, "departed")
        self.assertIn("箱号变化", hold["reason"])

    # ------------------------------------------------------------------
    # 区段权限与节点顺序
    # ------------------------------------------------------------------

    def test_carrier_segment_restriction(self):
        # 承运方不能写口岸节点
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.ingest(CARRIER, "ORD-1", "M-X", "B-1", "arrived_port",
                            basis="越权")
        # 口岸不能写境内承运节点
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.ingest(PORT, "ORD-1", "M-Y", "B-1", "loaded", basis="越权")
        # 客户只读，连消息都不能回传
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.ingest(CUSTOMER, "ORD-1", "M-Z", "B-1", "loaded", basis="越权")

    def test_no_node_skip_or_rollback(self):
        with self.assertRaisesRegex(ServiceError, "下一个检查点"):
            self.svc.ingest(CARRIER, "ORD-1", "M-SKIP", "B-1", "departed",
                            basis="跳过装箱")

    # ------------------------------------------------------------------
    # 异常扣留 + 赔付
    # ------------------------------------------------------------------

    def test_detention_and_compensation(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        self.svc.ingest(PORT, "ORD-1", "M-ARR", "B-1", "arrived_port", basis="抵岸")
        self.svc.ingest(PORT, "ORD-1", "M-TRANS", "B-1", "transship", basis="换装")
        self.svc.ingest(PORT, "ORD-1", "M-INSP", "B-1", "inspection", basis="查验")
        self.svc.detain(PORT, "ORD-1", "B-1", "单证与箱单不符，滞留口岸",
                        message_id="M-DET")
        self.assertTrue(self.batch().detained)

        # 赔付必须主管/货代登记，承运方不行。
        hold_no = next(iter(self.state().order("ORD-1").holds))
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.book_compensation(CARRIER, "ORD-1", hold_no, 1000, "x")
        self.svc.book_compensation(SUPERVISOR, "ORD-1", hold_no, 300_00,
                                   basis="滞留 2 天赔付协议 CMP-7")
        self.assertFalse(self.batch().detained)
        fees = order_tracking(self.state().order("ORD-1"))["fees"]
        self.assertEqual(fees["compensation"], 300_00)

        # 挂起关闭后才能继续走向放行。
        self.svc.ingest(PORT, "ORD-1", "M-REL", "B-1", "released", basis="补单放行")
        self.assertEqual(self.batch().node, "released")

    # ------------------------------------------------------------------
    # 改线：提案 + 主管批准，版本可审计
    # ------------------------------------------------------------------

    def test_route_change_requires_supervisor(self):
        new_nodes = ["booked", "loaded", "departed", "arrived_port",
                     "transship", "inspection", "released", "departed_port",
                     "abroad_transit", "arrived_almaty", "arrived_bishkek",
                     "split_dispatch", "delivered"]
        proposal = self.svc.propose_route_change(
            CARRIER, "ORD-1", new_nodes,
            note="哈方拥堵，改经阿拉木图中转", reason="口岸调度通知")
        # 货代不能批准
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.approve_route_change(FORWARDER, "ORD-1",
                                          proposal["proposal_id"], basis="x")
        # 提案待批期间路线仍是 v1
        self.assertEqual(self.state().order("ORD-1").current_route, 1)
        self.svc.approve_route_change(SUPERVISOR, "ORD-1",
                                      proposal["proposal_id"],
                                      basis="值班主管批准单 RC-2")
        order = self.state().order("ORD-1")
        self.assertEqual(order.current_route, 2)
        self.assertEqual(order.batches["B-1"].route_version, 2)

        snap = audit_snapshot(self.state(), "ORD-1")
        self.assertEqual([r["version"] for r in snap["routes"]], [1, 2])
        self.assertEqual(snap["routes"][1]["basis"], "值班主管批准单 RC-2")
        self.assertEqual(snap["routes"][1]["proposal_id"], proposal["proposal_id"])

    def test_route_change_cannot_erase_reached_node(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        # 新路线缺少已到达的 loaded
        bad_nodes = ["booked", "departed", "arrived_port", "transship",
                     "inspection", "released", "departed_port", "abroad_transit",
                     "arrived_bishkek", "split_dispatch", "delivered"]
        pid = self.svc.propose_route_change(CARRIER, "ORD-1", bad_nodes,
                                            note="错误路线", reason="测试")
        with self.assertRaisesRegex(ServiceError, "当前节点"):
            self.svc.approve_route_change(SUPERVISOR, "ORD-1",
                                          pid["proposal_id"], basis="x")

    # ------------------------------------------------------------------
    # 拆批与合批
    # ------------------------------------------------------------------

    def test_split_and_merge(self):
        pid = self.svc.propose_split(FORWARDER, "ORD-1", "B-1", ["S-2"],
                                     reason="客户要求比什凯克市区分拨")
        with self.assertRaisesRegex(ServiceError, "无权"):
            self.svc.approve_split(CARRIER, "ORD-1", pid, "B-2", basis="x")
        self.svc.approve_split(SUPERVISOR, "ORD-1", pid, "B-2",
                               basis="批准拆批 SP-1")
        order = self.state().order("ORD-1")
        self.assertEqual([s["slot_no"] for s in order.batches["B-1"].slots], ["S-1"])
        self.assertEqual([s["slot_no"] for s in order.batches["B-2"].slots], ["S-2"])
        self.assertEqual(order.batches["B-2"].node, order.batches["B-1"].node)

        # 合回 B-1
        mid = self.svc.propose_merge(FORWARDER, "ORD-1", "B-1", ["B-2"],
                                     reason="分拨计划取消")
        self.svc.approve_merge(SUPERVISOR, "ORD-1", mid, basis="批准合批 MG-1")
        order = self.state().order("ORD-1")
        self.assertFalse(order.batches["B-2"].active)
        self.assertEqual({s["slot_no"] for s in order.batches["B-1"].slots},
                         {"S-1", "S-2"})

    # ------------------------------------------------------------------
    # 费用结算只能一次
    # ------------------------------------------------------------------

    def test_fee_settle_once(self):
        self.svc.settle_fee(FORWARDER, "ORD-1", "ORD-1-F01", "settled", 500_000,
                            basis="到付结清")
        with self.assertRaisesRegex(ServiceError, "禁止重复结算"):
            self.svc.settle_fee(FORWARDER, "ORD-1", "ORD-1-F01", "settled", 500_000,
                                basis="重复扣款")
        # 放行后保证金可退还
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        for node, msg in [("arrived_port", "M1"), ("transship", "M2"),
                          ("inspection", "M3"), ("released", "M4")]:
            self.svc.ingest(PORT, "ORD-1", msg, "B-1", node, basis=node)
        deposit_id = next(f.fee_id for f in self.state().order("ORD-1").fees.values()
                          if f.fee_type == "release_deposit")
        self.svc.settle_fee(FORWARDER, "ORD-1", deposit_id, "refunded",
                            DEFAULT_DEPOSIT_CENTS, basis="无异常退保证金")
        fees = order_tracking(self.state().order("ORD-1"))["fees"]
        self.assertEqual(fees["settled"], 500_000)
        self.assertEqual(fees["refunded"], DEFAULT_DEPOSIT_CENTS)
        self.assertEqual(fees["held"], 0)

    # ------------------------------------------------------------------
    # 交付
    # ------------------------------------------------------------------

    def test_delivery_closes_batch(self):
        with tempfile.TemporaryDirectory() as td:
            store = EventStore(Path(td) / "l.jsonl")
            svc = FulfillmentService(store)
            short = Route(version=1, nodes=("booked", "loaded", "delivered"),
                          note="短路线")
            svc.open_order(FORWARDER, "ORD-9", "客户乙", 100_000, route=short)
            svc.create_batch(FORWARDER, "ORD-9", "B", [SLOTS[0]], basis="plan")
            with self.assertRaisesRegex(ServiceError, "未到终点"):
                svc.confirm_delivery(CARRIER, "ORD-9", "B", basis="提前签收")
            svc.ingest(CARRIER, "ORD-9", "M1", "B", "loaded", basis="装箱")
            svc.ingest(CARRIER, "ORD-9", "M2", "B", "delivered", basis="交付")
            svc.confirm_delivery(CARRIER, "ORD-9", "B", basis="比什凯克签收单")
            self.assertFalse(replay(store.events()).order("ORD-9").batches["B"].active)

    # ------------------------------------------------------------------
    # 客户脱敏
    # ------------------------------------------------------------------

    def test_customer_view_is_masked(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded",
                        basis="装箱", eta_range={"earliest": "2026-10-10",
                                                 "latest": "2026-10-12"})
        view = customer_view(self.state().order("ORD-1"))
        serialized = json.dumps(view, ensure_ascii=False)
        self.assertNotIn("fees", view)
        self.assertNotIn("500000", serialized)            # 费用金额
        self.assertNotIn("李运营", serialized)            # 责任人姓名
        self.assertNotIn("basis", serialized)             # 内部依据
        self.assertNotIn("message_id", serialized)
        self.assertEqual(view["batches"][0]["node"], "loaded")
        self.assertEqual(view["batches"][0]["eta_range"]["latest"], "2026-10-12")
        self.assertEqual(view["batches"][0]["slot_count"], 2)

    # ------------------------------------------------------------------
    # 审计时间点还原
    # ------------------------------------------------------------------

    def test_point_in_time_replay(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        load_state = self.state()
        seq_at_loaded = load_state.seq
        self.svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")

        past = replay(self.store.events(), until_seq=seq_at_loaded)
        self.assertEqual(past.seq, seq_at_loaded)
        self.assertEqual(past.order("ORD-1").batches["B-1"].node, "loaded")
        snap = audit_snapshot(past, "ORD-1")
        self.assertEqual(snap["current_route"], 1)
        loaded_hist = snap["batches"][0]["history"][-1]
        self.assertEqual(loaded_hist["node"], "loaded")
        self.assertEqual(loaded_hist["basis"], "装箱")  # 每次变更的依据可还原

        # 改线后再看：旧时间点仍是 v1，现在是 v2
        new_nodes = ["booked", "loaded", "departed", "arrived_port", "transship",
                     "inspection", "released", "departed_port", "abroad_transit",
                     "arrived_almaty", "arrived_bishkek", "split_dispatch",
                     "delivered"]
        pid = self.svc.propose_route_change(CARRIER, "ORD-1", new_nodes,
                                            note="绕行阿拉木图", reason="拥堵")
        self.svc.approve_route_change(SUPERVISOR, "ORD-1", pid["proposal_id"],
                                      basis="批准")
        past2 = replay(self.store.events(), until_seq=seq_at_loaded)
        self.assertEqual(past2.order("ORD-1").current_route, 1)
        self.assertEqual(self.state().order("ORD-1").current_route, 2)

    # ------------------------------------------------------------------
    # 崩溃恢复
    # ------------------------------------------------------------------

    def test_recovery_after_half_written_transaction(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        committed = self.store.committed_count()

        # 模拟进程在写 commit 之前崩溃：begin + 两个事件（含一笔保证金），
        # 没有 commit 行。
        next_seq = committed + 1
        with self.store.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"tx": 999, "stage": "begin",
                                 "event_count": 2}) + "\n")
            fh.write(json.dumps({"tx": 999, "seq": next_seq, "event": {
                "type": "node_advanced", "order_no": "ORD-1", "batch_no": "B-1",
                "node": "departed", "seq": next_seq}}) + "\n")
            fh.write(json.dumps({"tx": 999, "seq": next_seq + 1, "event": {
                "type": "fee_booked", "order_no": "ORD-1", "fee_id": "ORD-1-GHOST",
                "fee_type": "release_deposit", "amount_cents": 999_999,
                "seq": next_seq + 1}}) + "\n")
            fh.flush()

        reopened = EventStore(self.store.path)
        self.assertEqual(reopened.committed_count(), committed)   # 半截整笔回滚
        order = replay(reopened.events()).order("ORD-1")
        self.assertEqual(order.batches["B-1"].node, "loaded")    # 没跳过检查点
        self.assertNotIn("ORD-1-GHOST", order.fees)              # 没有多扣保证金

        # 恢复后服务继续工作，新事务编号与序号不冲突。
        svc = FulfillmentService(reopened)
        svc.ingest(CARRIER, "ORD-1", "M-DEP", "B-1", "departed", basis="发车")
        self.assertEqual(self.state().order("ORD-1").batches["B-1"].node, "departed")

    def test_recovery_after_torn_line(self):
        committed = self.store.committed_count()
        with self.store.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"tx": 998, "stage": "begin", "event_count": 1}) + "\n")
            fh.write('{"tx": 998, "seq": ' + str(committed + 1) + ', "event": {"type":')
            fh.flush()
        reopened = EventStore(self.store.path)
        self.assertEqual(reopened.committed_count(), committed)

    def test_corruption_midstream_is_loud(self):
        self.svc.ingest(CARRIER, "ORD-1", "M-LOAD", "B-1", "loaded", basis="装箱")
        # 已提交区域中间被破坏不能静默截断
        data = self.store.path.read_bytes().splitlines(keepends=True)
        data.insert(2, b"{bad json\n")
        self.store.path.write_bytes(b"".join(data))
        with self.assertRaises(CorruptLogError):
            EventStore(self.store.path)

    # ------------------------------------------------------------------
    # 并发：同一条消息被多个线程重复回传
    # ------------------------------------------------------------------

    def test_concurrent_duplicate_messages(self):
        import concurrent.futures

        def send(i: int):
            return self.svc.ingest(CARRIER, "ORD-1", "MSG-CONCUR", "B-1",
                                   "loaded", basis=f"并发回传 {i}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(send, range(8)))
        applied = sum(1 for r in results if r)
        self.assertEqual(applied, 1)                    # 只有一次真正推进
        self.assertEqual(self.batch().node, "loaded")
        self.assertEqual([h["node"] for h in self.batch().history
                          if h["node"] == "loaded"].count("loaded"), 1)


if __name__ == "__main__":
    unittest.main()
