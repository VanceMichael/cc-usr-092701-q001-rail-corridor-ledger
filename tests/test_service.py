import os
import tempfile
import unittest

from src.rail_corridor_ledger import demo
from src.rail_corridor_ledger.events import CrashError
from src.rail_corridor_ledger.model import (
    AuthError, DomainError, OrderError, ProposalError, RoutingError,
)
from src.rail_corridor_ledger.service import FulfillmentService


class ServiceCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.svc = FulfillmentService(os.path.join(self.dir, "wal"),
                                      clock=lambda: "2026-09-10T00:00:00+00:00")
        self.a = demo.actors()
        self.svc.place_order(self.a["manager"], demo.order_cmd())

    def advance(self, slots=("S1", "S2", "S3"), stop_before=None):
        """把指定货位沿 v1 推进到终点。stop_before: 停在某节点之前。"""
        route = demo.default_route()["nodes"]
        plan = [
            ("nanchang_load", self.a["carrier_cn"]),
            ("khorgos_transfer", self.a["carrier_cn"]),
            ("khorgos_inspection", self.a["customs"]),
            ("khorgos_release", self.a["customs"]),
            ("bishkek_arrival", self.a["carrier_kz"]),
            ("bishkek_deliver", self.a["carrier_kz"]),
        ]
        for i, (node, actor) in enumerate(plan):
            if stop_before == node:
                break
            self.svc.report_checkpoint(
                actor,
                {"msg_id": f"m-{node}-{''.join(slots)}",
                 "node_id": node, "slot_ids": list(slots)})

    # ------------------------------------------------------------ 基础结构
    def test_order_split_into_slots_and_batches(self):
        v = self.svc.operations_view()
        self.assertEqual([s["slot_id"] for s in v["slots"]], ["S1", "S2", "S3"])
        self.assertEqual({b["batch_id"]: b["slot_ids"] for b in v["batches"] if b["active"]},
                         {"B1": ["S1", "S2"], "B2": ["S3"]})
        self.assertEqual(v["route_version"], 1)

    def test_duplicate_place_order_rejected(self):
        second = dict(demo.order_cmd())
        second.pop("cmd_id")  # 不走命令号幂等，直接验证重复建单被领域拒绝
        with self.assertRaises(OrderError):
            self.svc.place_order(self.a["manager"], second)

    # ------------------------------------------------------------ 幂等/挂起
    def test_duplicate_message_does_not_readvance_or_recharge(self):
        self.svc.report_checkpoint(
            self.a["carrier_cn"],
            {"cmd_id": "c1", "msg_id": "M1", "node_id": "nanchang_load",
             "slot_ids": ["S1", "S2"]})
        cash_before = self.svc.operations_view()["money"]["accounts"]["cash"]["debit"]
        # 同报文原样重发（含不带 cmd_id 的重发）：零事件
        self.assertEqual(self.svc.report_checkpoint(
            self.a["carrier_cn"],
            {"msg_id": "M1", "node_id": "nanchang_load", "slot_ids": ["S1", "S2"]}), [])
        # 重开进程后仍去重
        svc2 = FulfillmentService(os.path.join(self.dir, "wal"),
                                  clock=self.svc.clock)
        self.assertEqual(svc2.report_checkpoint(
            self.a["carrier_cn"],
            {"msg_id": "M1", "node_id": "nanchang_load", "slot_ids": ["S1", "S2"]}), [])
        cash_after = svc2.operations_view()["money"]["accounts"]["cash"]["debit"]
        self.assertEqual(cash_before, cash_after)

    def test_command_id_retry_is_idempotent(self):
        cmd = {"cmd_id": "C1", "msg_id": "M9", "node_id": "nanchang_load",
               "slot_ids": ["S3"]}
        first = self.svc.report_checkpoint(self.a["carrier_cn"], cmd)
        self.assertEqual(len(first), 2)
        second = self.svc.report_checkpoint(self.a["carrier_cn"], dict(cmd))
        self.assertEqual([e["seq"] for e in second], [e["seq"] for e in first])

    def test_weight_drift_suspends_and_blocks_until_supervisor(self):
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "W1", "node_id": "nanchang_load", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "W2", "node_id": "khorgos_transfer", "slot_ids": ["S3"]})
        evs = self.svc.report_checkpoint(
            self.a["customs"],
            {"msg_id": "W3", "node_id": "khorgos_inspection", "slot_ids": ["S3"],
             "measured_weights": {"S3": "470"}})
        self.assertEqual([e["type"] for e in evs], ["MessageSuspended"])
        # 节点没有被推进
        s3 = next(s for s in self.svc.operations_view()["slots"] if s["slot_id"] == "S3")
        self.assertEqual(s3["current_node"], "khorgos_transfer")
        # 同编号再来也不会推进
        again = self.svc.report_checkpoint(
            self.a["customs"],
            {"msg_id": "W3", "node_id": "khorgos_inspection", "slot_ids": ["S3"],
             "measured_weights": {"S3": "470"}})
        self.assertEqual(again, [])
        # 口岸/承运方无权裁决
        with self.assertRaises(AuthError):
            self.svc.resolve_suspension(self.a["customs"],
                {"suspension_id": "SUS-W3", "action": "confirmed"})
        # 主管确认并补推进查验节点
        self.svc.resolve_suspension(self.a["supervisor"], {
            "cmd_id": "W4", "suspension_id": "SUS-W3", "action": "confirmed",
            "corrected_weights": {"S3": "470"}, "advance_node": True})
        v = self.svc.operations_view()
        s3 = next(s for s in v["slots"] if s["slot_id"] == "S3")
        self.assertEqual(s3["weight"], "470")
        self.assertEqual(s3["current_node"], "khorgos_inspection")
        self.assertEqual(v["money"]["accounts"]["deposit_hold"]["credit"], 0)
        # 正常推进到放行：保证金按 S3 既定份额冻结
        self.svc.report_checkpoint(self.a["customs"], {
            "msg_id": "W5", "node_id": "khorgos_release", "slot_ids": ["S3"]})
        v = self.svc.operations_view()
        self.assertEqual(v["money"]["accounts"]["deposit_hold"]["credit"],
                         next(s for s in v["slots"] if s["slot_id"] == "S3")["deposit_share"])
        self.assertGreater(v["money"]["accounts"]["deposit_hold"]["credit"], 0)
        # 裁决后该报文编号关闭
        self.assertEqual(self.svc.report_checkpoint(
            self.a["customs"],
            {"msg_id": "W3", "node_id": "khorgos_inspection", "slot_ids": ["S3"]}), [])

    def test_supervisor_can_discard_suspended_message(self):
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "D1", "node_id": "nanchang_load", "slot_ids": ["S1"]})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "D2", "node_id": "khorgos_transfer", "slot_ids": ["S1"]})
        self.svc.report_checkpoint(self.a["customs"],
            {"msg_id": "D3", "node_id": "khorgos_inspection", "slot_ids": ["S1"],
             "measured_weights": {"S1": "9999"}})
        self.svc.resolve_suspension(self.a["supervisor"],
            {"suspension_id": "SUS-D3", "action": "discarded", "note": "误报"})
        s1 = next(s for s in self.svc.operations_view()["slots"] if s["slot_id"] == "S1")
        self.assertEqual(s1["current_node"], "khorgos_transfer")  # 未补推进
        sus = next(x for x in self.svc.operations_view()["suspensions"]
                   if x["suspension_id"] == "SUS-D3")
        self.assertEqual(sus["status"], "discarded")

    # ------------------------------------------------------------ 顺序/异常
    def test_same_msg_id_different_slots_suspends(self):
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "Q1", "node_id": "nanchang_load", "slot_ids": ["S1"]})
        evs = self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "Q1", "node_id": "nanchang_load", "slot_ids": ["S2"]})
        self.assertEqual([e["type"] for e in evs], ["MessageSuspended"])
        sus = self.svc.operations_view()["suspensions"][-1]
        self.assertEqual(sus["kind"], "identity_conflict")
        # S2 没有被冒名推进
        s2 = next(s for s in self.svc.operations_view()["slots"] if s["slot_id"] == "S2")
        self.assertIsNone(s2["current_node"])

    def test_late_message_on_retired_route_version_rejected(self):
        # S3 先发运；随后主管改线（删掉查验节点），旧版本的迟到查验报文必须拒绝
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "L0", "node_id": "nanchang_load", "slot_ids": ["S3"]})
        new_route = demo.default_route()
        new_route["version"] = 2
        new_route["nodes"] = [n for n in new_route["nodes"]
                              if n["id"] != "khorgos_inspection"]
        self.svc.propose(self.a["manager"], {
            "cmd_id": "L1", "proposal_id": "PR", "kind": "reroute",
            "route": new_route, "carry_to_index": 1, "reason": "免检"})
        self.svc.review_proposal(self.a["supervisor"],
            {"cmd_id": "L2", "proposal_id": "PR", "decision": "approved"})
        with self.assertRaises(RoutingError):
            self.svc.report_checkpoint(self.a["customs"],
                {"msg_id": "L3", "node_id": "khorgos_inspection", "slot_ids": ["S3"]})

    def test_cannot_skip_node(self):
        with self.assertRaises(RoutingError):
            self.svc.report_checkpoint(self.a["carrier_cn"],
                {"msg_id": "J1", "node_id": "khorgos_transfer", "slot_ids": ["S1"]})

    def test_exception_holds_slot_and_blocks_downstream(self):
        self.advance(slots=("S1",), stop_before="bishkek_arrival")
        self.svc.report_exception(self.a["customs"], {
            "msg_id": "E1", "exception_id": "EX1",
            "node_id": "bishkek_arrival", "slot_ids": ["S1"],
            "reason": "箱体破损暂扣"})
        v = self.svc.operations_view()
        s1 = next(s for s in v["slots"] if s["slot_id"] == "S1")
        held = [t for t in s1["trace"] if t["status"] == "held"]
        self.assertEqual([t["node_id"] for t in held], ["bishkek_arrival"])
        exc = v["exceptions"][0]
        self.assertEqual(exc["status"], "open")
        with self.assertRaises(OrderError):
            self.svc.report_checkpoint(self.a["carrier_kz"],
                {"msg_id": "E2", "node_id": "bishkek_deliver", "slot_ids": ["S1"]})
        # 重复异常报文不重复登记
        self.assertEqual(self.svc.report_exception(self.a["customs"], {
            "msg_id": "E1", "exception_id": "EX1",
            "node_id": "bishkek_arrival", "slot_ids": ["S1"],
            "reason": "箱体破损暂扣"}), [])

    def test_only_supervisor_resolves_exception_with_claim(self):
        self.advance(slots=("S1",), stop_before="bishkek_arrival")
        self.svc.report_exception(self.a["customs"], {
            "msg_id": "E1", "exception_id": "EX1",
            "node_id": "bishkek_arrival", "slot_ids": ["S1"], "reason": "货损"})
        with self.assertRaises(AuthError):
            self.svc.resolve_exception(self.a["customs"],
                {"exception_id": "EX1", "claim_amount": "100"})
        self.svc.resolve_exception(self.a["supervisor"],
            {"cmd_id": "R1", "exception_id": "EX1", "claim_amount": "100"})
        accts = self.svc.operations_view()["money"]["accounts"]
        self.assertEqual(accts["claim_expense"]["debit"], 100)
        self.assertEqual(accts["claim_payable"]["credit"], 100)
        # 重复解除无效
        self.assertEqual(self.svc.resolve_exception(self.a["supervisor"],
            {"exception_id": "EX1", "claim_amount": "100"}), [])

    # ------------------------------------------------------------ 授权/区段
    def test_carrier_can_only_own_segment(self):
        with self.assertRaisesRegex(AuthError, "无权维护区段"):
            self.svc.report_checkpoint(self.a["carrier_kz"],
                {"msg_id": "A1", "node_id": "nanchang_load", "slot_ids": ["S1"]})

    def test_customs_only_khorgos_inspection_release(self):
        with self.assertRaises(AuthError):
            self.svc.report_checkpoint(self.a["customs"],
                {"msg_id": "A2", "node_id": "nanchang_load", "slot_ids": ["S1"]})
        with self.assertRaises(AuthError):
            self.svc.report_checkpoint(self.a["carrier_cn"],
                {"msg_id": "A3", "node_id": "khorgos_inspection", "slot_ids": ["S1"]})

    def test_customer_cannot_mutate(self):
        with self.assertRaises(AuthError):
            self.svc.report_checkpoint(self.a["customer"],
                {"msg_id": "A4", "node_id": "nanchang_load", "slot_ids": ["S1"]})

    # ------------------------------------------------------------ 改线/拆并
    def test_reroute_requires_approved_proposal_and_versions_route(self):
        with self.assertRaises(AuthError):
            self.svc.review_proposal(self.a["manager"],
                {"proposal_id": "P1", "decision": "approved"})
        new_route = demo.default_route()
        new_route["version"] = 2
        new_route["name"] = "南昌—霍尔果斯—比什凯克（快线）"
        new_route["nodes"] = [n for n in new_route["nodes"]
                              if n["id"] != "khorgos_inspection"]
        self.svc.propose(self.a["manager"], {
            "cmd_id": "P1", "proposal_id": "P1", "kind": "reroute",
            "route": new_route, "carry_to_index": 1, "reason": "免检资质"})
        # 未批准前路线还是 v1
        self.assertEqual(self.svc.operations_view()["route_version"], 1)
        self.svc.review_proposal(self.a["supervisor"],
            {"cmd_id": "P2", "proposal_id": "P1", "decision": "approved"})
        v = self.svc.operations_view()
        self.assertEqual(v["route_version"], 2)
        self.assertEqual([n["id"] for n in v["route"]["nodes"]],
                         ["nanchang_load", "khorgos_transfer", "khorgos_release",
                          "bishkek_arrival", "bishkek_deliver"])
        # 路线史保留 v1/v2，审计可还原
        self.assertEqual([r["version"] for r in v["route_history"]], [1, 2])
        # 重复审批被拒
        with self.assertRaises(ProposalError):
            self.svc.review_proposal(self.a["supervisor"],
                {"proposal_id": "P1", "decision": "approved"})

    def test_split_then_merge_with_supervisor_approval(self):
        self.svc.propose(self.a["manager"], {
            "cmd_id": "S1", "proposal_id": "PS", "kind": "split",
            "payload": {"parent_batch": "B1", "children": [
                {"batch_id": "B1a", "slot_ids": ["S1"], "carrier_id": demo.CARRIER_CN},
                {"batch_id": "B1b", "slot_ids": ["S2"], "carrier_id": demo.CARRIER_CN}]},
            "reason": "分拨"})
        # 批准前子批次不可用
        with self.assertRaises(OrderError):
            self.svc.report_checkpoint(self.a["carrier_cn"],
                {"msg_id": "X1", "node_id": "nanchang_load", "batch_id": "B1a"})
        self.svc.review_proposal(self.a["supervisor"],
            {"cmd_id": "S2", "proposal_id": "PS", "decision": "approved"})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "X1", "node_id": "nanchang_load", "batch_id": "B1a"})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "X2", "node_id": "nanchang_load", "batch_id": "B1b"})
        for bid, m in [("B1a", "Y1"), ("B1b", "Y2")]:
            self.svc.report_checkpoint(self.a["carrier_cn"],
                {"msg_id": m, "node_id": "khorgos_transfer", "batch_id": bid})
        # 覆盖不全的拆批拒绝
        bad = self.svc.propose
        with self.assertRaises(ProposalError):
            self.svc.propose(self.a["manager"], {
                "proposal_id": "PBAD", "kind": "split",
                "payload": {"parent_batch": "B2", "children": [
                    {"batch_id": "B2x", "slot_ids": [], "carrier_id": demo.CARRIER_CN}]}})
        # 同位置合批
        self.svc.propose(self.a["manager"], {
            "cmd_id": "M1", "proposal_id": "PM", "kind": "merge",
            "payload": {"batches": ["B1a", "B1b"], "result_batch_id": "B1m",
                        "carrier_id": demo.CARRIER_KZ},
            "reason": "同地合并"})
        self.svc.review_proposal(self.a["supervisor"],
            {"cmd_id": "M2", "proposal_id": "PM", "decision": "approved"})
        v = self.svc.operations_view()
        merged = next(b for b in v["batches"] if b["batch_id"] == "B1m")
        self.assertTrue(merged["active"])
        self.assertEqual(merged["slot_ids"], ["S1", "S2"])
        self.assertEqual(next(s for s in v["slots"] if s["slot_id"] == "S1")["batch_id"], "B1m")

    def test_merge_requires_same_position(self):
        # B1 走到换装，B2 停在起点，位置不同不能合
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "Z1", "node_id": "nanchang_load", "batch_id": "B1"})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "Z2", "node_id": "khorgos_transfer", "batch_id": "B1"})
        with self.assertRaises(ProposalError):
            self.svc.propose(self.a["manager"], {
                "proposal_id": "PD", "kind": "merge",
                "payload": {"batches": ["B1", "B2"], "result_batch_id": "BAD"}})

    # ------------------------------------------------------------ 资金守恒
    def test_money_conservation_end_to_end(self):
        self.advance()
        v = self.svc.operations_view()
        accts = v["money"]["accounts"]
        # 费用：预占全部结转为收入
        self.assertEqual(accts["fee_hold"], {"debit": 100000, "credit": 100000})
        self.assertEqual(accts["fee_income"]["credit"], 100000)
        # 保证金：全部冻结后全部退还
        self.assertEqual(accts["deposit_hold"]["debit"], accts["deposit_hold"]["credit"])
        self.assertEqual(accts["deposit_hold"]["credit"], 20000)
        # 现金：收入运费 100000 + 保证金 20000 借，退还保证金 20000 贷
        self.assertEqual(accts["cash"], {"debit": 120000, "credit": 20000})
        # 每条分录借=贷（逐事务）
        self.assertTrue(v["completed"])

    def test_partial_delivery_settles_only_delivered_shares(self):
        # 先只交付 S3；S1/S2 在途，费用不得结转
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "P0", "node_id": "nanchang_load", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["carrier_cn"],
            {"msg_id": "P1", "node_id": "khorgos_transfer", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["customs"],
            {"msg_id": "P2", "node_id": "khorgos_inspection", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["customs"],
            {"msg_id": "P3", "node_id": "khorgos_release", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["carrier_kz"],
            {"msg_id": "P4", "node_id": "bishkek_arrival", "slot_ids": ["S3"]})
        self.svc.report_checkpoint(self.a["carrier_kz"],
            {"msg_id": "P5", "node_id": "bishkek_deliver", "slot_ids": ["S3"]})
        v = self.svc.operations_view()
        self.assertFalse(v["completed"])
        s3 = next(s for s in v["slots"] if s["slot_id"] == "S3")
        self.assertEqual(acct(v, "fee_income", "credit"), s3["fee_share"])
        self.assertEqual(acct(v, "fee_hold", "debit"), s3["fee_share"])
        # 其余份额仍在预占
        self.assertEqual(acct(v, "fee_hold", "credit"), 100000)

    # ------------------------------------------------------------ 视图/审计
    def test_customer_view_is_masked(self):
        self.advance(slots=("S1",), stop_before="khorgos_release")
        cv = self.svc.customer_view()
        s1 = next(s for s in cv["slots"] if s["slot_id"] == "S1")
        self.assertNotIn("carrier_id", s1)
        self.assertEqual(s1["current_segment"], "霍尔果斯口岸")
        self.assertEqual(s1["eta_window"]["latest"], "2026-09-12")
        blob = repr(cv)
        self.assertNotIn(demo.CARRIER_CN, blob)
        self.assertNotIn(demo.CARRIER_KZ, blob)

    def test_audit_at_arbitrary_seq_and_ts(self):
        self.advance(slots=("S1",))
        events = self.svc.raw_events()
        mid = events[len(events) // 2]["seq"]
        at = self.svc.audit_at(seq=mid)
        now = self.svc.audit_at()
        self.assertLessEqual(
            sum(1 for s in at["slots"] for t in s["trace"]),
            sum(1 for s in now["slots"] for t in s["trace"]))
        # 时间点还原：用最早时间戳得到空单状态前（建单事件即首时间戳）
        first_ts = events[0]["ts"]
        at_ts = self.svc.audit_at(ts=first_ts)
        self.assertIsNotNone(at_ts["order_id"])

    def test_timeline_records_basis_for_each_change(self):
        self.advance(slots=("S1",), stop_before="bishkek_arrival")
        tl = self.svc.timeline()
        cps = [t for t in tl if t["type"] == "SlotCheckpointReached"]
        self.assertTrue(all(t.get("msg_id") for t in cps))
        self.assertEqual(cps[0]["node_id"], "nanchang_load")

    # ------------------------------------------------------------ 崩溃恢复
    def test_crash_mid_write_neither_double_charges_nor_skips(self):
        for stage in ("after_begin", "after_event", "partial_tail"):
            with self.subTest(stage=stage):
                path = os.path.join(self.dir, "wal-" + stage)
                svc = FulfillmentService(path, clock=self.svc.clock)
                svc.place_order(self.a["manager"], demo.order_cmd())
                fee_before = svc.operations_view()["money"]["accounts"]["fee_hold"]["credit"]
                with self.assertRaises(CrashError):
                    svc.report_checkpoint(self.a["carrier_cn"], {
                        "cmd_id": "K1", "msg_id": "K1",
                        "node_id": "nanchang_load", "slot_ids": ["S1", "S2"]},
                        crash=stage)
                # 重新打开：未提交事务完全不存在
                svc2 = FulfillmentService(path, clock=self.svc.clock)
                self.assertEqual(
                    svc2.operations_view()["money"]["accounts"]["fee_hold"]["credit"],
                    fee_before)
                # 重试同一命令：恰好推进一次
                svc2.report_checkpoint(self.a["carrier_cn"], {
                    "cmd_id": "K1", "msg_id": "K1",
                    "node_id": "nanchang_load", "slot_ids": ["S1", "S2"]})
                v = FulfillmentService(path, clock=self.svc.clock).operations_view()
                reached = [s for s in v["slots"] if s["current_node"] == "nanchang_load"]
                self.assertEqual({s["slot_id"] for s in reached}, {"S1", "S2"})

    def test_crash_after_commit_retry_is_idempent(self):
        path = os.path.join(self.dir, "wal-done")
        svc = FulfillmentService(path, clock=self.svc.clock)
        svc.place_order(self.a["manager"], demo.order_cmd())
        with self.assertRaises(CrashError):
            svc.report_checkpoint(self.a["carrier_cn"], {
                "cmd_id": "K2", "msg_id": "K2",
                "node_id": "nanchang_load", "slot_ids": ["S1"]}, crash="after_commit")
        svc2 = FulfillmentService(path, clock=self.svc.clock)
        retry = svc2.report_checkpoint(self.a["carrier_cn"], {
            "cmd_id": "K2", "msg_id": "K2",
            "node_id": "nanchang_load", "slot_ids": ["S1"]})
        # cmd_id 命中，返回首次事件，不新增
        total_after = len(svc2.raw_events())
        self.assertEqual(len(retry), 2)
        self.assertTrue(all(e["seq"] <= total_after for e in retry))


def acct(view, name, side):
    return view["money"]["accounts"][name][side]


if __name__ == "__main__":
    unittest.main()
