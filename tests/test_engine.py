"""正确性测试：约束、读集合冲突、原子发布、幂等与串行基线等价。"""
from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pointwise.engine import Engine, seed_data, preset_scenarios
from pointwise.constraints import Stager, SnapshotView, validate
from pointwise.coordinator import CommitCoordinator
from pointwise.models import (
    WriteItem, GRAN_REGION, GRAN_ROW,
    ACCEPTED, REJECTED_RECOMPUTE, REJECTED_MANUAL,
)
from pointwise.scenarios import ReplayEngine, BatchSpec


class ConstraintTests(unittest.TestCase):
    def setUp(self):
        self.eng = Engine()
        self.stager = self.eng.stager

    def test_negative_amount_is_hard_reject(self):
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("R1", "P101", -1)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, REJECTED_MANUAL)
        self.assertEqual(oc.reasons[0].code, "negative_amount")
        self.assertIsNone(oc.committed_store_version)
        # 拒绝时事实表原样不变
        self.assertEqual(self.eng.store.store_version, 0)

    def test_plan_not_found_is_hard_reject(self):
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("R1", "NOPE", 5)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, REJECTED_MANUAL)
        self.assertEqual(oc.reasons[0].code, "plan_not_found")

    def test_region_not_found(self):
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("ZZ", "P101", 5)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, REJECTED_MANUAL)
        self.assertEqual(oc.reasons[0].code, "region_not_found")

    def test_region_sum_exceeded_without_conflict_is_manual(self):
        # R1 合计 80，P101 改为 95 -> 合计 135 > 100，无并发 -> 人工类
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("R1", "P101", 95)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, REJECTED_MANUAL)
        self.assertEqual(oc.reasons[0].code, "region_sum_exceeded")
        self.assertFalse(oc.conflict)

    def test_valid_batch_accepted_and_version_bumps(self):
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("R1", "P101", 50)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, ACCEPTED)
        self.assertEqual(oc.committed_store_version, 1)
        self.assertEqual(self.eng.store.live_row("R1", "P101").amount, 50)
        self.assertEqual(self.eng.store.live_row("R1", "P101").version, 2)

    def test_read_set_records_all_region_members(self):
        snap = self.stager.take_snapshot()
        staged = self.stager.stage("x", [WriteItem("R1", "P101", 50)], snap=snap)
        rr = staged.read_set.region_reads[0]
        self.assertEqual(rr.region, "R1")
        self.assertEqual(set(rr.plans), {"P101", "P102", "P103"})
        self.assertEqual(rr.cap, 100)
        self.assertEqual(len(rr.row_versions), 3)


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.eng = Engine()

    def _two_batches_same_snapshot(self, w1, w2):
        snap = self.eng.stager.take_snapshot()
        a = self.eng.stager.stage("A", w1, snap=snap, label="A")
        b = self.eng.stager.stage("B", w2, snap=snap, label="B")
        return a, b

    def test_phantom_sum_region_mode_rejects_second_as_recompute(self):
        # R1 余量 20；A: P101 40->60（+20，刚好用尽），B: P102 30->45（+15）
        a, b = self._two_batches_same_snapshot(
            [WriteItem("R1", "P101", 60)],
            [WriteItem("R1", "P102", 45)])
        self.eng.coordinator.set_granularity(GRAN_REGION)
        oc_a = self.eng.coordinator.commit(a)
        oc_b = self.eng.coordinator.commit(b)
        self.assertEqual(oc_a.status, ACCEPTED)
        self.assertEqual(oc_b.status, REJECTED_RECOMPUTE)
        self.assertTrue(oc_b.conflict)
        self.assertTrue(oc_b.revalidated)
        # 冲突依据：B 的区域谓词读到的 P101 行版本已变
        kinds = {e.kind for e in oc_b.conflict_evidence}
        self.assertIn("region_row_version", kinds)
        blame = {bid for e in oc_b.conflict_evidence for bid in e.committed_by}
        self.assertIn("A", blame)
        # 事实表只反映 A
        self.assertEqual(self.eng.store.live_row("R1", "P101").amount, 60)
        self.assertEqual(self.eng.store.live_row("R1", "P102").amount, 30)
        self.assertEqual(self.eng.store.store_version, 1)

    def test_phantom_sum_row_mode_wrongly_accepts_both(self):
        # 缺陷对照：仅行版本核对无法发现区域合计前提失效
        a, b = self._two_batches_same_snapshot(
            [WriteItem("R1", "P101", 60)],
            [WriteItem("R1", "P102", 45)])
        self.eng.coordinator.set_granularity(GRAN_ROW)
        oc_a = self.eng.coordinator.commit(a)
        oc_b = self.eng.coordinator.commit(b)
        self.assertEqual(oc_a.status, ACCEPTED)
        self.assertEqual(oc_b.status, ACCEPTED)  # 错误双接受
        total = sum(r.amount for r in self.eng.store.live_region_rows("R1"))
        self.assertGreater(total, 100)  # 越限 115

    def test_same_row_write_conflict_both_modes_detect(self):
        for gran in (GRAN_REGION, GRAN_ROW):
            self.eng.reset()
            self.eng.coordinator.set_granularity(gran)
            a, b = self._two_batches_same_snapshot(
                [WriteItem("R2", "P201", 30)],
                [WriteItem("R2", "P201", 33)])
            self.assertEqual(self.eng.coordinator.commit(a).status, ACCEPTED)
            oc_b = self.eng.coordinator.commit(b)
            self.assertTrue(oc_b.conflict)
            self.assertEqual(oc_b.conflict_evidence[0].kind, "row_version")
            # 新状态下 R2 合计 33+15+10=58 <= 60 -> 重校验通过
            self.assertEqual(oc_b.status, ACCEPTED)
            self.assertEqual(self.eng.store.live_row("R2", "P201").amount, 33)

    def test_same_row_overflow_after_conflict_still_rejected_but_recompute(self):
        # B 想把 P201 改成 60：冲突后新状态（A 已改为30）下 60+15+10=85>60
        self.eng.coordinator.set_granularity(GRAN_REGION)
        a, b = self._two_batches_same_snapshot(
            [WriteItem("R2", "P201", 30)],
            [WriteItem("R2", "P201", 60)])
        self.assertEqual(self.eng.coordinator.commit(a).status, ACCEPTED)
        oc_b = self.eng.coordinator.commit(b)
        self.assertEqual(oc_b.status, REJECTED_RECOMPUTE)
        # B 未发布
        self.assertEqual(self.eng.store.live_row("R2", "P201").amount, 30)

    def test_cross_region_batches_both_accept_no_conflict(self):
        a, b = self._two_batches_same_snapshot(
            [WriteItem("R1", "P101", 45)],
            [WriteItem("R2", "P202", 20)])
        self.eng.coordinator.set_granularity(GRAN_REGION)
        self.assertEqual(self.eng.coordinator.commit(a).status, ACCEPTED)
        oc_b = self.eng.coordinator.commit(b)
        self.assertEqual(oc_b.status, ACCEPTED)
        self.assertFalse(oc_b.conflict)

    def test_idempotent_retry_returns_same_published_result(self):
        snap = self.eng.stager.take_snapshot()
        staged = self.eng.stager.stage("dup", [WriteItem("R1", "P103", 15)], snap=snap)
        first = self.eng.coordinator.commit(staged)
        # 模拟响应丢失：用同一 batch_id、同一写集合重试
        again = self.eng.stager.stage("dup", [WriteItem("R1", "P103", 15)],
                                      snap=self.eng.stager.take_snapshot())
        second = self.eng.coordinator.commit(again)
        self.assertEqual(first.status, ACCEPTED)
        self.assertEqual(second.status, ACCEPTED)
        self.assertEqual(first.committed_store_version, second.committed_store_version)
        self.assertEqual(second.attempt, 2)
        self.assertEqual(self.eng.store.store_version, 1)  # 只发布一次

    def test_recompute_then_retry_on_fresh_snapshot_succeeds_when_cap_allows(self):
        # 阶段1：A 释放 P101 40->20（R1 合计 60，余量 40）
        snap = self.eng.stager.take_snapshot()
        a = self.eng.stager.stage("A", [WriteItem("R1", "P101", 20)], snap=snap)
        self.assertEqual(self.eng.coordinator.commit(a).status, ACCEPTED)

        # 阶段2：C 基于 A 之后的快照，P102 30->70（合计 100，恰好合法）
        snap_c = self.eng.stager.take_snapshot()
        c = self.eng.stager.stage("C", [WriteItem("R1", "P102", 70)], snap=snap_c)

        # 阶段3：D 先提交，P103 10->30（合计 100）；C 的区域谓词前提被改变，
        # 锁内重校验 120>100 -> rejected_recompute，且 C 未发布
        snap_d = self.eng.stager.take_snapshot()
        d = self.eng.stager.stage("D", [WriteItem("R1", "P103", 30)], snap=snap_d)
        self.assertEqual(self.eng.coordinator.commit(d).status, ACCEPTED)
        oc_c = self.eng.coordinator.commit(c)
        self.assertEqual(oc_c.status, REJECTED_RECOMPUTE)
        self.assertEqual(self.eng.store.live_row("R1", "P102").amount, 30)

        # 阶段4：额度再次被释放（P103 30->5），C 基于全新快照重算（金额不变，
        # 系统不自动改小金额凑上限）：20+70+5=95 <= 100 -> 接受
        snap_e = self.eng.stager.take_snapshot()
        e = self.eng.stager.stage("E", [WriteItem("R1", "P103", 5)], snap=snap_e)
        self.assertEqual(self.eng.coordinator.commit(e).status, ACCEPTED)
        fresh = self.eng.stager.take_snapshot()
        c2 = self.eng.stager.stage("C-r1", [WriteItem("R1", "P102", 70)], snap=fresh)
        oc_c2 = self.eng.coordinator.commit(c2)
        self.assertEqual(oc_c2.status, ACCEPTED)
        self.assertEqual(sum(r.amount for r in self.eng.store.live_region_rows("R1")), 95)

    def test_system_never_auto_shrinks_amounts(self):
        snap = self.eng.stager.take_snapshot()
        staged = self.eng.stager.stage("big", [WriteItem("R1", "P101", 95)], snap=snap)
        oc = self.eng.coordinator.commit(staged)
        self.assertEqual(oc.status, REJECTED_MANUAL)
        self.assertEqual(self.eng.store.live_row("R1", "P101").amount, 40)

    def test_real_threads_region_mode_never_violates_caps(self):
        # 多线程压力：任意交错下区域合计永远不越限
        snap = self.eng.stager.take_snapshot()
        batches = []
        for i in range(12):
            region = "R1" if i % 2 == 0 else "R2"
            plan = {"R1": ["P101", "P102", "P103"],
                    "R2": ["P201", "P202", "P203"]}[region][i % 3]
            batches.append(self.eng.stager.stage(
                f"T{i:02d}", [WriteItem(region, plan, 60 if region == "R1" else 40)],
                snap=snap))
        barrier = threading.Barrier(len(batches))
        outcomes = []

        def run(b):
            barrier.wait()
            outcomes.append(self.eng.coordinator.commit(b))

        threads = [threading.Thread(target=run, args=(b,)) for b in batches]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for region, cap in (("R1", 100), ("R2", 60)):
            total = sum(r.amount for r in self.eng.store.live_region_rows(region))
            self.assertLessEqual(total, cap, f"{region} 越限: {total}>{cap}")


class SerialEquivalenceTests(unittest.TestCase):
    def test_region_mode_final_state_matches_some_serial_order(self):
        # 并发协调器（region 粒度）的最终事实表必须可由某个串行顺序产生
        eng = Engine()
        snap = eng.stager.take_snapshot()
        writes_a = [WriteItem("R1", "P101", 55)]
        writes_b = [WriteItem("R1", "P102", 40)]
        a = eng.stager.stage("A", writes_a, snap=snap)
        b = eng.stager.stage("B", writes_b, snap=snap)
        barrier = threading.Barrier(2)

        def run(batch):
            barrier.wait()
            eng.coordinator.commit(batch)

        ts = [threading.Thread(target=run, args=(x,)) for x in (a, b)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        concurrent_state = {(r["region"], r["plan"]): r["amount"]
                            for r in eng.store.state_dict()["rows"]}

        # 两种串行顺序分别模拟
        candidates = []
        for order in (("A", "B"), ("B", "A")):
            ref = Engine()
            rsnap = ref.stager.take_snapshot()
            staged = {"A": ref.stager.stage("A", writes_a, snap=rsnap),
                      "B": ref.stager.stage("B", writes_b, snap=rsnap)}
            for bid in order:
                ref.coordinator.commit(staged[bid])
            candidates.append({(r["region"], r["plan"]): r["amount"]
                               for r in ref.store.state_dict()["rows"]})
        self.assertIn(concurrent_state, candidates)

    def test_all_preset_scenarios_execute(self):
        eng = Engine()
        for gran in (GRAN_REGION, GRAN_ROW):
            for d in preset_scenarios().values():
                result = eng.replay.run(
                    d.id, eng.seed_rows, eng.seed_caps,
                    d.specs, d.commit_order, granularity=gran,
                    compute_delay=0, retries=d.retries)
                self.assertEqual(result.scenario, d.id)
                self.assertTrue(result.timeline)
                # 任何模式下硬违约批次都不能发布
                for oc in result.outcomes.values():
                    if any(i.severity == "hard" for i in oc.reasons):
                        self.assertNotEqual(oc.status, ACCEPTED)

    def test_phantom_scenario_timeline_matches_documented_outcomes(self):
        eng = Engine()
        d = preset_scenarios()["phantom_sum"]
        result = eng.replay.run(
            d.id, eng.seed_rows, eng.seed_caps,
            d.specs, d.commit_order, granularity=GRAN_REGION, compute_delay=0)
        self.assertEqual(result.outcomes["A"].status, ACCEPTED)
        self.assertEqual(result.outcomes["B"].status, REJECTED_RECOMPUTE)
        kinds = [e.kind for e in result.outcomes["B"].conflict_evidence]
        self.assertIn("region_row_version", kinds)


if __name__ == "__main__":
    unittest.main(verbosity=2)
