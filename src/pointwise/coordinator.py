"""提交协调器：读集合冲突检测 + 锁内原子"重校验即发布"。

关键设计：
  * 约束检查与正式发布在同一把提交锁内连续完成，中间不存在可被插队的窗口；
    拒绝路径在任何 apply 之前返回，事实表原样不变。
  * 提交端不只核对写集合的行版本，而是核对校验器实际读取的区域谓词
    （成员行集合、每个成员行版本、上限版本）。任何会改变判断前提的已提交
    修改都会被识别为冲突并触发基于当前状态的重新校验。
  * 区分两类拒绝：
      rejected_recompute —— 前提被并发提交改变，可基于新快照重新计算；
      rejected_manual    —— 输入本身违约（负值/方案缺失/无冲突下已超限），
                            必须人工调整，系统绝不自动改小金额凑上限。
  * 提交结果按 batch_id 幂等缓存：接受已落地而响应丢失时，重试返回同一结果。
"""
from __future__ import annotations

import threading
import time

from .constraints import StagedBatch, LiveView, validate
from .models import (
    CommitOutcome, ConflictEvidence, ValidationIssue,
    ACCEPTED, REJECTED_RECOMPUTE, REJECTED_MANUAL,
    GRAN_REGION, GRAN_ROW, rk,
)
from .store import Store


class CommitCoordinator:
    def __init__(self, store: Store, granularity: str = GRAN_REGION) -> None:
        assert granularity in (GRAN_REGION, GRAN_ROW)
        self.store = store
        self.granularity = granularity
        self._outcomes: dict[str, CommitOutcome] = {}
        # 提交锁：临界区只包含冲突检测、重校验与 apply，计算延迟不持锁
        self._commit_lock = threading.Lock()
        self.stats = {"commits": 0, "conflicts": 0, "revalidations": 0}

    def set_granularity(self, granularity: str) -> None:
        assert granularity in (GRAN_REGION, GRAN_ROW)
        self.granularity = granularity

    def reset(self) -> None:
        with self._commit_lock:
            self._outcomes.clear()
            self.stats = {"commits": 0, "conflicts": 0, "revalidations": 0}

    def cached_outcome(self, batch_id: str) -> CommitOutcome | None:
        return self._outcomes.get(batch_id)

    def all_outcomes(self) -> list[CommitOutcome]:
        return list(self._outcomes.values())

    # ------------------------------------------------------------------ 冲突归因
    def _events_after(self, base_version: int) -> list:
        return [e for e in self.store.events()
                if e.store_version_after > base_version]

    def _blame(self, base_version: int, region: str, plan: str | None) -> list[str]:
        key = rk(region, plan) if plan else None
        blame: list[str] = []
        for e in self._events_after(base_version):
            if (key is not None and key in e.changed_keys) or (
                    key is None and region in e.region):
                if e.batch_id not in blame:
                    blame.append(e.batch_id)
        return blame

    # ------------------------------------------------------------------ 读集合冲突检测
    def _detect_conflicts(self, batch: StagedBatch) -> list[ConflictEvidence]:
        evidence: list[ConflictEvidence] = []
        rs = batch.read_set
        base = batch.base_store_version

        # 行版本冲突：写集合基于的行已被别人改过（两种粒度都检查）
        for key_str, expected_ver in rs.written_row_versions.items():
            region, plan = key_str.split("|", 1)
            live = self.store.live_row(region, plan)
            actual_ver = live.version if live else None
            if actual_ver != expected_ver:
                evidence.append(ConflictEvidence(
                    kind="row_version", region=region, plan=plan,
                    expected=expected_ver, actual=actual_ver,
                    committed_by=self._blame(base, region, plan)))

        if self.granularity == GRAN_ROW:
            # 缺陷对照模式：只看写集合行版本，忽略区域谓词前提。
            return evidence

        # 区域谓词冲突：校验 SUM<=cap 时读过的每个成员行、成员关系、上限版本
        for rr in rs.region_reads:
            live_members = {r.plan: r for r in self.store.live_region_rows(rr.region)}
            expected_plans = set(rr.plans)
            actual_plans = set(live_members)

            for added in sorted(actual_plans - expected_plans):
                evidence.append(ConflictEvidence(
                    kind="region_membership", region=rr.region, plan=added,
                    expected="成员关系中不存在", actual=f"新成员行 v{live_members[added].version}",
                    committed_by=self._blame(base, rr.region, added)))
            for removed in sorted(expected_plans - actual_plans):
                evidence.append(ConflictEvidence(
                    kind="region_membership", region=rr.region, plan=removed,
                    expected="成员行存在", actual="成员行已消失",
                    committed_by=self._blame(base, rr.region, removed)))

            expected_map = dict(zip(rr.plans, rr.row_versions))
            for plan in sorted(expected_plans & actual_plans):
                exp_ver = expected_map[plan]
                act_ver = live_members[plan].version
                if exp_ver != act_ver:
                    evidence.append(ConflictEvidence(
                        kind="region_row_version", region=rr.region, plan=plan,
                        expected=f"v{exp_ver}", actual=f"v{act_ver}",
                        committed_by=self._blame(base, rr.region, plan)))

            live_cap = self.store.live_cap(rr.region)
            if live_cap is not None and live_cap.version != rr.cap_version:
                evidence.append(ConflictEvidence(
                    kind="cap_version", region=rr.region, plan=None,
                    expected=f"cap v{rr.cap_version}", actual=f"cap v{live_cap.version}",
                    committed_by=self._blame(base, rr.region, None)))
        return evidence

    # ------------------------------------------------------------------ 提交
    def commit(self, batch: StagedBatch) -> CommitOutcome:
        t0 = time.perf_counter()

        # 幂等：接受已落地但响应丢失时，重试返回同一发布结果，绝不重复发布
        cached = self._outcomes.get(batch.batch_id)
        if cached is not None:
            cached.attempt += 1
            return cached

        with self._commit_lock:
            # double-check：等待锁期间可能有同 id 的重试先行
            cached = self._outcomes.get(batch.batch_id)
            if cached is not None:
                cached.attempt += 1
                return cached

            evidence = self._detect_conflicts(batch)
            conflict = len(evidence) > 0
            self.stats["commits"] += 1
            if conflict:
                self.stats["conflicts"] += 1

            hard_advisory = [i for i in batch.advisory_issues if i.severity == "hard"]

            if self.granularity == GRAN_ROW and not conflict:
                # 缺陷对照路径：无行版本冲突就直接发布，区域额度前提失效也不重校验。
                # 硬违约（负值/方案缺失）仍拦截——它们与并发无关。
                if hard_advisory:
                    outcome = self._reject(
                        batch, REJECTED_MANUAL, hard_advisory, evidence,
                        conflict, False, t0,
                        "输入存在硬性违约，必须人工调整；未发布，事实表不变")
                else:
                    self.store.apply(batch.batch_id, batch.writes)
                    outcome = self._accept(batch, evidence, conflict, False, t0)
                self._outcomes[batch.batch_id] = outcome
                return outcome

            # 正确路径（region 模式，或 row 模式发生行冲突后升级为完整重校验）：
            # 锁内基于当前状态重新执行全部约束，校验通过才 apply —— 原子发布。
            self.stats["revalidations"] += 1
            live_view = LiveView(self.store)
            live_issues, _ = validate(
                live_view, batch.writes,
                batch.snapshot.snapshot_id, batch.base_store_version)

            if not live_issues:
                self.store.apply(batch.batch_id, batch.writes)
                outcome = self._accept(batch, evidence, conflict, True, t0)
                self._outcomes[batch.batch_id] = outcome
                return outcome

            hard = [i for i in live_issues if i.severity == "hard"]
            if hard:
                outcome = self._reject(
                    batch, REJECTED_MANUAL, live_issues, evidence,
                    conflict, True, t0,
                    "重新校验发现硬性违约（负值/方案不存在），必须人工调整输入；"
                    "未发布，事实表不变")
            elif conflict:
                outcome = self._reject(
                    batch, REJECTED_RECOMPUTE, live_issues, evidence,
                    conflict, True, t0,
                    "区域额度前提已被并发提交改变且当前超限；可基于新快照重新计算，"
                    "系统不会自动改小金额")
            else:
                outcome = self._reject(
                    batch, REJECTED_MANUAL, live_issues, evidence,
                    conflict, True, t0,
                    "无并发冲突但批次自身使区域超限，必须人工调整输入；"
                    "未发布，事实表不变")
            self._outcomes[batch.batch_id] = outcome
            return outcome

    # ------------------------------------------------------------------ 结果构造
    def _accept(self, batch: StagedBatch, evidence: list[ConflictEvidence],
                conflict: bool, revalidated: bool, t0: float) -> CommitOutcome:
        return CommitOutcome(
            batch_id=batch.batch_id,
            status=ACCEPTED,
            granularity=self.granularity,
            snapshot_id=batch.snapshot.snapshot_id,
            conflict=conflict,
            revalidated=revalidated,
            reasons=[],
            conflict_evidence=evidence,
            read_set=batch.read_set,
            committed_store_version=self.store.store_version,
            base_store_version=batch.base_store_version,
            elapsed_ms=(time.perf_counter() - t0) * 1000,
            message=("读集合冲突后重校验通过，原子发布" if conflict
                     else "约束校验通过，原子发布"),
        )

    def _reject(self, batch: StagedBatch, status: str,
                reasons: list[ValidationIssue], evidence: list[ConflictEvidence],
                conflict: bool, revalidated: bool, t0: float, message: str) -> CommitOutcome:
        return CommitOutcome(
            batch_id=batch.batch_id,
            status=status,
            granularity=self.granularity,
            snapshot_id=batch.snapshot.snapshot_id,
            conflict=conflict,
            revalidated=revalidated,
            reasons=reasons,
            conflict_evidence=evidence,
            read_set=batch.read_set,
            committed_store_version=None,
            base_store_version=batch.base_store_version,
            elapsed_ms=(time.perf_counter() - t0) * 1000,
            message=message,
        )
