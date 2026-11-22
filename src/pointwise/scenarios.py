"""确定性场景重放：两个工作进程从同一快照暂存两批更正，按指定顺序提交。

不依赖真实线程即可复现交错结果——两批在提交前都已基于同一快照暂存，
最终裁决只取决于提交顺序与校验粒度。时间线中的延迟为真实 sleep（可配），
用于让网页呈现实际耗时差异。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from .constraints import StagedBatch, Stager
from .coordinator import CommitCoordinator
from .models import (
    WriteItem, CommitOutcome, Snapshot,
    GRAN_REGION, GRAN_ROW,
)
from .store import Store


@dataclass
class BatchSpec:
    batch_id: str
    label: str
    worker: str
    writes: list[WriteItem]


@dataclass
class TimelineEvent:
    seq: int
    kind: str          # reset | snapshot | stage | commit | retry
    actor: str
    detail: dict
    elapsed_ms: float = 0.0


@dataclass
class ScenarioResult:
    scenario: str
    granularity: str
    shared_snapshot_id: int
    timeline: list[TimelineEvent] = field(default_factory=list)
    outcomes: dict[str, CommitOutcome] = field(default_factory=dict)
    final_state: dict = field(default_factory=dict)


class ReplayEngine:
    def __init__(self, store: Store, stager: Stager,
                 coordinator: CommitCoordinator) -> None:
        self.store = store
        self.stager = stager
        self.coordinator = coordinator

    def run(self, scenario_id: str, seed_rows, seed_caps, specs: list[BatchSpec],
            commit_order: list[str], granularity: str = GRAN_REGION,
            compute_delay: float = 0.05,
            retries: dict[str, int] | None = None) -> ScenarioResult:
        retries = retries or {}
        self.store.reset(seed_rows, seed_caps)
        self.coordinator.reset()
        self.coordinator.set_granularity(granularity)

        result = ScenarioResult(scenario=scenario_id, granularity=granularity,
                                shared_snapshot_id=-1)
        seq = 0

        def emit(kind: str, actor: str, detail: dict, elapsed: float = 0.0) -> None:
            nonlocal seq
            seq += 1
            result.timeline.append(TimelineEvent(seq, kind, actor, detail, elapsed))

        emit("reset", "system", {"granularity": granularity})

        # 两个独立工作进程从同一个快照计算
        shared: Snapshot = self.stager.take_snapshot()
        result.shared_snapshot_id = shared.snapshot_id
        emit("snapshot", "workers",
             {"snapshot_id": shared.snapshot_id, "store_version": shared.store_version})

        staged: dict[str, StagedBatch] = {}
        for spec in specs:
            t0 = time.perf_counter()
            if compute_delay:
                time.sleep(compute_delay)
            batch = self.stager.stage(spec.batch_id, spec.writes,
                                      snap=shared, label=spec.label)
            staged[spec.batch_id] = batch
            emit("stage", spec.worker, {
                "batch_id": spec.batch_id,
                "label": spec.label,
                "writes": [{"region": w.region, "plan": w.plan,
                            "new_amount": w.new_amount} for w in spec.writes],
                "advisory_issues": [i.__dict__ for i in batch.advisory_issues],
                "read_regions": batch.touched_regions(),
            }, (time.perf_counter() - t0) * 1000)

        # 按提交顺序发布；失败重试使用新快照（模拟重算，绝不改小金额）
        retry_counts: dict[str, int] = {}
        for batch_id in commit_order:
            spec = next(s for s in specs if s.batch_id == batch_id)
            current = staged[batch_id]
            t0 = time.perf_counter()
            outcome = self.coordinator.commit(current)
            emit("commit", spec.worker, {
                "batch_id": batch_id,
                "status": outcome.status,
                "conflict": outcome.conflict,
                "revalidated": outcome.revalidated,
                "attempt": outcome.attempt,
                "message": outcome.message,
                "reasons": [i.__dict__ for i in outcome.reasons],
                "evidence": [e.to_dict() for e in outcome.conflict_evidence],
            }, (time.perf_counter() - t0) * 1000)
            result.outcomes[batch_id] = outcome

            for attempt_idx in range(retries.get(batch_id, 0)):
                if outcome.status != "rejected_recompute":
                    break
                retry_counts[batch_id] = retry_counts.get(batch_id, 0) + 1
                new_id = f"{batch_id}-r{retry_counts[batch_id]}"
                fresh = self.stager.take_snapshot()
                re_staged = self.stager.stage(new_id, spec.writes, snap=fresh,
                                               label=f"{spec.label} 重算{retry_counts[batch_id]}")
                emit("retry", spec.worker, {
                    "batch_id": new_id, "retry_of": batch_id,
                    "from_snapshot": shared.snapshot_id,
                    "fresh_snapshot": fresh.snapshot_id,
                    "note": "基于新快照重新计算读集合，写入金额保持不变（不自动凑上限）",
                })
                t0 = time.perf_counter()
                outcome = self.coordinator.commit(re_staged)
                emit("commit", spec.worker, {
                    "batch_id": new_id, "retry_of": batch_id,
                    "status": outcome.status, "conflict": outcome.conflict,
                    "revalidated": outcome.revalidated,
                    "message": outcome.message,
                    "reasons": [i.__dict__ for i in outcome.reasons],
                }, (time.perf_counter() - t0) * 1000)
                result.outcomes[new_id] = outcome

        result.final_state = self.store.state_dict()
        return result
