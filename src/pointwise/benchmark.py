"""吞吐压测：真实线程并发提交 vs 串行基线，使用同一组工作负载对照。

计算延迟（模拟工作进程基于快照的计算耗时）在提交锁之外发生，因此并发模式
可以重叠计算；提交临界区仍由协调器串行化。压测同时统计接受/拒绝分布，
用于展示"吞吐提升"与"冲突代价"的实际差别。
"""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field

from .constraints import Stager, StagedBatch
from .coordinator import CommitCoordinator
from .models import WriteItem, CommitOutcome, GRAN_REGION
from .serial import SerialExecutor
from .store import Store


@dataclass
class BenchResult:
    mode: str
    granularity: str
    n_batches: int
    wall_ms: float
    throughput_per_s: float
    accepted: int
    rejected_recompute: int
    rejected_manual: int
    conflicts: int
    revalidations: int
    retries_succeeded: int = 0
    outcome_ids: list[str] = field(default_factory=list)
    final_version: int = 0


class Benchmark:
    def __init__(self, store: Store, stager: Stager,
                 coordinator: CommitCoordinator) -> None:
        self.store = store
        self.stager = stager
        self.coordinator = coordinator

    def _gen_workload(self, n: int, seed: int, pressure: float) -> list[list[WriteItem]]:
        rng = random.Random(seed)
        regions = sorted({r.region for r in self.store.deep_copy_seed()[0]})
        workload: list[list[WriteItem]] = []
        for i in range(n):
            region = regions[i % len(regions)]
            members = [r for r in self.store.live_region_rows(region)]
            plans = rng.sample([r.plan for r in members],
                               k=min(len(members), rng.randint(1, 2)))
            writes = []
            for plan in plans:
                cap_obj = self.store.live_cap(region)
                # pressure 比例的批次写入明显偏大的金额，制造额度压力
                if rng.random() < pressure:
                    amount = rng.randint(cap_obj.cap // 2, cap_obj.cap)
                else:
                    amount = rng.randint(5, max(6, cap_obj.cap // 4))
                writes.append(WriteItem(region, plan, amount))
            workload.append(writes)
        return workload

    def _stage_all(self, workload: list[list[WriteItem]], prefix: str,
                   shared_snapshot: bool) -> list[StagedBatch]:
        if shared_snapshot:
            snap = self.stager.take_snapshot()
            snaps = [snap] * len(workload)
        else:
            snaps = [self.stager.take_snapshot() for _ in workload]
        return [
            self.stager.stage(f"{prefix}-{i:03d}", writes, snap=snap,
                              label=f"{prefix}-{i:03d}")
            for i, (writes, snap) in enumerate(zip(workload, snaps))
        ]

    @staticmethod
    def _summarize(mode: str, granularity: str, n: int, t0: float,
                   outcomes: list[CommitOutcome], conflicts: int,
                   revalidations: int, retries_ok: int, final_version: int) -> BenchResult:
        wall = (time.perf_counter() - t0) * 1000
        return BenchResult(
            mode=mode, granularity=granularity, n_batches=n, wall_ms=round(wall, 2),
            throughput_per_s=round(n / max(wall / 1000, 1e-9), 2),
            accepted=sum(o.status == "accepted" for o in outcomes),
            rejected_recompute=sum(o.status == "rejected_recompute" for o in outcomes),
            rejected_manual=sum(o.status == "rejected_manual" for o in outcomes),
            conflicts=conflicts, revalidations=revalidations,
            retries_succeeded=retries_ok,
            outcome_ids=[o.batch_id for o in outcomes],
            final_version=final_version,
        )

    def run_serial(self, workload: list[list[WriteItem]], granularity: str,
                   compute_delay: float) -> BenchResult:
        seed_rows, seed_caps = self.store.deep_copy_seed()
        self.store.reset(seed_rows, seed_caps)
        self.coordinator.reset()
        self.coordinator.set_granularity(granularity)
        batches = self._stage_all(workload, "ser", shared_snapshot=True)
        t0 = time.perf_counter()
        outcomes = SerialExecutor(self.coordinator).run(batches, compute_delay)
        return self._summarize("serial", granularity, len(batches), t0, outcomes,
                               self.coordinator.stats["conflicts"],
                               self.coordinator.stats["revalidations"], 0,
                               self.store.store_version)

    def run_concurrent(self, workload: list[list[WriteItem]], granularity: str,
                       compute_delay: float, workers: int = 4,
                       auto_retry_recompute: bool = True) -> BenchResult:
        seed_rows, seed_caps = self.store.deep_copy_seed()
        self.store.reset(seed_rows, seed_caps)
        self.coordinator.reset()
        self.coordinator.set_granularity(granularity)
        batches = self._stage_all(workload, "con", shared_snapshot=True)
        outcomes: list[CommitOutcome] = []
        out_lock = threading.Lock()
        retries_ok = 0

        def worker(batch: StagedBatch) -> None:
            nonlocal retries_ok
            if compute_delay:
                time.sleep(compute_delay)  # 锁外计算，可与其他进程重叠
            outcome = self.coordinator.commit(batch)
            with out_lock:
                outcomes.append(outcome)
            # 可重算批次自动基于新快照重试（金额不变）；manual 批次绝不自动处理
            if auto_retry_recompute and outcome.status == "rejected_recompute":
                spec_writes = batch.writes
                attempt = 0
                while attempt < 2:
                    attempt += 1
                    fresh = self.stager.take_snapshot()
                    re_id = f"{batch.batch_id}-r{attempt}"
                    re_batch = self.stager.stage(re_id, spec_writes, snap=fresh)
                    re_outcome = self.coordinator.commit(re_batch)
                    with out_lock:
                        outcomes.append(re_outcome)
                    if re_outcome.status == "accepted":
                        with out_lock:
                            retries_ok += 1
                        break
                    if re_outcome.status == "rejected_manual":
                        break

        t0 = time.perf_counter()
        threads = []
        # 用信号量限制并发工作进程数，但所有批次同时排队、交错进入提交
        sem = threading.Semaphore(workers)

        def bounded(b: StagedBatch) -> None:
            with sem:
                worker(b)

        for b in batches:
            th = threading.Thread(target=bounded, args=(b,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()
        return self._summarize("concurrent", granularity, len(batches), t0, outcomes,
                               self.coordinator.stats["conflicts"],
                               self.coordinator.stats["revalidations"], retries_ok,
                               self.store.store_version)
