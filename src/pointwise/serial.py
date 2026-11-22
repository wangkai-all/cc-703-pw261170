"""串行执行基线：同一套存储与协调器，批次严格按顺序提交。

作为正确性对照：并发协调器（region 粒度）在任意交错下的最终事实表，
必须与该串行基线按某个提交顺序得到的结果一致。
"""
from __future__ import annotations

import time

from .constraints import StagedBatch
from .coordinator import CommitCoordinator
from .models import CommitOutcome


class SerialExecutor:
    def __init__(self, coordinator: CommitCoordinator) -> None:
        self.coordinator = coordinator

    def run(self, batches: list[StagedBatch],
            compute_delay: float = 0.0) -> list[CommitOutcome]:
        outcomes: list[CommitOutcome] = []
        for batch in batches:
            if compute_delay > 0:
                time.sleep(compute_delay)  # 串行下计算延迟完整累加
            outcomes.append(self.coordinator.commit(batch))
        return outcomes
