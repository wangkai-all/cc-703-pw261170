"""服务门面：统一持有存储、暂存器、协调器，预置种子数据与教学场景。"""
from __future__ import annotations

from dataclasses import dataclass

from .benchmark import Benchmark
from .constraints import Stager
from .coordinator import CommitCoordinator
from .models import (
    Row, RegionCap, WriteItem,
    GRAN_REGION, GRAN_ROW,
)
from .scenarios import BatchSpec, ReplayEngine
from .store import Store


# ---------------------------------------------------------------------------
# 默认种子数据：两区域、每区三方案
# ---------------------------------------------------------------------------

def seed_data() -> tuple[list[Row], list[RegionCap]]:
    rows = [
        Row("R1", "P101", 40, 1),
        Row("R1", "P102", 30, 1),
        Row("R1", "P103", 10, 1),   # R1 合计 80 / 上限 100，余量 20
        Row("R2", "P201", 25, 1),
        Row("R2", "P202", 15, 1),
        Row("R2", "P203", 10, 1),   # R2 合计 50 / 上限 60，余量 10
    ]
    caps = [RegionCap("R1", 100, 1), RegionCap("R2", 60, 1)]
    return rows, caps


# ---------------------------------------------------------------------------
# 预置教学场景
# ---------------------------------------------------------------------------

@dataclass
class ScenarioDef:
    id: str
    title: str
    description: str
    specs: list[BatchSpec]
    commit_order: list[str]
    retries: dict[str, int]


def preset_scenarios() -> dict[str, ScenarioDef]:
    return {
        "phantom_sum": ScenarioDef(
            id="phantom_sum",
            title="幻影超额：行版本无冲突，区域合计越限（核心场景）",
            description=(
                "R1 上限 100、当前合计 80。批次A 改 P101→60，批次B 改 P102→45，"
                "各自单独提交都合法（90 与 95），且写入行互不重叠、行版本无冲突；"
                "但两批都依赖同一区域余量 20，合起来合计 115 越限。"
                "区域谓词读集合能识别前提变化并要求B重算；仅行版本模式会错误地双接受。"
            ),
            specs=[
                BatchSpec("A", "批次A·工作进程1", "worker-1",
                          [WriteItem("R1", "P101", 60)]),
                BatchSpec("B", "批次B·工作进程2", "worker-2",
                          [WriteItem("R1", "P102", 45)]),
            ],
            commit_order=["A", "B"],
            retries={},
        ),
        "same_row": ScenarioDef(
            id="same_row",
            title="同行覆盖：经典行版本写-写冲突",
            description=(
                "两个工作进程同时更正 P201：A 改为 30，B 改为 33。B 基于的行版本"
                "已被 A 推翻，两种粒度都能检测；B 在新状态下重校验（合计 53<=60）"
                "通过后接受。"
            ),
            specs=[
                BatchSpec("A", "批次A·工作进程1", "worker-1",
                          [WriteItem("R2", "P201", 30)]),
                BatchSpec("B", "批次B·工作进程2", "worker-2",
                          [WriteItem("R2", "P201", 33)]),
            ],
            commit_order=["A", "B"],
            retries={},
        ),
        "hard_reject": ScenarioDef(
            id="hard_reject",
            title="硬性违约：负值与方案不存在，必须人工介入",
            description=(
                "批次A 含负金额，批次B 引用不存在的方案 P999。两类问题与并发无关，"
                "任何模式下都拒绝且不发布；重算不会自愈，系统也不会自动改小金额。"
            ),
            specs=[
                BatchSpec("A", "批次A·负值", "worker-1",
                          [WriteItem("R1", "P103", -5)]),
                BatchSpec("B", "批次B·幽灵方案", "worker-2",
                          [WriteItem("R1", "P999", 10)]),
            ],
            commit_order=["A", "B"],
            retries={},
        ),
        "capacity_manual": ScenarioDef(
            id="capacity_manual",
            title="无冲突超限：输入本身过大，归类为人工调整",
            description=(
                "单批次把 P101 改为 95，R1 合计将达 135>100。没有任何并发提交，"
                "不存在可重算的新前提——这是输入错误，归类 rejected_manual。"
            ),
            specs=[
                BatchSpec("A", "批次A·超额输入", "worker-1",
                          [WriteItem("R1", "P101", 95)]),
            ],
            commit_order=["A"],
            retries={},
        ),
        "retry_recompute": ScenarioDef(
            id="retry_recompute",
            title="重算恢复：幻影超额后基于新快照自动重试",
            description=(
                "与 phantom_sum 相同的两批，B 被拒绝后基于新快照重新计算（金额不变），"
                "此时 R1 余量已变化，新读集合重校验仍为 115>100——演示系统如实"
                "报告仍需人工调整，而不是偷偷改小金额凑过上限。"
            ),
            specs=[
                BatchSpec("A", "批次A·工作进程1", "worker-1",
                          [WriteItem("R1", "P101", 60)]),
                BatchSpec("B", "批次B·工作进程2", "worker-2",
                          [WriteItem("R1", "P102", 45)]),
            ],
            commit_order=["A", "B"],
            retries={"B": 1},
        ),
        "lost_response": ScenarioDef(
            id="lost_response",
            title="响应丢失重试：幂等返回同一发布结果",
            description=(
                "批次A 已接受但响应丢失，客户端以同一 batch_id 重试。协调器命中"
                "结果缓存，返回同一 accepted 结果与同一发布版本，绝不重复发布。"
            ),
            specs=[
                BatchSpec("A", "批次A·丢失响应", "worker-1",
                          [WriteItem("R1", "P103", 15)]),
            ],
            commit_order=["A", "A"],
            retries={},
        ),
        "cross_region": ScenarioDef(
            id="cross_region",
            title="跨区域独立：互不相关的批次无冲突并发通过",
            description=(
                "A 改 R1 的 P101，B 改 R2 的 P202，读集合区域不相交，无冲突、"
                "无需重校验，两批均直接接受。"
            ),
            specs=[
                BatchSpec("A", "批次A·R1", "worker-1",
                          [WriteItem("R1", "P101", 45)]),
                BatchSpec("B", "批次B·R2", "worker-2",
                          [WriteItem("R2", "P202", 20)]),
            ],
            commit_order=["A", "B"],
            retries={},
        ),
    }


class Engine:
    def __init__(self) -> None:
        self.store = Store()
        rows, caps = seed_data()
        self.store.load(rows, caps)
        self.seed_rows, self.seed_caps = rows, caps
        self.stager = Stager(self.store)
        self.coordinator = CommitCoordinator(self.store, GRAN_REGION)
        self.replay = ReplayEngine(self.store, self.stager, self.coordinator)
        self.benchmark = Benchmark(self.store, self.stager, self.coordinator)

    def reset(self) -> None:
        rows, caps = seed_data()
        self.seed_rows, self.seed_caps = rows, caps
        self.store.reset(rows, caps)
        self.coordinator.reset()
        self.coordinator.set_granularity(GRAN_REGION)
