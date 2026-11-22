"""核心数据模型：事实行、区域上限、快照、写项、谓词读集合、校验问题与提交结果。

金额一律使用整数单位，避免浮点求和误差。
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


# ---------------------------------------------------------------------------
# 存储层模型
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RowKey:
    region: str
    plan: str


@dataclass
class Row:
    """资源分配事实行，amount 为绝对金额，version 随每次写入递增。"""
    region: str
    plan: str
    amount: int
    version: int

    def key(self) -> RowKey:
        return RowKey(self.region, self.plan)

    def clone(self) -> "Row":
        return Row(self.region, self.plan, self.amount, self.version)


@dataclass
class RegionCap:
    """区域可分配总量上限，自身也带版本（本系统上限静态，但版本仍被读集合记录）。"""
    region: str
    cap: int
    version: int

    def clone(self) -> "RegionCap":
        return RegionCap(self.region, self.cap, self.version)


@dataclass
class Snapshot:
    """某一时刻的整库一致性快照，工作进程只基于快照做计算。"""
    snapshot_id: int
    store_version: int
    rows: dict[RowKey, Row]
    caps: dict[str, RegionCap]
    taken_at: float

    def clone_rows(self) -> dict[RowKey, Row]:
        return {k: r.clone() for k, r in self.rows.items()}


# ---------------------------------------------------------------------------
# 写入与读集合
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WriteItem:
    """对单个方案行的更正：把 amount 绝对改写为 new_amount。"""
    region: str
    plan: str
    new_amount: int

    def key(self) -> RowKey:
        return RowKey(self.region, self.plan)


@dataclass(frozen=True)
class RegionRead:
    """一次区域求和约束实际读取的谓词数据。

    谓词文本：SUM(amount) WHERE region = :region <= cap(:region)
    任何成员行的版本/成员关系、上限版本发生变化，都意味着判断前提已失效。
    """
    region: str
    predicate: str
    plans: tuple[str, ...]
    row_versions: tuple[int, ...]
    row_amounts: tuple[int, ...]
    cap: int
    cap_version: int


@dataclass
class ReadSet:
    """校验器实际读取过的全部数据及版本，提交端据此判断是否需要重校验。"""
    snapshot_id: int
    snapshot_store_version: int
    region_reads: list[RegionRead] = field(default_factory=list)
    written_row_versions: dict[str, int] = field(default_factory=dict)  # "region|plan" -> version
    plan_dim_version: int = 0  # 方案维度表版本（本系统维度静态，恒为 0）

    def touched_regions(self) -> list[str]:
        return [r.region for r in self.region_reads]


@dataclass
class ValidationIssue:
    """单条约束违约。severity: hard=输入本身非法；capacity=区域额度类。"""
    code: str  # negative_amount | plan_not_found | region_not_found | region_sum_exceeded
    severity: str  # hard | capacity
    message: str
    region: Optional[str] = None
    plan: Optional[str] = None
    detail: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 冲突证据与提交结果
# ---------------------------------------------------------------------------

@dataclass
class ConflictEvidence:
    """读集合与当前已提交状态之间的具体差异，供网页展示"冲突依据"。"""
    kind: str  # row_version | region_membership | region_row_version | cap_version
    region: str
    plan: Optional[str]
    expected: object
    actual: object
    committed_by: list[str] = field(default_factory=list)  # 造成差异的已提交批次

    def to_dict(self) -> dict:
        return asdict(self)


# 提交结果状态码
ACCEPTED = "accepted"
REJECTED_RECOMPUTE = "rejected_recompute"  # 可基于新快照重新计算
REJECTED_MANUAL = "rejected_manual"        # 必须人工调整输入

# 校验粒度
GRAN_REGION = "region"  # 区域谓词读集合 + 行版本（可串行化，正确模式）
GRAN_ROW = "row"        # 仅行版本（故意保留的缺陷对照模式）


@dataclass
class CommitOutcome:
    batch_id: str
    status: str
    granularity: str
    snapshot_id: int
    conflict: bool
    revalidated: bool
    reasons: list[ValidationIssue]
    conflict_evidence: list[ConflictEvidence]
    read_set: Optional[ReadSet]
    committed_store_version: Optional[int]
    base_store_version: int
    elapsed_ms: float
    attempt: int = 1
    retry_of: Optional[str] = None
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "granularity": self.granularity,
            "snapshot_id": self.snapshot_id,
            "conflict": self.conflict,
            "revalidated": self.revalidated,
            "reasons": [asdict(r) for r in self.reasons],
            "conflict_evidence": [e.to_dict() for e in self.conflict_evidence],
            "read_set": read_set_to_dict(self.read_set) if self.read_set else None,
            "committed_store_version": self.committed_store_version,
            "base_store_version": self.base_store_version,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "attempt": self.attempt,
            "retry_of": self.retry_of,
            "message": self.message,
        }


def read_set_to_dict(rs: ReadSet) -> dict:
    return {
        "snapshot_id": rs.snapshot_id,
        "snapshot_store_version": rs.snapshot_store_version,
        "plan_dim_version": rs.plan_dim_version,
        "written_row_versions": dict(rs.written_row_versions),
        "region_reads": [asdict(r) for r in rs.region_reads],
    }


def new_batch_id() -> str:
    return "b-" + uuid.uuid4().hex[:10]


def now_ms() -> float:
    return time.perf_counter()


def rk(region: str, plan: str) -> str:
    return f"{region}|{plan}"
