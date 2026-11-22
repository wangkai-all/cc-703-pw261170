"""暂存器与约束校验器。

只支持三类业务约束（不承诺分析任意 SQL）：
  1. region_sum_exceeded —— 按区域求和不得超过区域可分配总量上限；
  2. negative_amount     —— 更正金额必须非负；
  3. plan_not_found      —— 方案行（及区域）必须存在。

校验器在评估约束的同时构建 *谓词读集合*：区域谓词 SUM(amount) WHERE region=?
读取了哪些成员行的什么版本、上限的什么版本、写入行基于什么版本。
提交端据此判断已提交修改是否会改变这些判断的前提。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import (
    Snapshot, Row, RegionCap, WriteItem, ReadSet, RegionRead,
    ValidationIssue, rk,
)
from .store import Store


class View(Protocol):
    """统一只读视图：快照视图用于工作进程，实时视图用于提交锁内重校验。"""
    def get_row(self, region: str, plan: str) -> Row | None: ...
    def region_rows(self, region: str) -> list[Row]: ...
    def get_cap(self, region: str) -> RegionCap | None: ...
    @property
    def plan_dim_version(self) -> int: ...


class SnapshotView:
    def __init__(self, snap: Snapshot):
        self.snap = snap

    def get_row(self, region: str, plan: str) -> Row | None:
        row = self.snap.rows.get((region, plan))
        return row.clone() if row else None

    def region_rows(self, region: str) -> list[Row]:
        return [r.clone() for r in self.snap.rows.values() if r.region == region]

    def get_cap(self, region: str) -> RegionCap | None:
        cap = self.snap.caps.get(region)
        return cap.clone() if cap else None

    @property
    def plan_dim_version(self) -> int:
        return 0


class LiveView:
    """提交锁内的实时视图。必须在 Store.lock 保护下构造与使用。"""
    def __init__(self, store: Store):
        self.store = store

    def get_row(self, region: str, plan: str) -> Row | None:
        return self.store.live_row(region, plan)

    def region_rows(self, region: str) -> list[Row]:
        return self.store.live_region_rows(region)

    def get_cap(self, region: str) -> RegionCap | None:
        return self.store.live_cap(region)

    @property
    def plan_dim_version(self) -> int:
        return 0


# ---------------------------------------------------------------------------
# 校验 + 读集合构建
# ---------------------------------------------------------------------------

def validate(view: View, writes: list[WriteItem], snapshot_id: int,
             snapshot_store_version: int) -> tuple[list[ValidationIssue], ReadSet]:
    """对写集合执行三类约束校验，并返回完整谓词读集合。"""
    issues: list[ValidationIssue] = []
    read_set = ReadSet(snapshot_id=snapshot_id,
                       snapshot_store_version=snapshot_store_version)

    touched_regions: list[str] = []
    for w in writes:
        if w.region not in touched_regions:
            touched_regions.append(w.region)

    # --- 约束 2/3：方案存在性、区域存在性、非负（硬约束，输入本身非法）-------
    write_by_key: dict[tuple[str, str], WriteItem] = {}
    for w in writes:
        write_by_key[(w.region, w.plan)] = w
        row = view.get_row(w.region, w.plan)
        if not isinstance(w.new_amount, int) or isinstance(w.new_amount, bool):
            issues.append(ValidationIssue(
                "negative_amount", "hard",
                f"方案 {w.region}/{w.plan} 的金额必须是整数",
                w.region, w.plan, {"value": repr(w.new_amount)}))
            continue
        if row is None:
            cap = view.get_cap(w.region)
            code = "region_not_found" if cap is None else "plan_not_found"
            issues.append(ValidationIssue(
                code, "hard",
                f"区域 {w.region} 内不存在方案 {w.plan}",
                w.region, w.plan))
            continue
        read_set.written_row_versions[rk(w.region, w.plan)] = row.version
        if w.new_amount < 0:
            issues.append(ValidationIssue(
                "negative_amount", "hard",
                f"方案 {w.region}/{w.plan} 的更正金额 {w.new_amount} 为负",
                w.region, w.plan, {"value": w.new_amount}))

    # --- 约束 1：按区域求和 <= 区域上限（额度类，谓词读集合的核心）-----------
    for region in touched_regions:
        cap_obj = view.get_cap(region)
        members = sorted(view.region_rows(region), key=lambda r: r.plan)
        plans = tuple(r.plan for r in members)
        row_versions = tuple(r.version for r in members)
        row_amounts = tuple(r.amount for r in members)

        if cap_obj is not None:
            # 谓词：SUM(amount) WHERE region=:region <= cap(:region)
            proposed = 0
            for r in members:
                w = write_by_key.get((r.region, r.plan))
                proposed += w.new_amount if w is not None else r.amount
            # 写集合里若包含该区域中不存在的方案，已在上面记 hard issue，此处不纳入求和
            if proposed > cap_obj.cap:
                issues.append(ValidationIssue(
                    "region_sum_exceeded", "capacity",
                    f"区域 {region} 更正后合计 {proposed} 超过上限 {cap_obj.cap}",
                    region, None,
                    {"sum": proposed, "cap": cap_obj.cap,
                     "headroom": cap_obj.cap - proposed}))
            read_set.region_reads.append(RegionRead(
                region=region,
                predicate=f"SUM(amount) WHERE region='{region}' <= cap('{region}')",
                plans=plans,
                row_versions=row_versions,
                row_amounts=row_amounts,
                cap=cap_obj.cap,
                cap_version=cap_obj.version,
            ))
        else:
            # 区域不存在：记录一个空成员谓词，hard issue 已在上面登记
            read_set.region_reads.append(RegionRead(
                region=region,
                predicate=f"SUM(amount) WHERE region='{region}' <= cap('{region}') [cap missing]",
                plans=plans, row_versions=row_versions, row_amounts=row_amounts,
                cap=-1, cap_version=-1,
            ))

    read_set.plan_dim_version = view.plan_dim_version
    return issues, read_set


# ---------------------------------------------------------------------------
# 暂存批次
# ---------------------------------------------------------------------------

@dataclass
class StagedBatch:
    batch_id: str
    label: str
    snapshot: Snapshot
    writes: list[WriteItem]
    base_store_version: int
    advisory_issues: list[ValidationIssue]
    read_set: ReadSet
    staged_at: float

    def touched_regions(self) -> list[str]:
        return self.read_set.touched_regions()


class Stager:
    """工作进程侧：从同一快照计算更正并暂存（暂存不修改事实表）。"""
    def __init__(self, store: Store):
        self.store = store

    def take_snapshot(self) -> Snapshot:
        return self.store.snapshot()

    def stage(self, batch_id: str, writes: list[WriteItem],
              snap: Snapshot | None = None, label: str = "") -> StagedBatch:
        snap = snap or self.take_snapshot()
        view = SnapshotView(snap)
        issues, read_set = validate(view, writes, snap.snapshot_id, snap.store_version)
        import time
        return StagedBatch(
            batch_id=batch_id,
            label=label or batch_id,
            snapshot=snap,
            writes=list(writes),
            base_store_version=snap.store_version,
            advisory_issues=issues,
            read_set=read_set,
            staged_at=time.time(),
        )
