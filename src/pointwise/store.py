"""内存事实表存储：行版本、整库版本、快照、事件历史与提交应用。

所有 mutating 操作仅由提交协调器在提交锁内调用；快照读取本身无锁、
基于 copy-on-read 保证工作进程拿到不可变视图。
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass

from .models import (
    Row, RegionCap, Snapshot, RowKey, WriteItem, rk,
)


@dataclass
class CommitEvent:
    """已提交批次在事实表上留下的历史，用于冲突归因（committed_by）。"""
    batch_id: str
    store_version_after: int
    changed_keys: list[str]
    region: list[str]
    at: float


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rows: dict[RowKey, Row] = {}
        self._caps: dict[str, RegionCap] = {}
        self._store_version = 0
        self._snapshot_seq = 0
        self._events: list[CommitEvent] = []

    # ------------------------------------------------------------------ 装载
    def load(self, rows: list[Row], caps: list[RegionCap]) -> None:
        with self._lock:
            self._rows = {(r.region, r.plan): r.clone() for r in rows}
            self._caps = {c.region: c.clone() for c in caps}
            self._store_version = 0
            self._snapshot_seq = 0
            self._events = []

    def reset(self, rows: list[Row], caps: list[RegionCap]) -> None:
        self.load(rows, caps)

    # ------------------------------------------------------------------ 快照
    def snapshot(self) -> Snapshot:
        with self._lock:
            self._snapshot_seq += 1
            snap = Snapshot(
                snapshot_id=self._snapshot_seq,
                store_version=self._store_version,
                rows={k: r.clone() for k, r in self._rows.items()},
                caps={k: c.clone() for k, c in self._caps.items()},
                taken_at=time.time(),
            )
            return snap

    # ------------------------------------------------------------------ 实时读取（提交锁内使用）
    def live_row(self, region: str, plan: str) -> Row | None:
        return self._rows.get((region, plan))

    def live_region_rows(self, region: str) -> list[Row]:
        return [r.clone() for r in self._rows.values() if r.region == region]

    def live_cap(self, region: str) -> RegionCap | None:
        return self._caps.get(region)

    @property
    def store_version(self) -> int:
        return self._store_version

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------ 提交应用
    def apply(self, batch_id: str, writes: list[WriteItem]) -> CommitEvent:
        """仅在提交锁内调用。版本号先增再写，保证"已发布"与新版本原子可见。"""
        changed: list[str] = []
        regions: list[str] = []
        for w in writes:
            key = (w.region, w.plan)
            row = self._rows[key]  # 方案存在性已由校验保证
            row.amount = w.new_amount
            row.version += 1
            changed.append(rk(w.region, w.plan))
            if w.region not in regions:
                regions.append(w.region)
        self._store_version += 1
        evt = CommitEvent(
            batch_id=batch_id,
            store_version_after=self._store_version,
            changed_keys=changed,
            region=regions,
            at=time.time(),
        )
        self._events.append(evt)
        return evt

    def events(self) -> list[CommitEvent]:
        with self._lock:
            return list(self._events)

    def events_touching(self, region: str) -> list[CommitEvent]:
        with self._lock:
            return [e for e in self._events if region in e.region]

    # ------------------------------------------------------------------ 查询
    def state_dict(self) -> dict:
        with self._lock:
            rows = [
                {"region": r.region, "plan": r.plan, "amount": r.amount, "version": r.version}
                for r in sorted(self._rows.values(), key=lambda x: (x.region, x.plan))
            ]
            caps = [
                {"region": c.region, "cap": c.cap, "version": c.version}
                for c in sorted(self._caps.values(), key=lambda x: x.region)
            ]
            return {
                "store_version": self._store_version,
                "rows": rows,
                "caps": caps,
                "events": [
                    {
                        "batch_id": e.batch_id,
                        "store_version_after": e.store_version_after,
                        "changed_keys": e.changed_keys,
                        "regions": e.region,
                    }
                    for e in self._events
                ],
            }

    def deep_copy_seed(self) -> tuple[list[Row], list[RegionCap]]:
        with self._lock:
            return (
                [r.clone() for r in self._rows.values()],
                [c.clone() for c in self._caps.values()],
            )
