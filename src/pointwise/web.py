"""HTTP 服务：JSON API + 静态前端，仅依赖 Python 标准库。"""
from __future__ import annotations

import json
import threading
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .constraints import StagedBatch
from .engine import Engine, preset_scenarios, seed_data
from .models import (
    WriteItem, Row, RegionCap, CommitOutcome,
    GRAN_REGION, GRAN_ROW, new_batch_id,
)
from .scenarios import ScenarioResult, TimelineEvent
from .benchmark import BenchResult

WEB_ROOT = Path(__file__).resolve().parent.parent.parent / "web"


# ---------------------------------------------------------------------------
# 序列化
# ---------------------------------------------------------------------------

def outcome_dict(o: CommitOutcome) -> dict:
    return o.to_dict()


def scenario_dict(r: ScenarioResult) -> dict:
    return {
        "scenario": r.scenario,
        "granularity": r.granularity,
        "shared_snapshot_id": r.shared_snapshot_id,
        "timeline": [
            {"seq": e.seq, "kind": e.kind, "actor": e.actor,
             "detail": e.detail, "elapsed_ms": round(e.elapsed_ms, 3)}
            for e in r.timeline
        ],
        "outcomes": {k: outcome_dict(o) for k, o in r.outcomes.items()},
        "final_state": r.final_state,
    }


def bench_dict(b: BenchResult) -> dict:
    return asdict(b)


def staged_dict(s: StagedBatch) -> dict:
    return {
        "batch_id": s.batch_id,
        "label": s.label,
        "snapshot_id": s.snapshot.snapshot_id,
        "base_store_version": s.base_store_version,
        "writes": [{"region": w.region, "plan": w.plan,
                    "new_amount": w.new_amount} for w in s.writes],
        "advisory_issues": [asdict(i) for i in s.advisory_issues],
        "read_set": asdict(s.read_set),
        "touched_regions": s.touched_regions(),
    }


# ---------------------------------------------------------------------------
# 请求处理
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    engine: Engine = None  # 由 main 注入

    def log_message(self, fmt, *args):  # 安静一点
        pass

    # ------------------------------------------------------------------ 工具
    def _json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _static(self, rel: str) -> None:
        target = (WEB_ROOT / rel).resolve()
        if not str(target).startswith(str(WEB_ROOT.resolve())) or not target.is_file():
            self.send_error(404)
            return
        ctype = "text/html; charset=utf-8" if target.suffix == ".html" else \
                "application/javascript; charset=utf-8" if target.suffix == ".js" else \
                "text/css; charset=utf-8"
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------ 路由
    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            return self._static("index.html")
        if path == "/app.js":
            return self._static("app.js")
        if path == "/api/state":
            return self._json(200, {
                "state": self.engine.store.state_dict(),
                "granularity": self.engine.coordinator.granularity,
                "stats": self.engine.coordinator.stats,
                "cached_outcomes": [o.batch_id for o in self.engine.coordinator.all_outcomes()],
            })
        if path == "/api/scenarios":
            return self._json(200, {
                "scenarios": [
                    {"id": d.id, "title": d.title, "description": d.description,
                     "specs": [{"batch_id": s.batch_id, "label": s.label,
                                "worker": s.worker,
                                "writes": [{"region": w.region, "plan": w.plan,
                                            "new_amount": w.new_amount} for w in s.writes]}
                               for s in d.specs],
                     "commit_order": d.commit_order, "retries": d.retries}
                    for d in preset_scenarios().values()
                ]
            })
        self.send_error(404)

    def do_POST(self):  # noqa: N802
        path = urlparse(self.path).path
        try:
            data = self._read_json()
            if path == "/api/reset":
                self.engine.reset()
                return self._json(200, {"ok": True, "state": self.engine.store.state_dict()})

            if path == "/api/granularity":
                gran = data.get("granularity", GRAN_REGION)
                if gran not in (GRAN_REGION, GRAN_ROW):
                    return self._json(400, {"error": "granularity 仅支持 region|row"})
                self.engine.coordinator.set_granularity(gran)
                return self._json(200, {"ok": True, "granularity": gran})

            if path == "/api/scenario/run":
                return self._run_scenario(data)

            if path == "/api/stage":
                return self._stage(data)

            if path == "/api/commit":
                return self._commit(data)

            if path == "/api/commit-parallel":
                return self._commit_parallel(data)

            if path == "/api/benchmark":
                return self._benchmark(data)

            self.send_error(404)
        except Exception as exc:  # 面向教学 UI，返回可读错误
            return self._json(500, {"error": f"{type(exc).__name__}: {exc}"})

    # ------------------------------------------------------------------ 业务处理
    def _parse_writes(self, data: dict) -> list[WriteItem]:
        writes = []
        for w in data.get("writes", []):
            amount = w.get("new_amount")
            if not isinstance(amount, int) or isinstance(amount, bool):
                raise ValueError("new_amount 必须为整数（本系统不接受浮点金额）")
            writes.append(WriteItem(w["region"], w["plan"], amount))
        if not writes:
            raise ValueError("writes 不能为空")
        return writes

    def _run_scenario(self, data: dict) -> None:
        scenarios = preset_scenarios()
        scenario = scenarios.get(data.get("scenario"))
        if scenario is None:
            return self._json(400, {"error": f"未知场景 {data.get('scenario')}"})
        gran = data.get("granularity", GRAN_REGION)
        delay = float(data.get("compute_delay", 0.05))
        result = self.engine.replay.run(
            scenario.id, self.engine.seed_rows, self.engine.seed_caps,
            scenario.specs, scenario.commit_order,
            granularity=gran, compute_delay=delay, retries=scenario.retries)
        return self._json(200, {
            "result": scenario_dict(result),
            "state": self.engine.store.state_dict(),
            "stats": self.engine.coordinator.stats,
        })

    def _stage(self, data: dict) -> None:
        writes = self._parse_writes(data)
        batch_id = data.get("batch_id") or new_batch_id()
        label = data.get("label", batch_id)
        snap = self.engine.stager.take_snapshot()
        staged = self.engine.stager.stage(batch_id, writes, snap=snap, label=label)
        return self._json(200, {"staged": staged_dict(staged)})

    def _commit(self, data: dict) -> None:
        staged = self._rehydrate(data)
        outcome = self.engine.coordinator.commit(staged)
        return self._json(200, {
            "outcome": outcome_dict(outcome),
            "state": self.engine.store.state_dict(),
        })

    def _rehydrate(self, data: dict) -> StagedBatch:
        """把 API 请求重建为内存暂存批次（同快照语义由 snapshot_id 承载）。"""
        writes = self._parse_writes(data)
        snap = self.engine.stager.take_snapshot()
        # 允许客户端指定基线版本，用于模拟"从同一旧快照计算"的交错
        base_version = int(data.get("base_store_version", snap.store_version))
        if base_version != snap.store_version:
            # 构造一个历史版本视图：用当前快照但标注旧版本号，
            # 读集合中的行版本由校验器按当前行记录；场景重放才是精确交错的主路径。
            snap.store_version = base_version
        return self.engine.stager.stage(
            data.get("batch_id") or new_batch_id(), writes,
            snap=snap, label=data.get("label", ""))

    def _commit_parallel(self, data: dict) -> None:
        """两个独立工作进程：屏障同步后并发提交同一组已暂存批次。"""
        specs = data.get("batches", [])
        if not specs:
            return self._json(400, {"error": "batches 不能为空"})
        shared = self.engine.stager.take_snapshot()
        staged_list = []
        for s in specs:
            writes = [WriteItem(w["region"], w["plan"], w["new_amount"]) for w in s["writes"]]
            staged_list.append(self.engine.stager.stage(
                s.get("batch_id") or new_batch_id(), writes,
                snap=shared, label=s.get("label", "")))

        barrier = threading.Barrier(len(staged_list))
        results: dict[str, dict] = {}
        lock = threading.Lock()

        def worker(st: StagedBatch) -> None:
            barrier.wait()  # 同时起跑，制造真实交错
            oc = self.engine.coordinator.commit(st)
            with lock:
                results[st.batch_id] = outcome_dict(oc)

        threads = [threading.Thread(target=worker, args=(s,)) for s in staged_list]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return self._json(200, {
            "shared_snapshot_id": shared.snapshot_id,
            "outcomes": results,
            "state": self.engine.store.state_dict(),
            "stats": self.engine.coordinator.stats,
        })

    def _benchmark(self, data: dict) -> None:
        n = int(data.get("n", 20))
        seed = int(data.get("seed", 42))
        pressure = float(data.get("pressure", 0.5))
        delay = float(data.get("compute_delay", 0.02))
        workers = int(data.get("workers", 4))
        gran = data.get("granularity", GRAN_REGION)
        workload = self.engine.benchmark._gen_workload(n, seed, pressure)
        serial_r = self.engine.benchmark.run_serial(workload, gran, delay)
        concurrent_r = self.engine.benchmark.run_concurrent(
            workload, gran, delay, workers=workers,
            auto_retry_recompute=bool(data.get("auto_retry", True)))
        return self._json(200, {
            "serial": bench_dict(serial_r),
            "concurrent": bench_dict(concurrent_r),
            "params": {"n": n, "seed": seed, "pressure": pressure,
                       "compute_delay": delay, "workers": workers,
                       "granularity": gran},
        })


def create_server(host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    Handler.engine = Engine()
    httpd = ThreadingHTTPServer((host, port), Handler)
    return httpd


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="并发批次发布演示服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    httpd = create_server(args.host, args.port)
    print(f"服务已启动: http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
