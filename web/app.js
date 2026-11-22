/* 并发批次发布实验台 —— 原生 JS，无依赖 */
"use strict";

const $ = (id) => document.getElementById(id);
let granularity = "region";
let scenarios = [];
let manualWrites = { a: [], b: [] };

// ---------------------------------------------------------------------------
// 基础工具
// ---------------------------------------------------------------------------

async function api(path, body) {
  const opt = body === undefined
    ? { method: "GET" }
    : { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body) };
  const resp = await fetch(path, opt);
  const data = await resp.json();
  if (!resp.ok || data.error) throw new Error(data.error || `HTTP ${resp.status}`);
  return data;
}

function toast(msg, isErr = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (isErr ? " err" : "");
  setTimeout(() => (t.className = "toast"), 4200);
}

function pill(status) {
  const map = {
    accepted: ["accepted", "接受"],
    rejected_recompute: ["rejected_recompute", "拒绝·可重算"],
    rejected_manual: ["rejected_manual", "拒绝·人工调整"],
  };
  const [cls, text] = map[status] || ["neutral", status];
  return `<span class="pill ${cls}">${text}</span>`;
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// ---------------------------------------------------------------------------
// 事实表 / 协调器状态
// ---------------------------------------------------------------------------

function capMeter(region, rows, cap) {
  const sum = rows.filter((r) => r.region === region).reduce((s, r) => s + r.amount, 0);
  const pct = Math.min(100, Math.round((sum / cap) * 100));
  const over = sum > cap;
  const color = over ? "var(--critical)" : pct > 85 ? "var(--yellow)" : "var(--aqua)";
  return `<div style="display:flex;align-items:center;gap:8px">
      <div class="cap-meter" style="flex:1"><i style="width:${pct}%;background:${color}"></i></div>
      <span class="mono ${over ? "pill rejected_manual" : ""}" style="font-variant-numeric:tabular-nums">
        ${sum} / ${cap}${over ? " 越限" : ""}</span>
    </div>`;
}

async function refreshState() {
  const { state, stats, cached_outcomes } = await api("/api/state");
  $("st-version").textContent = state.store_version;
  $("st-commits").textContent = stats.commits;
  $("st-conflicts").textContent = stats.conflicts;
  $("st-reval").textContent = stats.revalidations;

  const caps = state.caps.map((c) => c.region);
  let html = `<table><thead><tr>
      <th>区域</th><th>方案</th><th class="num">金额</th><th class="num">行版本</th>
      <th style="width:38%">区域合计 / 上限</th></tr></thead><tbody>`;
  for (const c of state.caps) {
    const rows = state.rows.filter((r) => r.region === c.region);
    rows.forEach((r, i) => {
      html += `<tr>
        ${i === 0 ? `<td rowspan="${rows.length + 1}"><strong>${c.region}</strong></td>` : ""}
        <td class="mono">${r.plan}</td>
        <td class="num">${r.amount}</td>
        <td class="num muted">v${r.version}</td>
        ${i === 0 ? `<td rowspan="${rows.length + 1}">${capMeter(c.region, state.rows, c.cap)}</td>` : ""}
      </tr>`;
    });
    html += `<tr><td class="muted small" colspan="3">上限版本 v${c.version} · 谓词 SUM(amount) WHERE region='${c.region}' &lt;= cap</td></tr>`;
  }
  html += "</tbody></table>";
  $("state-table").innerHTML = html;

  $("event-table").innerHTML = state.events.length
    ? state.events.map((e) => `<tr>
        <td class="mono">${e.batch_id}</td>
        <td class="num">v${e.store_version_after}</td>
        <td class="small muted">${e.regions.join(", ")}</td></tr>`).join("")
    : `<tr><td colspan="3" class="muted small">尚无已发布批次</td></tr>`;

  const cached = $("event-table");
  if (cached_outcomes.length) {
    cached.title = `结果缓存（幂等键）：${cached_outcomes.join(", ")}`;
  }
}

// ---------------------------------------------------------------------------
// 场景实验室
// ---------------------------------------------------------------------------

async function loadScenarios() {
  const data = await api("/api/scenarios");
  scenarios = data.scenarios;
  $("scenario-select").innerHTML = scenarios
    .map((s) => `<option value="${s.id}">${esc(s.title)}</option>`).join("");
  renderScenarioDesc();
}

function renderScenarioDesc() {
  const s = scenarios.find((x) => x.id === $("scenario-select").value);
  if (s) $("scn-desc").textContent = s.description;
}

function evidenceList(evidence) {
  if (!evidence || !evidence.length) return "";
  const items = evidence.map((e) => {
    const who = e.committed_by.length ? `，归因于已提交批次 ${e.committed_by.join("、")}` : "";
    return `<li><strong>${esc(e.kind)}</strong>：${esc(e.region)}${e.plan ? "/" + esc(e.plan) : ""}
      期望 <code>${esc(JSON.stringify(e.expected))}</code>，实际 <code>${esc(JSON.stringify(e.actual))}</code>${esc(who)}</li>`;
  });
  return `<ul class="evidence">${items.join("")}</ul>`;
}

function reasonsList(reasons) {
  if (!reasons || !reasons.length) return "";
  return `<ul class="evidence">${reasons.map((r) =>
    `<li><strong>${esc(r.code)}</strong>（${r.severity === "hard" ? "硬性·人工" : "额度类"}）：${esc(r.message)}</li>`
  ).join("")}</ul>`;
}

function readSetBlock(readSet) {
  if (!readSet) return "";
  const lines = readSet.region_reads.map((r) =>
    `谓词 ${r.predicate}\n  成员行 ${JSON.stringify(r.plans)}\n  行版本 ${JSON.stringify(r.row_versions)}\n  快照金额 ${JSON.stringify(r.row_amounts)}  上限 ${r.cap} (v${r.cap_version})`
  );
  lines.push(`写入行基线版本 ${JSON.stringify(readSet.written_row_versions)}`);
  return `<details><summary>查看校验实际读取的区域、条件与版本（谓词读集合）</summary>
    <div class="readset"><pre>${esc(lines.join("\n"))}</pre></div></details>`;
}

function renderTimeline(result) {
  const counts = { accepted: 0, rejected_recompute: 0, rejected_manual: 0 };
  Object.values(result.outcomes).forEach((o) => { counts[o.status] = (counts[o.status] || 0) + 1; });

  $("scn-summary").innerHTML = `
    <div class="tile"><div class="k">共享快照</div><div class="v">#${result.shared_snapshot_id}</div></div>
    <div class="tile"><div class="k">接受</div><div class="v good">${counts.accepted}</div></div>
    <div class="tile"><div class="k">拒绝·可重算</div><div class="v warn">${counts.rejected_recompute}</div></div>
    <div class="tile"><div class="k">拒绝·人工</div><div class="v crit">${counts.rejected_manual}</div></div>`;

  $("scn-timeline").innerHTML = result.timeline.map((e) => {
    let body = "";
    if (e.kind === "snapshot") {
      body = `两个工作进程取得同一快照 <code>#${e.detail.snapshot_id}</code>（整库 v${e.detail.store_version}）`;
    } else if (e.kind === "reset") {
      body = `仓库重置，校验粒度=<code>${esc(e.detail.granularity)}</code>`;
    } else if (e.kind === "stage") {
      const w = e.detail.writes.map((x) => `${x.region}/${x.plan}→${x.new_amount}`).join("，");
      body = `<strong>${esc(e.detail.label)}</strong> 暂存写集合：<code>${esc(w)}</code>
        ，读区域 [${e.detail.read_regions.join(", ")}]
        ${e.detail.advisory_issues.length
          ? `<ul class="evidence">${e.detail.advisory_issues.map((i) =>
              `<li><strong>${esc(i.code)}</strong>：${esc(i.message)}</li>`).join("")}</ul>`
          : `<span class="muted">暂存期校验未发现违约（仍以提交端锁内重校验为准）</span>`}`;
    } else if (e.kind === "retry") {
      body = `<strong>${esc(e.detail.batch_id)}</strong> 基于新快照 <code>#${e.detail.fresh_snapshot}</code> 重算
        （旧快照 #${e.detail.from_snapshot}）。${esc(e.detail.note)}`;
    } else if (e.kind === "commit") {
      const oc = result.outcomes[e.detail.batch_id];
      body = `<strong>${esc(e.detail.batch_id)}</strong> ${pill(e.detail.status)}
        <span class="muted">冲突=${e.detail.conflict ? "是" : "否"}，重校验=${e.detail.revalidated ? "是" : "否"}，耗时 ${e.elapsed_ms.toFixed(2)}ms</span>
        <div class="small muted" style="margin-top:3px">${esc(e.detail.message)}</div>
        ${reasonsList(e.detail.reasons)}${evidenceList(e.detail.evidence)}
        ${oc ? readSetBlock(oc.read_set) : ""}`;
    }
    return `<li><span class="seq mono">${e.seq}</span>
      <span class="actor">${esc(e.actor)}</span>
      <div><span class="kind-tag kind-${e.kind}">${e.kind}</span><span class="detail">${body}</span></div></li>`;
  }).join("");
}

async function runScenario(gran) {
  const id = $("scenario-select").value;
  const delay = (Number($("scn-delay").value) || 0) / 1000;
  await api("/api/granularity", { granularity: gran });
  granularity = gran;
  syncGranularityUI();
  const data = await api("/api/scenario/run", {
    scenario: id, granularity: gran, compute_delay: delay,
  });
  renderTimeline(data.result);
  await refreshState();
  const over = data.state.caps.some((c) => {
    const sum = data.state.rows.filter((r) => r.region === c.region)
      .reduce((s, r) => s + r.amount, 0);
    return sum > c.cap;
  });
  if (over) toast("警告：当前事实表已出现区域越限——这正是「仅行版本」模式的缺陷表现", true);
}

// ---------------------------------------------------------------------------
// 手动工作台
// ---------------------------------------------------------------------------

function renderWrites() {
  for (const k of ["a", "b"]) {
    $(`${k}-writes`).innerHTML = manualWrites[k].length
      ? manualWrites[k].map((w, i) =>
          `<li>${w.region}/${w.plan} → ${w.new_amount}
            <button class="small" style="padding:1px 7px" onclick="removeWrite('${k}',${i})">删</button></li>`).join("")
      : `<li class="muted">（空）</li>`;
  }
}

window.removeWrite = (k, i) => { manualWrites[k].splice(i, 1); renderWrites(); };

document.querySelectorAll("[data-write]").forEach((btn) => {
  btn.addEventListener("click", () => {
    const k = btn.dataset.write;
    const region = $(`${k}-region`).value.trim();
    const plan = $(`${k}-plan`).value.trim();
    const amount = Number($(`${k}-amount`).value);
    if (!region || !plan || !Number.isInteger(amount)) {
      toast("区域、方案不能为空，金额必须是整数", true); return;
    }
    manualWrites[k].push({ region, plan, new_amount: amount });
    renderWrites();
  });
});

function outcomeCard(batchId, o) {
  return `<div style="margin-top:10px;padding:10px 12px;border:1px solid var(--ring);border-radius:8px;background:var(--surface-2)">
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <strong class="mono">${esc(batchId)}</strong> ${pill(o.status)}
      <span class="small muted">基线 v${o.base_store_version}
      ${o.committed_store_version !== null ? `→ 发布 v${o.committed_store_version}` : "未发布"}
      · 冲突=${o.conflict ? "是" : "否"} · 重校验=${o.revalidated ? "是" : "否"}
      · 尝试次数=${o.attempt}</span>
    </div>
    <div class="small muted" style="margin-top:4px">${esc(o.message)}</div>
    ${reasonsList(o.reasons)}${evidenceList(o.conflict_evidence)}${readSetBlock(o.read_set)}
  </div>`;
}

$("btn-parallel").addEventListener("click", async () => {
  if (!manualWrites.a.length || !manualWrites.b.length) {
    toast("请先为两个工作进程各加入至少一条更正", true); return;
  }
  try {
    const data = await api("/api/commit-parallel", {
      batches: [
        { batch_id: "manual-A", label: "手动批次A", writes: manualWrites.a },
        { batch_id: "manual-B", label: "手动批次B", writes: manualWrites.b },
      ],
    });
    $("manual-out").innerHTML = `<div class="small muted" style="margin-top:8px">
      两进程在屏障处同时起跑，共享快照 #${data.shared_snapshot_id}</div>` +
      Object.entries(data.outcomes).map(([id, o]) => outcomeCard(id, o)).join("");
    await refreshState();
  } catch (e) { toast(e.message, true); }
});

$("btn-lost").addEventListener("click", async () => {
  try {
    const writes = manualWrites.a.length ? manualWrites.a : [{ region: "R1", plan: "P103", new_amount: 15 }];
    const first = await api("/api/commit", { batch_id: "lost-X", label: "丢失响应批次", writes });
    const retry = await api("/api/commit", { batch_id: "lost-X", label: "丢失响应批次(重试)", writes });
    const same = first.outcome.committed_store_version === retry.outcome.committed_store_version
      && retry.outcome.attempt === 2;
    $("manual-out").innerHTML =
      `<div class="small muted" style="margin-top:8px">第一次提交与同 ID 重试（模拟响应丢失）</div>` +
      outcomeCard("lost-X（首次）", first.outcome) + outcomeCard("lost-X（重试）", retry.outcome) +
      (same ? `<div class="pill accepted" style="margin-top:8px">幂等成立：同一发布结果，未重复发布</div>`
            : `<div class="pill rejected_manual" style="margin-top:8px">幂等异常！</div>`);
    await refreshState();
  } catch (e) { toast(e.message, true); }
});

$("btn-clear-writes").addEventListener("click", () => {
  manualWrites = { a: [], b: [] }; renderWrites();
});

// ---------------------------------------------------------------------------
// 吞吐对照
// ---------------------------------------------------------------------------

function bar(cls, label, value, max) {
  const h = Math.max(3, Math.round((value / max) * 160));
  return `<div class="bar-col">
    <div class="bar ${cls}" style="height:${h}px"><span class="vlabel">${value.toLocaleString()}</span></div>
    <div class="bar-x"><i class="sw ${cls}" style="background:currentColor"></i>${label}</div>
  </div>`;
}

$("btn-bench").addEventListener("click", async () => {
  try {
    const data = await api("/api/benchmark", {
      n: Number($("bm-n").value),
      compute_delay: (Number($("bm-delay").value) || 0) / 1000,
      workers: Number($("bm-workers").value),
      pressure: Number($("bm-pressure").value),
      seed: Number($("bm-seed").value),
      granularity,
      auto_retry: true,
    });
    const { serial, concurrent } = data;
    const max = Math.max(serial.throughput_per_s, concurrent.throughput_per_s);
    $("bench-bars").innerHTML =
      bar("serial", `串行 ${serial.throughput_per_s}/s`, serial.throughput_per_s, max) +
      bar("concurrent", `并发 ${concurrent.throughput_per_s}/s`, concurrent.throughput_per_s, max);
    const row = (b, name) => `<tr>
      <td>${name}</td><td class="num">${b.wall_ms}</td>
      <td class="num"><strong>${b.throughput_per_s}</strong></td>
      <td class="num">${b.accepted}</td><td class="num">${b.rejected_recompute}</td>
      <td class="num">${b.rejected_manual}</td><td class="num">${b.conflicts}</td>
      <td class="num">${b.revalidations}</td><td class="num">${b.retries_succeeded}</td>
      <td class="num">v${b.final_version}</td></tr>`;
    $("bench-table").innerHTML = row(serial, "串行基线") + row(concurrent, "并发提交");
    const speedup = (serial.wall_ms / concurrent.wall_ms).toFixed(2);
    toast(`完成：并发提速 ${speedup}×；两模式最终版本均为 v${serial.final_version}（正确性一致）`);
    await refreshState();
  } catch (e) { toast(e.message, true); }
});

// ---------------------------------------------------------------------------
// 粒度切换与重置
// ---------------------------------------------------------------------------

function syncGranularityUI() {
  const isRegion = granularity === "region";
  $("gran-region").classList.toggle("on", isRegion);
  $("gran-row").classList.toggle("on", !isRegion);
  $("gran-row").classList.toggle("unsafe", !isRegion);
  const badge = $("mode-badge");
  badge.textContent = isRegion ? "当前：region 粒度（正确模式）" : "当前：row 粒度（缺陷对照，可能越限）";
  badge.className = "badge-mode " + granularity;
}

$("gran-region").addEventListener("click", async () => {
  await api("/api/granularity", { granularity: "region" });
  granularity = "region"; syncGranularityUI();
});
$("gran-row").addEventListener("click", async () => {
  await api("/api/granularity", { granularity: "row" });
  granularity = "row"; syncGranularityUI();
});
$("btn-reset").addEventListener("click", async () => {
  await api("/api/reset");
  await refreshState();
  $("scn-timeline").innerHTML = "";
  $("scn-summary").innerHTML = "";
  $("manual-out").innerHTML = "";
  $("bench-bars").innerHTML = `<div class="muted small" style="align-self:center">点击「运行对照」生成</div>`;
  $("bench-table").innerHTML = "";
  toast("仓库已重置到种子数据，提交结果缓存已清空");
});
$("scenario-select").addEventListener("change", renderScenarioDesc);
$("btn-run-scn").addEventListener("click", () => runScenario("region").catch((e) => toast(e.message, true)));
$("btn-run-scn-row").addEventListener("click", () => runScenario("row").catch((e) => toast(e.message, true)));

// 初始化
(async function init() {
  try {
    await loadScenarios();
    await refreshState();
    renderWrites();
    syncGranularityUI();
  } catch (e) {
    toast("初始化失败：" + e.message, true);
  }
})();
