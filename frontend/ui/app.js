// TaoSentinel minimal UI (spec 14 §8.6). Plain ES2020, no build step, same origin as the API.
"use strict";

const API = location.origin;
const TOKEN_KEY = "taosentinel.token";
const PILLARS = ["P1", "P2", "P3", "P4", "P5", "P6"];
const PILLAR_NAMES = { P1: "Chain", P2: "Builder", P3: "Product", P4: "Team", P5: "Community", P6: "Narrative" };
const app = document.getElementById("app");
let current = { view: null, refresh: null, timer: null, streamKinds: [] };

// ── helpers ───────────────────────────────────────────────────────────────────────────────────────────────────
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (v, d = 1) => (v === null || v === undefined ? "—" : Number(v).toFixed(d));
const dot2 = (v) => (v === null || v === undefined ? "—" : Number(v).toFixed(2).replace(/^0\./, "."));
const when = (iso) => (iso ? iso.replace("T", " ").replace(/:\d\dZ$/, "Z") : "—");
const safeUrl = (u) => (typeof u === "string" && /^https?:\/\//i.test(u) ? u : null);
const ext = (u, label) => (safeUrl(u) ? `<a href="${esc(u)}" target="_blank" rel="noopener noreferrer">${esc(label)}</a>` : "");

function token() { try { return localStorage.getItem(TOKEN_KEY) || ""; } catch { return ""; } }
function setToken(v) { try { v ? localStorage.setItem(TOKEN_KEY, v) : localStorage.removeItem(TOKEN_KEY); } catch { /* private mode */ } }

async function getJSON(path) {
  const r = await fetch(API + path, { headers: { Accept: "application/json" } });
  if (!r.ok) {
    let body = {};
    try { body = await r.json(); } catch { /* not json */ }
    throw new Error(body.error || String(r.status));
  }
  return r.json();
}

async function postJSON(path, body) {
  const headers = { "Content-Type": "application/json" };
  const t = token();
  if (t) headers["X-Auth-Token"] = t;
  const r = await fetch(API + path, { method: "POST", headers, body: JSON.stringify(body) });
  let data = {};
  try { data = await r.json(); } catch { /* empty */ }
  if (r.status === 401) { showTokenPanel(); throw new Error("unauthorized — set the API token"); }
  if (!r.ok) {
    const d = typeof data.detail === "string" && data.detail !== data.error ? `: ${data.detail}` : "";
    throw new Error((data.error || String(r.status)) + d);
  }
  return data;
}

function tierBadge(t) { return t ? `<span class="tier t${esc(t)}">${esc(t)}</span>` : `<span class="muted">—</span>`; }

function heat(v) {
  if (v === null || v === undefined) return `<span class="heat hna">n/a</span>`;
  const i = Math.min(4, Math.max(0, Math.floor(Number(v) * 5)));
  return `<span class="heat h${i}">${dot2(v)}</span>`;
}

function sparkline(vals, w = 80, h = 20) {
  if (!vals || vals.length < 2) return `<span class="muted">${vals && vals.length ? fmt(vals[0]) : "—"}</span>`;
  const lo = Math.min(...vals), hi = Math.max(...vals), span = hi - lo || 1;
  const pts = vals.map((v, i) => `${(i / (vals.length - 1)) * (w - 2) + 1},${h - 1 - ((v - lo) / span) * (h - 2)}`).join(" ");
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true"><polyline fill="none" stroke="var(--s1)" stroke-width="1.5" points="${pts}"/></svg>`;
}

function lineChart(points, series) {
  // points: [{as_of, …}], series: [{key, color, label}]; y fixed 0..100 with tier guides 75/60/45/30.
  const W = 900, H = 240, L = 34, R = 10, T = 10, B = 24;
  if (!points.length) return `<p class="muted">No official history yet.</p>`;
  const t = points.map((p) => Date.parse(p.as_of));
  const t0 = Math.min(...t), t1 = Math.max(...t), ts = t1 - t0 || 1;
  const x = (v) => L + ((v - t0) / ts) * (W - L - R);
  const y = (v) => T + (1 - v / 100) * (H - T - B);
  let g = "";
  for (const lvl of [30, 45, 60, 75]) {
    g += `<line x1="${L}" x2="${W - R}" y1="${y(lvl)}" y2="${y(lvl)}" stroke="var(--grid)" stroke-dasharray="3 3"/><text x="4" y="${y(lvl) + 4}">${lvl}</text>`;
  }
  g += `<text x="${L}" y="${H - 6}">${esc(points[0].as_of.slice(0, 10))}</text><text x="${W - R}" y="${H - 6}" text-anchor="end">${esc(points[points.length - 1].as_of.slice(0, 10))}</text>`;
  for (const s of series) {
    let d = "", pen = false;
    points.forEach((p, i) => {
      const v = p[s.key];
      if (v === null || v === undefined) { pen = false; return; }
      d += `${pen ? "L" : "M"}${x(t[i]).toFixed(1)},${y(v).toFixed(1)}`;
      pen = true;
    });
    if (d) g += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2"/>`;
  }
  const legend = series.map((s) => `<span><i style="background:${s.color}"></i>${esc(s.label)}</span>`).join("");
  return `<svg class="chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="score history">${g}</svg><div class="legend">${legend}</div>`;
}

// ── header: health dot, token panel, nav ─────────────────────────────────────────────────────────────────────
async function pollHealth() {
  const el = document.getElementById("health-dot");
  try {
    const h = await getJSON("/health");
    const cls = h.status === "ok" ? "dot-green" : h.status === "degraded" ? "dot-amber" : "dot-grey";
    el.className = `dot ${cls}`;
    el.title = `health: ${h.status}${(h.degraded_reasons || []).length ? " — " + h.degraded_reasons.join(", ") : ""}`;
  } catch (e) {
    el.className = "dot dot-red";
    el.title = `health: unreachable (${e.message})`;
  }
}

function showTokenPanel() {
  const p = document.getElementById("token-panel");
  p.hidden = false;
  document.getElementById("token-input").value = token();
}

document.getElementById("token-btn").addEventListener("click", () => {
  const p = document.getElementById("token-panel");
  p.hidden ? showTokenPanel() : (p.hidden = true);
});
document.getElementById("token-save").addEventListener("click", () => {
  setToken(document.getElementById("token-input").value.trim());
  document.getElementById("token-panel").hidden = true;
});
document.getElementById("token-clear").addEventListener("click", () => {
  setToken("");
  document.getElementById("token-input").value = "";
});

document.addEventListener("click", (ev) => {
  const a = ev.target.closest("a[data-nav]");
  if (!a || ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.button !== 0) return;
  ev.preventDefault();
  navigate(a.getAttribute("href"));
});
window.addEventListener("popstate", () => route());

function navigate(href) {
  history.pushState(null, "", href);
  route();
}

// ── stream ───────────────────────────────────────────────────────────────────────────────────────────────────
let debounce = null;
function onStream(kind) {
  if (!current.refresh || !current.streamKinds.includes(kind)) return;
  clearTimeout(debounce);
  debounce = setTimeout(() => current.refresh && current.refresh(true), 2000);
}
try {
  const es = new EventSource(`${API}/api/v1/stream?types=score,event`);
  es.addEventListener("score", () => onStream("score"));
  es.addEventListener("event", () => onStream("event"));
  es.addEventListener("resync", () => current.refresh && current.refresh(true));
} catch { /* SSE unsupported: polling still runs */ }

// ── router ───────────────────────────────────────────────────────────────────────────────────────────────────
function route() {
  clearInterval(current.timer);
  current = { view: null, refresh: null, timer: null, streamKinds: [] };
  let path = location.pathname.replace(/\/+$/, "");
  if (path === "/ui" || path === "") { history.replaceState(null, "", "/ui/matrix"); path = "/ui/matrix"; }
  const m = path.match(/^\/ui\/subnet\/(\d+)$/);
  document.querySelectorAll("nav a[data-view]").forEach((a) => a.classList.toggle("active", path.startsWith("/ui/" + a.dataset.view)));
  if (path === "/ui/matrix") return matrixView();
  if (path === "/ui/events") return eventsView();
  if (m) return subnetView(Number(m[1]));
  app.innerHTML = `<h1>Not found</h1><p><a href="/ui/matrix" data-nav>Back to the matrix</a></p>`;
}

function every(ms, fn) { current.refresh = fn; current.timer = setInterval(() => fn(true), ms); fn(false); }

// ── matrix ───────────────────────────────────────────────────────────────────────────────────────────────────
const mstate = { tiers: new Set(), minConf: 0, pre: false, ins: false, q: "", sort: "composite", asc: false, data: null };
const SORTS = {
  netuid: (r) => r.netuid, name: (r) => (r.name || "").toLowerCase(), tier: (r) => r.score?.tier ?? "Z",
  composite: (r) => r.score?.composite, quality: (r) => r.score?.quality, velocity: (r) => r.score?.velocity,
  confidence: (r) => r.score?.confidence, flags: (r) => (r.score?.red_flags || []).length,
  ...Object.fromEntries(PILLARS.map((p) => [p, (r) => r.score?.pillars?.[p]])),
};

function matrixView() {
  current.view = "matrix";
  current.streamKinds = ["score"];
  app.innerHTML = `
    <h1>Subnet matrix</h1>
    <div id="m-stats" class="stats"></div>
    <div class="bar">
      <span class="tiers">${["A", "B", "C", "D", "F"].map((t) => `<button type="button" data-tier="${t}">${t}</button>`).join(" ")}</span>
      <label class="toggle">min conf <input id="m-conf" type="range" min="0" max="1" step="0.05" value="${mstate.minConf}"><span id="m-conf-v">${mstate.minConf.toFixed(2)}</span></label>
      <label class="toggle"><input id="m-pre" type="checkbox" ${mstate.pre ? "checked" : ""}> pre-launch</label>
      <label class="toggle"><input id="m-ins" type="checkbox" ${mstate.ins ? "checked" : ""}> insufficient</label>
      <input id="m-q" type="search" placeholder="name or netuid" value="${esc(mstate.q)}">
      <a id="m-csv" href="#">CSV</a>
    </div>
    <div id="m-table" class="table-wrap"><p class="muted" style="padding:10px">Loading…</p></div>`;
  app.querySelectorAll("[data-tier]").forEach((b) => {
    b.classList.toggle("on", mstate.tiers.has(b.dataset.tier));
    b.addEventListener("click", () => {
      mstate.tiers.has(b.dataset.tier) ? mstate.tiers.delete(b.dataset.tier) : mstate.tiers.add(b.dataset.tier);
      b.classList.toggle("on");
      renderMatrix();
    });
  });
  const conf = document.getElementById("m-conf");
  conf.addEventListener("input", () => { mstate.minConf = Number(conf.value); document.getElementById("m-conf-v").textContent = mstate.minConf.toFixed(2); renderMatrix(); });
  document.getElementById("m-pre").addEventListener("change", (e) => { mstate.pre = e.target.checked; renderMatrix(); });
  document.getElementById("m-ins").addEventListener("change", (e) => { mstate.ins = e.target.checked; renderMatrix(); });
  document.getElementById("m-q").addEventListener("input", (e) => { mstate.q = e.target.value.trim().toLowerCase(); renderMatrix(); });
  document.getElementById("m-csv").addEventListener("click", (e) => {
    e.preventDefault();
    window.open(`${API}/api/v1/matrix.csv?include_prelaunch=${mstate.pre ? 1 : 0}&include_insufficient=${mstate.ins ? 1 : 0}`);
  });
  every(60000, async () => {
    try {
      mstate.data = await getJSON("/api/v1/subnets?include_prelaunch=1");
      renderMatrix();
    } catch (e) {
      document.getElementById("m-table").innerHTML = `<p class="msg err" style="padding:10px">Failed to load: ${esc(e.message)}</p>`;
    }
  });
}

function renderMatrix() {
  const d = mstate.data;
  if (!d || current.view !== "matrix") return;
  let rows = d.subnets.filter((r) => {
    const s = r.score;
    if (r.status === "pre_launch" && !mstate.pre) return false;
    if (s && s.insufficient_data && !mstate.ins) return false;
    if (mstate.tiers.size && !(s && mstate.tiers.has(s.tier))) return false;
    if (mstate.minConf > 0 && !(s && s.confidence >= mstate.minConf)) return false;
    if (mstate.q && !(String(r.netuid) === mstate.q || (r.name || "").toLowerCase().includes(mstate.q))) return false;
    return true;
  });
  const key = SORTS[mstate.sort];
  rows = rows.slice().sort((a, b) => {
    const va = key(a), vb = key(b);
    if (va === vb) return a.netuid - b.netuid;
    if (va === null || va === undefined) return 1;
    if (vb === null || vb === undefined) return -1;
    return (va < vb ? -1 : 1) * (mstate.asc ? 1 : -1);
  });
  const scored = d.subnets.filter((r) => r.score).length;
  document.getElementById("m-stats").innerHTML =
    `<div class="stat"><div class="k">shown</div><div class="v">${rows.length}</div></div>` +
    `<div class="stat"><div class="k">scored</div><div class="v">${scored}</div></div>` +
    `<div class="stat"><div class="k">as_of</div><div class="v mono">${esc(when(d.as_of))}</div></div>` +
    `<div class="stat"><div class="k">spec</div><div class="v mono">${esc(d.spec_version)}</div></div>` +
    (d.stale ? `<div class="banner">Scores are stale (older than the feed TTL, or scoring is paused).</div>` : "");
  const th = (k, label, num = false) =>
    `<th class="sort${num ? " num" : ""}${mstate.sort === k ? " on" + (mstate.asc ? " asc" : "") : ""}" data-sort="${k}">${label}</th>`;
  const body = rows.map((r) => {
    const s = r.score || {};
    const flags = (s.red_flags || []).map((f) => `<span class="chip flag">${esc(f)}</span>`).join("");
    const pre = r.status === "pre_launch" ? ` <span class="chip">pre-launch</span>` : "";
    const ins = s.insufficient_data ? ` <span class="chip">insufficient</span>` : "";
    return `<tr><td class="num">${r.netuid}</td>
      <td><a href="/ui/subnet/${r.netuid}" data-nav>${esc(r.name || "?")}</a>${pre}${ins}</td>
      <td>${tierBadge(s.tier)}</td><td class="num">${fmt(s.composite)}</td><td class="num">${fmt(s.quality)}</td>
      <td class="num">${fmt(s.velocity)}</td>
      ${PILLARS.map((p) => `<td>${heat(s.pillars ? s.pillars[p] : null)}</td>`).join("")}
      <td class="num">${dot2(s.confidence)}</td><td>${flags}</td><td>${sparkline(r.spark_7d)}</td></tr>`;
  }).join("");
  document.getElementById("m-table").innerHTML = `<table><thead><tr>
    ${th("netuid", "SN", true)}${th("name", "name")}${th("tier", "tier")}${th("composite", "composite", true)}
    ${th("quality", "health", true)}${th("velocity", "velocity", true)}${PILLARS.map((p) => th(p, p)).join("")}
    ${th("confidence", "conf", true)}${th("flags", "flags")}<th>7d</th></tr></thead>
    <tbody>${body || `<tr><td colspan="16" class="muted">No subnets match.</td></tr>`}</tbody></table>`;
  document.querySelectorAll("#m-table th[data-sort]").forEach((h) => h.addEventListener("click", () => {
    if (mstate.sort === h.dataset.sort) mstate.asc = !mstate.asc;
    else { mstate.sort = h.dataset.sort; mstate.asc = ["netuid", "name", "tier"].includes(mstate.sort); }
    renderMatrix();
  }));
}

// ── subnet ───────────────────────────────────────────────────────────────────────────────────────────────────
function statusBadge(e) {
  const st = e.raw_status;
  if (st === "ok") return `<span class="badge b-ok">ok</span>`;
  if (st === "absent") return `<span class="badge b-absent" title="${esc(e.reason || "")}">verifiable absence</span>`;
  if (st === "not_applicable") return `<span class="badge b-na">n/a</span>`;
  return `<span class="badge b-fail" title="${esc(e.reason || "")}">collection failure</span>`;
}

function subnetView(netuid) {
  current.view = "subnet";
  current.streamKinds = ["score", "event"];
  app.innerHTML = `<p class="muted">Loading SN${netuid}…</p>`;
  every(30000, async (isRefresh) => {
    try {
      const d = await getJSON(`/api/v1/subnets/${netuid}`);
      if (current.view !== "subnet") return;
      if (isRefresh && app.contains(document.activeElement) && document.activeElement.matches("input, textarea")) return;
      renderSubnet(d);
    } catch (e) {
      app.innerHTML = `<h1>SN${netuid}</h1><p class="msg err">${esc(e.message === "unknown_netuid" ? "No current occupant on this netuid." : "Failed to load: " + e.message)}</p>`;
    }
  });
}

function renderSubnet(d) {
  const s = d.score;
  const links = [ext(d.links.website, "web"), ext(d.links.github, "github"), ext(d.links.docs, "docs"), ext(d.links.discord, "discord"),
    ext(d.links.telegram, "telegram"), d.links.x ? ext(`https://x.com/${d.links.x}`, "x") : ""].filter(Boolean).join(" · ");
  let html = `<h1>SN${d.netuid} ${esc(d.name || "?")} ${s ? tierBadge(s.tier) : ""}</h1>
    <div class="muted mono">${esc(d.occupant_key)} · ${esc(d.status)}${d.tenure_days !== null ? ` · ${d.tenure_days} d` : ""}${d.immune ? " · immune" : ""}</div>
    <div class="bar">${links}</div>`;
  if (!s) {
    html += `<div class="banner">No score yet for ${esc(d.spec_version)} (the next official run is at 00:15Z).</div>`;
  } else {
    if (s.stale) html += `<div class="banner">This score is stale (older than the feed TTL, or scoring is paused).</div>`;
    if (s.status === "pre_launch") html += `<div class="banner">Pre-launch: no composite until emission starts.</div>`;
    if (s.insufficient_data) html += `<div class="banner">Insufficient data (confidence below 0.4): treat with care.</div>`;
    html += `<div class="stats">
      <div class="stat"><div class="k">composite</div><div class="v">${fmt(s.composite)}</div></div>
      <div class="stat"><div class="k">health</div><div class="v">${fmt(s.quality)}</div></div>
      <div class="stat"><div class="k">velocity</div><div class="v">${fmt(s.velocity)}</div></div>
      <div class="stat"><div class="k">confidence</div><div class="v">${dot2(s.confidence)}</div></div>
      <div class="stat"><div class="k">regime</div><div class="v">${esc(s.regime || "—")}</div></div>
      <div class="stat"><div class="k">as_of</div><div class="v mono">${esc(when(s.as_of))} ${esc(s.run_kind)}</div></div></div>`;
    const ex = s.explain || {};
    const drv = (xs) => (xs || []).map((x) => `<div><code>${esc(x.metric)}</code> <span class="muted">${esc(x.pillar)}</span> ${fmt(x.impact, 1)}</div>`).join("") || `<div class="muted">none</div>`;
    const flags = (s.red_flags || []).map((f) => `<div><span class="chip flag">${esc(f.flag)}</span> ×${fmt(f.multiplier, 2)} since ${esc(when(f.since))} · ${esc(f.state)} · ${esc(f.origin)}${f.sticky ? " · sticky" : ""}</div>`).join("") || `<div class="muted">none</div>`;
    html += `<div class="grid2">
      <div class="card"><h2 style="margin-top:0">Pillars</h2>${PILLARS.map((p) => {
        const v = s.pillars[p], cov = s.coverage ? s.coverage[p] : null;
        return `<div class="pillar" title="coverage ${dot2(cov)}"><span>${p} ${PILLAR_NAMES[p]}</span><span class="track"><span class="fill" style="width:${v === null ? 0 : Math.round(v * 100)}%"></span></span><span class="mono">${v === null ? "n/a" : dot2(v)}</span></div>`;
      }).join("")}</div>
      <div class="card"><h2 style="margin-top:0">Explain</h2>
        <div class="grid2"><div><b>Top +</b>${drv(ex.top_positive)}</div><div><b>Top −</b>${drv(ex.top_negative)}</div></div>
        <h2>Red flags</h2>${flags}
        <div class="muted" style="margin-top:8px">raw composite ${fmt(s.composite_raw)} · spec ${esc(s.spec_version)}</div></div></div>`;
  }
  html += `<h2>History (official, 90 d)</h2><div class="card">${lineChart(d.history_90d, [
    { key: "composite", color: "var(--s1)", label: "composite" },
    { key: "quality", color: "var(--s3)", label: "health" },
    { key: "velocity", color: "var(--s2)", label: "velocity" }])}</div>`;
  if (d.site) {
    html += `<h2>Website</h2><div class="card">${ext(d.site.url, d.site.url || "site")} — last event <code>${esc(d.site.last_event)}</code>${d.site.last_event === "site_recovered" ? ` (reason <b>${esc(d.site.reason)}</b>)` : ""} at ${esc(when(d.site.at))}</div>`;
  }
  if (d.evidence.length) {
    html += `<h2>Evidence (${d.evidence.length})</h2><div class="table-wrap"><table><thead><tr><th>metric</th><th>pillar</th><th class="num">m</th><th>raw</th><th>status</th><th>raw as_of</th><th>fresh</th><th>link</th></tr></thead><tbody>
      ${d.evidence.map((e) => `<tr><td><code>${esc(e.metric_key)}</code>${e.fallback ? ` <span class="chip">fallback</span>` : ""}</td><td>${esc(e.pillar || "")}</td>
        <td class="num">${e.m === null ? "—" : dot2(e.m)}</td><td class="mono">${esc(typeof e.raw === "object" && e.raw !== null ? JSON.stringify(e.raw) : (e.raw ?? ""))}</td>
        <td>${statusBadge(e)}${e.reason ? ` <span class="muted">${esc(e.reason)}</span>` : ""}</td><td class="mono">${esc(when(e.raw_as_of))}</td>
        <td><span class="dot ${e.fresh ? "dot-green" : "dot-grey"}" title="freshness ${e.freshness_s ?? "?"} s"></span></td><td>${ext(e.evidence_url, "source")}</td></tr>`).join("")}
      </tbody></table></div>`;
  }
  html += `<h2>Events</h2><div class="card events-list" style="padding:0">${d.events.length ? d.events.map(eventRow).join("") : `<div class="ev muted">none</div>`}</div>`;
  html += `<div class="grid2">
    <div><h2>Team</h2><div class="card">${teamHtml(d)}</div></div>
    <div><h2>Product</h2><div class="card">${productHtml(d)}</div>
      <h2>Notes</h2><div class="card">${notesHtml(d)}</div></div></div>`;
  app.innerHTML = html;
  wireSubnetForms(d);
}

function teamHtml(d) {
  const rows = d.team.map((t) => `<tr><td>#${t.id}</td><td>${esc(t.name)}</td><td>${esc(t.role || "")}</td><td>${esc(t.status)}</td>
    <td class="num">${t.evidence_count ?? 0}</td><td>${ext(t.profile_url, "profile")}</td>
    <td>${t.status === "candidate" ? `<form class="form verify" data-id="${t.id}"><input type="url" name="u1" placeholder="evidence URL" required><input type="url" name="u2" placeholder="second source"><button>Verify</button><span class="msg"></span></form>` : ""}</td></tr>`).join("");
  const disc = d.disclosure ? `<div class="muted">disclosure: <b>${esc(d.disclosure.value)}</b> (${esc(when(d.disclosure.set_at))})</div>` : "";
  return `${disc}${rows ? `<div class="table-wrap"><table><tbody>${rows}</tbody></table></div>` : `<div class="muted">No team members recorded.</div>`}
    <form class="form" id="team-form"><input type="text" name="name" placeholder="name" required maxlength="120">
      <input type="text" name="role" placeholder="role" maxlength="60"><input type="url" name="profile_url" placeholder="profile URL">
      <input type="url" name="evidence_url" placeholder="evidence URL"><button>Add candidate</button><span class="msg"></span></form>`;
}

function productHtml(d) {
  const p = d.product;
  const cur = p ? `<div>status <b>${esc(p.status || "cleared")}</b> · ${ext(p.evidence_url, "evidence")} · ${esc(when(p.set_at))} by ${esc(p.set_by)}${p.note ? `<div class="muted">${esc(p.note)}</div>` : ""}</div>` : `<div class="muted">Not set.</div>`;
  return `${cur}<form class="form" id="product-form"><select name="status"><option>live</option><option>beta</option><option>waitlist</option><option>none</option><option value="">clear</option></select>
    <input type="url" name="evidence_url" placeholder="evidence URL" required><input type="text" name="note" placeholder="note"><button>Set</button><span class="msg"></span></form>`;
}

function notesHtml(d) {
  const ns = d.notes.map((n) => `<div style="margin-bottom:6px"><div>${esc(n.text)}</div><div class="muted">${ext(n.evidence_url, "evidence")} · ${esc(when(n.created_at))} by ${esc(n.created_by)}</div></div>`).join("");
  return `${ns || `<div class="muted">No notes.</div>`}<form class="form" id="note-form"><input type="text" name="text" placeholder="note" required maxlength="2000">
    <input type="url" name="evidence_url" placeholder="evidence URL" required><button>Add note</button><span class="msg"></span></form>`;
}

function submitter(form, build, path, after) {
  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const msg = form.querySelector(".msg");
    const btn = form.querySelector("button");
    btn.disabled = true;
    msg.className = "msg";
    msg.textContent = "…";
    try {
      const res = await postJSON(typeof path === "function" ? path(form) : path, build(new FormData(form)));
      msg.className = "msg ok";
      msg.textContent = res.warning ? `saved (${res.warning})` : "saved";
      if (after) setTimeout(after, 600);
    } catch (e) {
      msg.className = "msg err";
      msg.textContent = e.message;
    } finally {
      btn.disabled = false;
    }
  });
}

function wireSubnetForms(d) {
  const reload = () => current.refresh && current.refresh(false);
  const opt = (v) => (v && String(v).trim() ? String(v).trim() : null);
  const tf = document.getElementById("team-form");
  if (tf) submitter(tf, (f) => ({ netuid: d.netuid, name: f.get("name"), role: opt(f.get("role")), profile_url: opt(f.get("profile_url")), evidence_url: opt(f.get("evidence_url")) }), "/api/v1/manual/team", reload);
  document.querySelectorAll("form.verify").forEach((vf) =>
    submitter(vf, (f) => ({ evidence_urls: [f.get("u1"), f.get("u2")].map(opt).filter(Boolean) }), (form) => `/api/v1/manual/team/${form.dataset.id}/verify`, reload));
  const pf = document.getElementById("product-form");
  if (pf) submitter(pf, (f) => ({ netuid: d.netuid, status: opt(f.get("status")), evidence_url: f.get("evidence_url"), note: opt(f.get("note")) }), "/api/v1/manual/product", reload);
  const nf = document.getElementById("note-form");
  if (nf) submitter(nf, (f) => ({ netuid: d.netuid, text: f.get("text"), evidence_url: f.get("evidence_url") }), "/api/v1/manual/note", reload);
}

function eventRow(e) {
  const p = e.payload || {};
  const kv = Object.entries(p).filter(([k]) => !["alert_route", "text"].includes(k)).slice(0, 6)
    .map(([k, v]) => `${esc(k)}=${esc(typeof v === "object" ? JSON.stringify(v) : v).slice(0, 80)}`).join(" · ");
  const who = e.netuid !== null && e.netuid !== undefined ? `<a href="/ui/subnet/${e.netuid}" data-nav>SN${e.netuid}${e.name ? " " + esc(e.name) : ""}</a> · ` : "";
  return `<div class="ev"><div class="mono muted">${esc(when(e.observed_at))}</div><div><span class="chip sev-${esc(e.severity)}">${esc(e.severity)}</span></div>
    <div>${who}<code>${esc(e.event_type)}</code> <span class="muted">${esc(e.source || "")}</span><div class="payload">${kv}</div></div></div>`;
}

// ── events ───────────────────────────────────────────────────────────────────────────────────────────────────
function eventsView() {
  current.view = "events";
  current.streamKinds = ["event"];
  const qs = new URLSearchParams(location.search);
  const st = { sev: new Set((qs.get("severity") || "").split(",").filter(Boolean)), type: qs.get("type") || "", netuid: qs.get("netuid") || "", since: qs.get("since") || "7d", rows: [], next: null };
  app.innerHTML = `<h1>Events</h1>
    <div class="bar"><span class="sev">${["critical", "warning", "info"].map((s) => `<button type="button" data-sev="${s}" class="${st.sev.has(s) ? "on" : ""}">${s}</button>`).join(" ")}</span>
      <input id="e-type" type="text" placeholder="types (comma list)" value="${esc(st.type)}">
      <input id="e-net" type="number" min="0" max="1023" placeholder="netuid" value="${esc(st.netuid)}" style="width:90px">
      <select id="e-since">${["24h", "7d", "30d"].map((s) => `<option ${s === st.since ? "selected" : ""}>${s}</option>`).join("")}</select></div>
    <div id="e-list" class="card events-list" style="padding:0"><div class="ev muted">Loading…</div></div>
    <p><button type="button" id="e-more" class="secondary" hidden>Load more</button></p>`;
  const params = (before) => {
    const p = new URLSearchParams({ since: st.since, limit: "200" });
    if (st.sev.size) p.set("severity", [...st.sev].join(","));
    if (st.type) p.set("type", st.type);
    if (st.netuid) p.set("netuid", st.netuid);
    if (before) p.set("before_id", before);
    return p;
  };
  const sync = () => {
    const p = params(null);
    p.delete("limit");
    history.replaceState(null, "", `/ui/events?${p}`);
  };
  const draw = () => {
    document.getElementById("e-list").innerHTML = st.rows.length ? st.rows.map(eventRow).join("") : `<div class="ev muted">No events match.</div>`;
    document.getElementById("e-more").hidden = !st.next;
  };
  const load = async () => {
    try {
      const d = await getJSON(`/api/v1/events?${params(null)}`);
      st.rows = d.events;
      st.next = d.next_before_id;
      draw();
    } catch (e) {
      document.getElementById("e-list").innerHTML = `<div class="ev msg err">Failed to load: ${esc(e.message)}</div>`;
    }
  };
  app.querySelectorAll("[data-sev]").forEach((b) => b.addEventListener("click", () => {
    st.sev.has(b.dataset.sev) ? st.sev.delete(b.dataset.sev) : st.sev.add(b.dataset.sev);
    b.classList.toggle("on");
    sync(); load();
  }));
  let typing = null;
  const onInput = () => { clearTimeout(typing); typing = setTimeout(() => { st.type = document.getElementById("e-type").value.trim(); st.netuid = document.getElementById("e-net").value.trim(); sync(); load(); }, 400); };
  document.getElementById("e-type").addEventListener("input", onInput);
  document.getElementById("e-net").addEventListener("input", onInput);
  document.getElementById("e-since").addEventListener("change", (e) => { st.since = e.target.value; sync(); load(); });
  document.getElementById("e-more").addEventListener("click", async () => {
    try {
      const d = await getJSON(`/api/v1/events?${params(st.next)}`);
      st.rows = st.rows.concat(d.events);
      st.next = d.next_before_id;
      draw();
    } catch (e) { /* keep what is shown */ }
  });
  every(30000, load);
}

pollHealth();
setInterval(pollHealth, 30000);
route();
