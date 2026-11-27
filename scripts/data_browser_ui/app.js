// Lakehouse explorer: vanilla JS + inline SVG, no build step. Talks only to the read-only JSON API in
// scripts/data_browser.py. Every value from the API is rendered with textContent (never innerHTML).

const app = document.getElementById("app");
const tip = document.getElementById("tip");
const SVGNS = "http://www.w3.org/2000/svg";

// ---- tiny DOM helpers ---------------------------------------------------------------------------

function h(tag, attrs = {}, ...kids) {
  const el = document.createElement(tag);
  setAttrs(el, attrs);
  append(el, kids);
  return el;
}
function s(tag, attrs = {}, ...kids) {
  const el = document.createElementNS(SVGNS, tag);
  setAttrs(el, attrs);
  append(el, kids);
  return el;
}
function setAttrs(el, attrs) {
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "class") el.setAttribute("class", v);
    else if (k === "text") el.textContent = v;
    else if (k === "style") Object.assign(el.style, v);
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? "" : v);
  }
}
function append(el, kids) {
  for (const k of kids.flat(Infinity)) {
    if (k === null || k === undefined || k === false) continue;
    el.append(k instanceof Node ? k : document.createTextNode(String(k)));
  }
}

const fmt = {
  n: (v) => (v === null || v === undefined || v === "" ? "–" : Number(v).toLocaleString()),
  bytes(b) {
    if (b === null || b === undefined) return "–";
    const u = ["B", "KB", "MB", "GB", "TB"];
    let i = 0, x = Number(b);
    while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(x >= 100 || i === 0 ? 0 : 1)} ${u[i]}`;
  },
  ago(iso) {
    if (!iso) return "–";
    const t = Date.parse(String(iso).replace(" ", "T"));
    if (Number.isNaN(t)) return String(iso);
    const sec = Math.round((Date.now() - t) / 1000);
    if (sec < 90) return `${sec}s ago`;
    if (sec < 5400) return `${Math.round(sec / 60)} min ago`;
    if (sec < 129600) return `${Math.round(sec / 3600)} h ago`;
    return `${Math.round(sec / 86400)} d ago`;
  },
  short: (t, n = 60) => { t = String(t ?? ""); return t.length > n ? t.slice(0, n - 1) + "…" : t; },
};

async function api(path, params = {}) {
  const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== ""));
  const r = await fetch(`/api/${path}${q.toString() ? "?" + q : ""}`);
  const j = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
  if (!r.ok || j.error) throw new Error(j.error || `HTTP ${r.status}`);
  return j;
}

function showTip(ev, lines) {
  tip.replaceChildren(...lines.map((l, i) => (i === 0 ? h("b", { text: l }) : h("div", { text: l }))));
  tip.hidden = false;
  const pad = 14, w = tip.offsetWidth, hh = tip.offsetHeight;
  tip.style.left = `${Math.min(ev.clientX + pad, innerWidth - w - 8)}px`;
  tip.style.top = `${Math.min(ev.clientY + pad, innerHeight - hh - 8)}px`;
}
const hideTip = () => { tip.hidden = true; };
function tipOn(el, lines) {
  el.addEventListener("mousemove", (ev) => showTip(ev, typeof lines === "function" ? lines() : lines));
  el.addEventListener("mouseleave", hideTip);
  return el;
}

function grid(names, rows, opts = {}) {
  if (!rows.length) return h("p", { class: "muted", text: "(no rows)" });
  const numeric = names.map((_, i) => rows.every((r) => r[i] === null || typeof r[i] === "number"));
  const head = h("tr", {}, names.map((n, i) => h("th", { class: numeric[i] ? "num" : "", text: n })));
  const body = rows.map((r, ri) => h("tr", { class: opts.onRow ? "click" : "", onclick: opts.onRow && (() => opts.onRow(ri)) },
    r.map((v, i) => {
      if (opts.cell) { const c = opts.cell(i, v, ri); if (c) return c; }
      return h("td", { class: v === null ? "null" : numeric[i] ? "num" : "", text: v === null ? "null" : typeof v === "number" ? v.toLocaleString() : String(v) });
    })));
  return h("div", { class: "tablewrap", style: opts.maxHeight ? { maxHeight: opts.maxHeight } : undefined },
    h("table", {}, h("thead", {}, head), h("tbody", {}, body)));
}
const tiles = (pairs) => h("div", { class: "tiles" }, pairs.filter(Boolean).map(([k, v]) => h("div", { class: "tile" }, h("div", { class: "v", text: v }), h("div", { class: "k", text: k }))));
const kv = (pairs) => h("div", { class: "kv" }, pairs.filter(Boolean).flatMap(([k, v]) => [h("div", { class: "k", text: k }), h("div", { class: "v" }, v instanceof Node ? v : String(v ?? "–"))]));
const link = (href, text, cls) => h("a", { href, text, class: cls });
const page = (...kids) => h("div", { class: "page" }, kids);
const statusBadge = (st) => h("span", { class: `badge ${st === "ok" ? "st-ok" : "st-bad"}`, text: st === "ok" ? "✓ ok" : `⚠ ${st}` });

// ---- store symbols ------------------------------------------------------------------------------

const STORE = {
  external: { label: "External source", icon: "M4 12.5h8.2a3 3 0 0 0 .3-6 4.2 4.2 0 0 0-8-.6A3.2 3.2 0 0 0 4 12.5z" },
  local: { label: "Local disk (data/)", icon: "M1.5 4h5l1.6 1.8h6.4v7.7h-13z" },
  kafka: { label: "Redpanda topic (Kafka API)", icon: "M1.5 5.5c2.2-2 4.3 2 6.5 0s4.3-2 6.5 0M1.5 9c2.2-2 4.3 2 6.5 0s4.3-2 6.5 0M1.5 12.5c2.2-2 4.3 2 6.5 0s4.3-2 6.5 0" },
  s3: { label: "Iceberg table on SeaweedFS S3", icon: "M2 4.5c0-1.6 12-1.6 12 0l-1.6 9c-.3 1.2-8.5 1.2-8.8 0zM2 4.5c0 1.5 12 1.5 12 0" },
  postgres: { label: "PostgreSQL table", icon: "M2.5 3.8c0-2 11-2 11 0v8.4c0 2-11 2-11 0zM2.5 3.8c0 2 11 2 11 0M2.5 8c0 2 11 2 11 0" },
  app: { label: "Application", icon: "M1.5 2.5h13v11h-13zM1.5 5.5h13M3.5 4h.1M5.5 4h.1" },
};
const storeColor = (st) => `var(--store-${st})`;
function icon(store, x, y, size = 16) {
  return s("path", { class: "icon", d: STORE[store].icon, stroke: storeColor(store), transform: `translate(${x},${y}) scale(${size / 16})` });
}
function storeBadge(store) {
  return h("span", { class: "badge" }, h("span", { class: "sw", style: { background: storeColor(store) } }), STORE[store]?.label || store);
}

// ---- flow data (cached) -------------------------------------------------------------------------

let FLOW = null;
async function getFlow(force = false) {
  if (!FLOW || force) {
    FLOW = await api("flow");
    FLOW.byId = Object.fromEntries(FLOW.nodes.map((n) => [n.id, n]));
    document.getElementById("status").textContent = `stats ${fmt.ago(FLOW.generated_at)}`;
  }
  return FLOW;
}
function nodeHref(n) {
  const [kind, rest] = [n.id.split(":")[0], n.id.slice(n.id.indexOf(":") + 1)];
  if (kind === "iceberg") return `#/iceberg/${rest}`;
  if (kind === "pg") return `#/pg/${rest}`;
  if (kind === "file") return `#/files/local/${rest}`;
  if (n.id === "app:qa" || n.id === "app:triage") return `#/runs?agent=${rest}`;
  if (kind === "kafka") return "#/pg/ops.stream_metrics";
  return null;
}
function statLine(n) {
  const st = n.stats || {};
  if (st.missing) return "table not created yet";
  if (n.store === "s3") return `${fmt.n(st.records)} rows · ${fmt.n(st.files)} files · ${fmt.bytes(st.bytes)}`;
  if (n.store === "postgres") return st.records === undefined ? "" : `${fmt.n(st.records)} rows · ${fmt.bytes(st.bytes)}`;
  if (n.store === "kafka") return st.updated ? `${fmt.n(st.events_15m)} events / 15 min` : "no stream metrics";
  if (n.store === "local") return `${fmt.n(st.files)} files · ${fmt.bytes(st.bytes)}`;
  if (n.store === "app") return st.runs !== undefined ? `${fmt.n(st.runs)} runs · last ${fmt.ago(st.updated)}` : "";
  if (st.last_run) return `last run ${st.last_run} · ${fmt.ago(st.updated)}`;
  return "";
}
function lineage(F, id, dir) { // all ancestors (dir="up") or descendants ("down") of a node
  const seen = new Set([id]), stack = [id];
  while (stack.length) {
    const cur = stack.pop();
    for (const e of F.edges) {
      const [a, b] = dir === "up" ? [e.to, e.from] : [e.from, e.to];
      if (a === cur && !seen.has(b) && e.kind !== "write") { seen.add(b); stack.push(b); }
    }
  }
  return seen;
}

// ---- view: flow map -----------------------------------------------------------------------------

const LAYER_LABEL = { source: "Sources", landing: "Landing", bronze: "Bronze", silver: "Silver", lake_silver: "lake_silver",
  staging: "dbt staging", intermediate: "dbt intermediate", marts: "marts · ops", agent: "agent (pgvector)", consumer: "Consumers" };
const LANES = [
  { label: "External", from: 0, to: 0 }, { label: "Disk · Redpanda", from: 1, to: 1 },
  { label: "SeaweedFS S3 · Apache Iceberg", from: 2, to: 3 }, { label: "PostgreSQL 16 warehouse", from: 4, to: 8 },
  { label: "Apps", from: 9, to: 9 },
];
const NW = 172, NH = 46, COLW = 212, GAP = 14, TOP = 74, LEFT = 24;
const mapState = { opts: { reads: true, writes: true, ops: true }, selected: null, view: null, query: "" };

function layoutMap(F) {
  const o = mapState.opts;
  const nodes = F.nodes.filter((n) => o.ops || !n.id.startsWith("pg:ops."));
  const ids = new Set(nodes.map((n) => n.id));
  const edges = F.edges.filter((e) => ids.has(e.from) && ids.has(e.to) && (o.reads || e.kind !== "read") && (o.writes || e.kind !== "write"));
  const cols = F.layers.map(() => []);
  const order = ["external", "local", "kafka", "s3", "postgres", "app"];
  for (const n of nodes) cols[F.layers.indexOf(n.layer)].push(n);
  for (const c of cols) c.sort((a, b) => order.indexOf(a.store) - order.indexOf(b.store) || a.label.localeCompare(b.label));
  const pos = {};
  const place = () => cols.forEach((c, ci) => c.forEach((n, i) => { pos[n.id] = { x: LEFT + ci * COLW, y: TOP + i * (NH + GAP) }; }));
  place();
  const forward = edges.filter((e) => e.kind !== "write");
  for (let pass = 0; pass < 4; pass++) { // barycenter ordering: put each node near the average of its neighbours
    cols.forEach((c, ci) => {
      const key = {};
      for (const n of c) {
        const near = forward.filter((e) => (pass % 2 ? e.from === n.id : e.to === n.id)).map((e) => pos[pass % 2 ? e.to : e.from]).filter((p) => p && p.x !== LEFT + ci * COLW);
        key[n.id] = near.length ? near.reduce((a, p) => a + p.y, 0) / near.length : pos[n.id].y;
      }
      c.sort((a, b) => key[a.id] - key[b.id]);
    });
    place();
  }
  const height = TOP + Math.max(...cols.map((c) => c.length)) * (NH + GAP) + 70;
  return { nodes, edges, pos, width: LEFT * 2 + F.layers.length * COLW - (COLW - NW), height };
}

function edgePath(a, b) {
  if (b.x > a.x) {
    const x1 = a.x + NW, y1 = a.y + NH / 2, x2 = b.x - 6, y2 = b.y + NH / 2, dx = Math.max(40, (x2 - x1) / 2);
    return `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
  }
  const x1 = a.x + NW / 2, y1 = a.y + NH, x2 = b.x + NW / 2 + 18, y2 = b.y + NH + 6; // write-back edges loop underneath
  return `M${x1},${y1} C${x1},${y1 + 70} ${x2},${y2 + 70} ${x2},${y2}`;
}

async function viewMap() {
  const F = await getFlow();
  const side = h("aside", { class: "side" });
  const canvas = h("div", { class: "canvas" });
  const search = h("input", { type: "search", placeholder: "find a table…", value: mapState.query, style: { width: "180px" } });
  const opt = (key, label) => h("label", {}, h("input", { type: "checkbox", checked: mapState.opts[key], onchange: (e) => { mapState.opts[key] = e.target.checked; draw(); } }), label);
  const toolbar = h("div", { class: "toolbar" }, search, opt("reads", "consumer reads"), opt("writes", "agent writes"), opt("ops", "ops tables"),
    h("span", { class: "spacer" }),
    h("button", { text: "Fit", onclick: () => fit() }), h("button", { text: "+", onclick: () => zoom(0.8) }), h("button", { text: "−", onclick: () => zoom(1.25) }),
    h("button", { text: "Refresh stats", onclick: async () => { await getFlow(true); render(); } }));
  const legend = h("div", { class: "legend" },
    Object.entries(STORE).map(([k, v]) => h("span", {}, h("span", { class: "sw badge", style: { background: storeColor(k), width: "10px", height: "10px", padding: 0, border: 0 } }), v.label)),
    h("span", { text: "— batch   ╌ stream (moving = live)   ··· write-back" }),
    h("span", {}, h("span", { class: "badge st-ok", text: "✓" }), "reconciled"), h("span", {}, h("span", { class: "badge st-bad", text: "⚠" }), "mismatch"));
  canvas.append(legend);
  const warn = F.warnings.length ? h("div", { class: "banner", style: { margin: "0", borderRadius: 0 } }, F.warnings.join(" · ")) : null;
  app.replaceChildren(h("div", { class: "mapview" }, h("div", { class: "mapmain" }, toolbar, warn, canvas), side));

  let L, svg, vp;
  const vb = { x: 0, y: 0, w: 1, h: 1 };
  const setVB = () => svg.setAttribute("viewBox", `${vb.x} ${vb.y} ${vb.w} ${vb.h}`);
  function fit() {
    const r = canvas.getBoundingClientRect(), k = Math.max(L.width / r.width, L.height / r.height);
    Object.assign(vb, { w: r.width * k, h: r.height * k, x: (L.width - r.width * k) / 2, y: 0 });
    setVB();
  }
  function zoom(f, cx = 0.5, cy = 0.5) {
    const nw = vb.w * f, nh = vb.h * f;
    vb.x += (vb.w - nw) * cx; vb.y += (vb.h - nh) * cy; vb.w = nw; vb.h = nh;
    setVB();
  }

  function draw() {
    L = layoutMap(F);
    const now = Date.now();
    vp = s("g");
    LANES.forEach((ln) => {
      const x = LEFT + ln.from * COLW - 10, w = (ln.to - ln.from) * COLW + NW + 20;
      vp.append(s("rect", { class: "lane", x, y: 8, width: w, height: L.height - 16, rx: 10 }),
        s("text", { class: "lane-label", x: x + 10, y: 26, text: ln.label }));
    });
    F.layers.forEach((ly, i) => vp.append(s("text", { class: "col-label", x: LEFT + i * COLW, y: 54, text: LAYER_LABEL[ly] || ly })));
    const edgeEls = [];
    for (const e of L.edges) {
      const a = L.pos[e.from], b = L.pos[e.to];
      const live = e.kind === "stream" && F.byId[e.from]?.stats?.updated && now - Date.parse(String(F.byId[e.from].stats.updated).replace(" ", "T")) < 5 * 60e3;
      const st = e.check ? (e.check.status === "ok" ? " ok" : " bad") : "";
      const d = edgePath(a, b);
      const path = s("path", { class: `edge k-${e.kind}${st}${e.kind === "stream" && !live ? " stale" : ""}`, d, "marker-end": "url(#arrow)" });
      const hit = s("path", { class: "edge-hit", d, onclick: (ev) => { ev.stopPropagation(); selectEdge(e); } });
      tipOn(hit, () => [`${F.byId[e.from].label} → ${F.byId[e.to].label}`, e.via, e.check ? `reconciliation: ${e.check.status} · ${fmt.n(e.check.count_from)} → ${fmt.n(e.check.count_to)} · ${fmt.ago(e.check.checked_at)}` : `${e.kind} hop`]);
      edgeEls.push({ e, path });
      vp.append(path, hit);
    }
    const nodeEls = {};
    for (const n of L.nodes) {
      const p = L.pos[n.id];
      const g = s("g", { class: `node${mapState.selected === n.id ? " sel" : ""}`, transform: `translate(${p.x},${p.y})`, onclick: (ev) => { ev.stopPropagation(); selectNode(n.id); } },
        s("rect", { class: "box", width: NW, height: NH, rx: 7 }),
        s("rect", { x: 0, y: 0, width: 4, height: NH, rx: 2, fill: storeColor(n.store) }),
        icon(n.store, 10, 7),
        s("text", { x: 32, y: 19, text: fmt.short(n.label, 21) }),
        s("text", { class: "t2", x: 12, y: 37, text: fmt.short(statLine(n), 30) }));
      const bad = F.edges.some((e) => e.to === n.id && e.check && e.check.status !== "ok");
      if (bad) g.append(s("circle", { cx: NW - 10, cy: 10, r: 5, fill: "var(--critical)" }), s("text", { x: NW - 13, y: 13.5, text: "!", style: { fill: "#fff", fontSize: "10px", fontWeight: 700 } }));
      if (n.stats?.missing) g.append(s("rect", { class: "box", width: NW, height: NH, rx: 7, style: { fill: "none", strokeDasharray: "4 3" } }));
      tipOn(g, () => [n.label, STORE[n.store].label, statLine(n), n.description].filter(Boolean));
      g.addEventListener("mouseenter", () => highlight(n.id));
      g.addEventListener("mouseleave", () => highlight(mapState.selected));
      nodeEls[n.id] = g;
      vp.append(g);
    }
    const defs = s("defs", {}, s("marker", { id: "arrow", viewBox: "0 0 10 10", refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" },
      s("path", { d: "M0,1 L9,5 L0,9 z", fill: "var(--line-strong)" })));
    const prev = svg ? { ...vb } : null;
    svg = s("svg", { role: "img", "aria-label": "Lakehouse lineage map" }, defs, vp);
    canvas.querySelector("svg")?.remove();
    canvas.prepend(svg);
    if (prev) { Object.assign(vb, prev); setVB(); } else fit();

    function highlight(id) {
      const q = mapState.query.trim().toLowerCase();
      for (const [nid, g] of Object.entries(nodeEls)) g.classList.toggle("match", !!q && (nid.toLowerCase().includes(q)));
      if (!id) { Object.values(nodeEls).forEach((g) => g.classList.remove("dimmed")); edgeEls.forEach(({ path }) => path.classList.remove("dimmed", "hl")); return; }
      const keep = new Set([...lineage(F, id, "up"), ...lineage(F, id, "down")]);
      for (const [nid, g] of Object.entries(nodeEls)) g.classList.toggle("dimmed", !keep.has(nid));
      for (const { e, path } of edgeEls) {
        const on = keep.has(e.from) && keep.has(e.to) && e.kind !== "write";
        path.classList.toggle("dimmed", !on); path.classList.toggle("hl", on && !e.check);
      }
    }
    mapState.highlight = highlight;
    highlight(mapState.selected);
  }

  function selectNode(id) {
    mapState.selected = id;
    renderSide(id);
    draw();
  }
  function selectEdge(e) { mapState.selected = null; draw(); side.replaceChildren(...edgePanel(F, e)); }
  function renderSide(id) {
    if (!id) {
      side.replaceChildren(h("h1", { text: "Data flow" }),
        h("p", { class: "ink2", text: "Every table and hop of the pipeline, left to right. dbt hops come from dbt's manifest.json; the Spark, stream and agent hops mirror the jobs that run them. Batch hops that write ops.reconciliation are coloured by their latest check." }),
        h("p", { class: "ink2", text: "Hover a node to trace its lineage, click it for details, click a line for the hop. Drag to pan, scroll to zoom." }),
        tiles([["tables on S3", fmt.n(F.nodes.filter((n) => n.store === "s3").length)], ["tables in Postgres", fmt.n(F.nodes.filter((n) => n.store === "postgres").length)],
          ["hops", fmt.n(F.edges.length)], ["reconciled hops", fmt.n(F.edges.filter((e) => e.check).length)]]));
      return;
    }
    side.replaceChildren(...nodePanel(F, F.byId[id], selectNode));
  }

  let drag = null, dragged = false;
  canvas.addEventListener("click", () => { if (dragged) return; mapState.selected = null; renderSide(null); draw(); });
  canvas.addEventListener("pointerdown", (ev) => {
    dragged = false;
    if (ev.target.closest(".node,.edge-hit,.legend")) return;
    drag = { x: ev.clientX, y: ev.clientY, vx: vb.x, vy: vb.y };
    canvas.setPointerCapture(ev.pointerId);
    canvas.classList.add("drag");
  });
  canvas.addEventListener("pointermove", (ev) => {
    if (!drag) return;
    const r = canvas.getBoundingClientRect(), k = vb.w / r.width;
    if (Math.abs(ev.clientX - drag.x) + Math.abs(ev.clientY - drag.y) > 3) dragged = true;
    vb.x = drag.vx - (ev.clientX - drag.x) * k; vb.y = drag.vy - (ev.clientY - drag.y) * k; setVB();
  });
  canvas.addEventListener("pointerup", () => { drag = null; canvas.classList.remove("drag"); });
  canvas.addEventListener("wheel", (ev) => {
    ev.preventDefault();
    const r = canvas.getBoundingClientRect();
    zoom(ev.deltaY > 0 ? 1.12 : 1 / 1.12, (ev.clientX - r.left) / r.width, (ev.clientY - r.top) / r.height);
  }, { passive: false });
  search.addEventListener("input", () => { mapState.query = search.value; mapState.highlight(mapState.selected); });
  search.addEventListener("keydown", (ev) => {
    if (ev.key !== "Enter") return;
    const q = search.value.trim().toLowerCase();
    const hit = F.nodes.find((n) => n.id.toLowerCase().includes(q));
    if (hit) selectNode(hit.id);
  });
  draw();
  renderSide(mapState.selected);
}

function hopRow(F, e, otherId, onPick) {
  const other = F.byId[otherId];
  return h("div", { class: "hop" },
    h("div", { class: "row" }, h("span", { class: "sw badge", style: { background: storeColor(other.store), width: "9px", height: "9px", padding: 0, border: 0 } }),
      h("a", { href: "#/", text: other.label, onclick: (ev) => { ev.preventDefault(); onPick(other.id); } }),
      h("span", { class: "badge", text: e.kind }), e.check ? statusBadge(e.check.status) : null),
    h("div", { class: "via", text: e.via }));
}

function nodePanel(F, n, onPick) {
  const st = n.stats || {};
  const href = nodeHref(n);
  const up = F.edges.filter((e) => e.to === n.id), down = F.edges.filter((e) => e.from === n.id);
  const statPairs = Object.entries(st).map(([k, v]) => [k, k === "bytes" ? fmt.bytes(v) : k === "updated" ? `${v} (${fmt.ago(v)})` : typeof v === "number" ? fmt.n(v) : String(v)]);
  const loc = n.location ? n.location.replace(/^s3:\/\/lake\/?/, "") : null;
  return [
    h("div", { class: "muted small", text: `${LAYER_LABEL[n.layer] || n.layer}` }),
    h("h1", { text: n.label }),
    h("div", { class: "row" }, storeBadge(n.store), n.dbt ? h("span", { class: "badge", text: `dbt ${n.dbt}${n.materialized ? " · " + n.materialized : ""}` }) : null),
    n.description ? h("p", { class: "ink2", text: n.description }) : null,
    h("div", { class: "row" },
      href ? h("a", { href, text: n.store === "app" ? "Open runs →" : "Open →" }) : null,
      loc ? h("a", { href: `#/files/s3/${loc}`, text: "Browse files on S3 →" }) : null,
      n.id === "app:grafana" ? h("a", { href: "http://localhost:3000", target: "_blank", rel: "noopener", text: "Open Grafana ↗" }) : null),
    statPairs.length ? kv(statPairs) : null,
    n.location ? kv([["location", h("code", { text: n.location })]]) : null,
    h("h3", { text: `Upstream (${up.length})` }), up.length ? up.map((e) => hopRow(F, e, e.from, onPick)) : h("p", { class: "muted", text: "none: this is where data enters" }),
    h("h3", { text: `Downstream (${down.length})` }), down.length ? down.map((e) => hopRow(F, e, e.to, onPick)) : h("p", { class: "muted", text: "none" }),
    h("p", { class: "muted small mono", text: n.id }),
  ];
}

function edgePanel(F, e) {
  const c = e.check;
  return [
    h("div", { class: "muted small", text: `${e.kind} hop` }),
    h("h1", { text: `${F.byId[e.from].label} → ${F.byId[e.to].label}` }),
    h("p", { class: "ink2", text: e.via }),
    c ? h("div", {}, h("h3", { text: "Latest reconciliation (ops.reconciliation)" }), statusBadge(c.status),
      kv([["source / entity", `${c.source} / ${c.entity}`], ["layers", `${c.layer_from} → ${c.layer_to}`], ["count from", fmt.n(c.count_from)], ["count to", fmt.n(c.count_to)],
        ["missing", fmt.n(c.missing)], ["duplicates", fmt.n(c.duplicates)], ["late", fmt.n(c.late)], ["checked", `${c.checked_at} (${fmt.ago(c.checked_at)})`],
        ["failed checks, 7 d", fmt.n(c.failed_7d)]]), link("#/pg/ops.reconciliation", "All reconciliation rows →"))
      : h("p", { class: "muted", text: e.recon?.length ? "No reconciliation row written yet for this hop." : "This hop doesn't write ops.reconciliation (streams are measured in ops.stream_metrics; dbt hops by dbt tests)." }),
  ];
}

// ---- view: storage map (treemaps) ---------------------------------------------------------------

function squarify(items, x, y, w, hgt) { // items: [{value}], sorted desc -> adds {x,y,w,h}
  const total = items.reduce((a, i) => a + i.value, 0);
  if (!total) return items;
  const scale = (w * hgt) / total;
  let rest = items.map((i) => ({ ...i, area: i.value * scale })), out = [];
  const worst = (row, side) => { const sum = row.reduce((a, r) => a + r.area, 0); return Math.max(...row.map((r) => Math.max((side * side * r.area) / (sum * sum), (sum * sum) / (side * side * r.area)))); };
  while (rest.length) {
    const side = Math.min(w, hgt);
    let row = [rest[0]], i = 1;
    while (i < rest.length && worst([...row, rest[i]], side) <= worst(row, side)) row.push(rest[i++]);
    rest = rest.slice(i);
    const sum = row.reduce((a, r) => a + r.area, 0);
    if (w >= hgt) { const rw = sum / hgt; let yy = y; for (const r of row) { const rh = r.area / rw; out.push({ ...r, x, y: yy, w: rw, h: rh }); yy += rh; } x += rw; w -= rw; }
    else { const rh = sum / w; let xx = x; for (const r of row) { const rw = r.area / rh; out.push({ ...r, x: xx, y, w: rw, h: rh }); xx += rw; } y += rh; hgt -= rh; }
  }
  return out;
}

function treemap(groups, store, W = 640, H = 340) {
  const svg = s("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, width: "100%", role: "img" });
  const gs = squarify(groups.filter((g) => g.value > 0).sort((a, b) => b.value - a.value), 0, 0, W, H);
  for (const g of gs) {
    svg.append(s("rect", { class: "tm-group", x: g.x + 1, y: g.y + 1, width: Math.max(0, g.w - 2), height: Math.max(0, g.h - 2), rx: 6 }));
    svg.append(s("text", { class: "t2", x: g.x + 7, y: g.y + 14, text: `${g.label} · ${fmt.bytes(g.value)}` }));
    const items = squarify(g.items.filter((i) => i.value > 0).sort((a, b) => b.value - a.value), g.x + 4, g.y + 20, Math.max(1, g.w - 8), Math.max(1, g.h - 24));
    for (const it of items) {
      const tile = s("g", { class: "tm-tile", onclick: () => { location.hash = it.href; } },
        s("rect", { x: it.x, y: it.y, width: Math.max(0, it.w), height: Math.max(0, it.h), rx: 4, fill: storeColor(store) }));
      if (it.w > 60 && it.h > 30) tile.append(s("text", { class: "tm-text", x: it.x + 6, y: it.y + 15, text: fmt.short(it.label, Math.floor(it.w / 6.5)) }),
        s("text", { class: "tm-text", x: it.x + 6, y: it.y + 28, text: fmt.bytes(it.value), style: { opacity: .85 } }));
      tipOn(tile, [it.label, `${fmt.bytes(it.value)} (${((100 * it.value) / g.value).toFixed(1)} % of ${g.label})`, ...(it.extra || [])]);
      svg.append(tile);
    }
  }
  return svg;
}

async function viewStorage() {
  const F = await getFlow();
  const ice = F.nodes.filter((n) => n.store === "s3" && n.stats && !n.stats.missing);
  const pgs = F.nodes.filter((n) => n.store === "postgres" && n.stats);
  const group = (nodes, keyFn, itemFn) => Object.values(nodes.reduce((acc, n) => {
    const k = keyFn(n); (acc[k] ||= { label: k, value: 0, items: [] }); const it = itemFn(n); acc[k].items.push(it); acc[k].value += it.value; return acc; }, {}));
  const iceGroups = group(ice, (n) => n.layer, (n) => ({ label: n.label, value: n.stats.bytes, href: `#/iceberg/${n.id.slice(8)}`, extra: [`${fmt.n(n.stats.records)} rows · ${fmt.n(n.stats.files)} live data files`] }));
  const pgGroups = group(pgs, (n) => n.id.slice(3).split(".")[0], (n) => ({ label: n.label, value: n.stats.bytes, href: `#/pg/${n.id.slice(3)}`, extra: [`${fmt.n(n.stats.records)} rows · ${n.stats.kind}`] }));
  const overhead = h("div", {}, h("p", { class: "muted", text: "measuring bucket prefixes…" }));
  app.replaceChildren(page(
    h("h1", { text: "Storage map" }),
    h("p", { class: "ink2", text: "Where the bytes live. Tile area = size; click a tile to open the table." }),
    tiles([["Iceberg live data", fmt.bytes(ice.reduce((a, n) => a + n.stats.bytes, 0))], ["Iceberg data files", fmt.n(ice.reduce((a, n) => a + n.stats.files, 0))],
      ["Postgres tables", fmt.bytes(pgs.reduce((a, n) => a + (n.stats.bytes || 0), 0))], ["local data/", fmt.bytes(F.nodes.filter((n) => n.store === "local").reduce((a, n) => a + (n.stats?.bytes || 0), 0))]]),
    h("div", { class: "cards", style: { gridTemplateColumns: "repeat(auto-fit, minmax(480px, 1fr))" } },
      h("div", { class: "card" }, h("h2", { style: { marginTop: 0 } }, storeBadge("s3"), " bucket lake · Iceberg live data files"), treemap(iceGroups, "s3"),
        h("p", { class: "muted small", text: "Live = referenced by the current snapshot (total-files-size). Older snapshots, metadata and orphans add to what's on disk: see below." })),
      h("div", { class: "card" }, h("h2", { style: { marginTop: 0 } }, storeBadge("postgres"), " warehouse · pg_total_relation_size"), treemap(pgGroups, "postgres"),
        h("p", { class: "muted small", text: "Includes indexes and TOAST: agent.chunks carries a 384-dim vector, an HNSW index and a GIN full-text index per chunk." }))),
    h("h2", { text: "On disk vs. live, per Iceberg table" }), overhead));

  const rows = await Promise.all(ice.map(async (n) => {
    const prefix = n.location.replace(/^s3:\/\/lake\/?/, "");
    try { const d = await api("dirsize", { root: "s3", path: prefix }); return { n, prefix, d }; } catch (err) { return { n, prefix, err }; }
  }));
  let other = null;
  try { other = await api("dirsize", { root: "s3", path: "checkpoints" }); } catch { /* no checkpoints yet */ }
  const data = rows.map(({ n, prefix, d }) => [n.id.slice(8), n.stats.bytes, d ? d.bytes : null, d ? d.files : null, d ? (d.bytes ? +(d.bytes / Math.max(1, n.stats.bytes)).toFixed(2) : null) : null, prefix]);
  overhead.replaceChildren(
    h("p", { class: "ink2", text: "On-disk bytes include every snapshot still kept, the metadata tree and files no snapshot references. A high ratio is what snapshot expiry and orphan cleanup in the maintenance DAG reclaim." }),
    grid(["table", "live data bytes", "bytes on disk", "objects", "disk / live", "prefix"], data, {
      onRow: (i) => { location.hash = `#/iceberg/${data[i][0]}`; },
      cell: (i, v) => (i === 1 || i === 2 ? h("td", { class: "num", text: fmt.bytes(v) }) : i === 5 ? h("td", {}, link(`#/files/s3/${v}`, v)) : null),
    }),
    other ? h("p", { class: "muted", text: `checkpoints/ (Spark streaming state): ${fmt.n(other.files)} objects · ${fmt.bytes(other.bytes)}` }) : null);
}

// ---- view: files --------------------------------------------------------------------------------

function crumbs(root, base, path) {
  const parts = path.split("/").filter(Boolean);
  const out = [link(`#/files/${root}/`, root === "s3" ? "s3://lake" : "data/")];
  parts.forEach((p, i) => out.push(h("span", { text: "/" }), link(`#/files/${root}/${parts.slice(0, i + 1).join("/")}`, p)));
  return h("div", { class: "crumbs mono" }, h("span", { class: "row", style: { display: "inline-flex" } },
    h("a", { href: "#/files/s3/", text: "S3", class: root === "s3" ? "" : "muted" }), "·", h("a", { href: "#/files/local/", text: "local", class: root === "local" ? "" : "muted" }), " "), out);
}

function hbars(items, { value, label, sub, W = 560 }) { // horizontal bars, single series
  const max = Math.max(1, ...items.map(value)), rowH = 22, lw = 170;
  const svg = s("svg", { class: "chart", viewBox: `0 0 ${W} ${items.length * rowH + 4}`, width: "100%", style: { maxWidth: `${W}px` } });
  items.forEach((it, i) => {
    const w = Math.max(2, ((W - lw - 90) * value(it)) / max), y = i * rowH + 3;
    const g = s("g", {}, s("text", { x: lw - 8, y: y + 12, "text-anchor": "end", class: "t2", text: fmt.short(label(it), 28) }),
      s("rect", { class: "bar", x: lw, y: y + 2, width: w, height: rowH - 8, rx: 3 }),
      s("text", { x: lw + w + 6, y: y + 12, class: "t2", text: sub(it) }),
      s("rect", { x: 0, y, width: W, height: rowH, fill: "transparent" }));
    tipOn(g, [label(it), sub(it)]);
    svg.append(g);
  });
  return svg;
}

async function viewFiles(root, path) {
  const d = await api("files", { root, path, limit: 25 });
  const head = [crumbs(root, d.base, d.path), d.table ? h("div", { class: "banner info" }, "Part of Iceberg table ", link(`#/iceberg/${d.table}`, d.table), " — the table view explains how these files fit together.") : null];
  if (d.type === "dir") {
    const max = Math.max(1, ...d.entries.map((e) => e.bytes || 0));
    const sizeOut = h("div");
    const rows = d.entries.map((e) => [e.name + (e.dir ? "/" : ""), e.bytes, e.modified]);
    app.replaceChildren(page(...head, h("div", { class: "row" }, h("h1", { text: d.path || (root === "s3" ? "s3://lake" : "data/") }), h("span", { class: "muted", text: `${fmt.n(d.total)} entries` }),
      h("span", { class: "spacer" }), h("button", { text: "Measure this prefix", onclick: async (ev) => {
        ev.target.disabled = true; sizeOut.replaceChildren(h("p", { class: "muted", text: "listing recursively…" }));
        try {
          const z = await api("dirsize", { root, path: d.path });
          sizeOut.replaceChildren(tiles([["objects", fmt.n(z.files)], ["bytes", fmt.bytes(z.bytes)], ["listed in", `${z.seconds}s`]]),
            hbars(z.by_ext.slice(0, 10), { value: (r) => r.bytes, label: (r) => `.${r.ext}`, sub: (r) => `${fmt.bytes(r.bytes)} · ${fmt.n(r.files)} files` }));
        } catch (err) { sizeOut.replaceChildren(h("div", { class: "banner error", text: err.message })); }
      } })), sizeOut,
      grid(["name", "size", "modified"], rows, {
        onRow: (i) => { location.hash = `#/files/${root}/${d.entries[i].path}`; },
        cell: (i, v, ri) => {
          const e = d.entries[ri];
          if (i === 0) return h("td", {}, h("span", { class: "mono", text: `${e.dir ? "▸ " : "  "}${v}` }));
          if (i === 1) return h("td", { class: "num" }, e.dir ? "" : h("span", { class: "row", style: { justifyContent: "flex-end", flexWrap: "nowrap" } },
            h("span", { style: { display: "inline-block", height: "6px", borderRadius: "3px", background: "var(--seq)", width: `${Math.max(2, (60 * (e.bytes || 0)) / max)}px` } }), fmt.bytes(v)));
          return null;
        },
      })));
    return;
  }
  const p = d.preview || {};
  const body = [];
  if (p.kind === "parquet") {
    body.push(tiles([["rows", fmt.n(p.rows_total)], ["row groups", fmt.n(p.num_row_groups)], ["file size", fmt.bytes(d.bytes)], ["columns", fmt.n(p.columns.length)]]),
      h("p", { class: "muted small", text: `written by ${p.created_by}` }),
      h("h2", { text: "Column chunks (row group 0)" }),
      h("p", { class: "ink2", text: "Parquet stores each column separately, compressed and encoded. The min/max statistics let a reader skip a row group without reading it." }),
      grid(["column", "physical", "codec", "encodings", "compressed", "uncompressed", "min", "max", "nulls"],
        p.columns.map((c) => [c.name, c.type, c.codec, c.encodings.join(", "), c.compressed, c.uncompressed, c.min, c.max, c.nulls]),
        { cell: (i, v) => (i === 4 || i === 5 ? h("td", { class: "num", text: fmt.bytes(v) }) : null) }),
      p.row_groups.length > 1 ? h("div", {}, h("h2", { text: "Row groups" }), hbars(p.row_groups.map((g, i) => ({ ...g, i })), { value: (g) => g.compressed, label: (g) => `group ${g.i}`, sub: (g) => `${fmt.n(g.rows)} rows · ${fmt.bytes(g.compressed)}` })) : null,
      h("h2", { text: `First ${p.rows.length} rows` }), grid(p.names, p.rows));
  } else if (p.kind === "json" || p.kind === "text") {
    body.push(p.cut ? h("p", { class: "muted", text: "first 64 KB" }) : null, h("pre", { text: p.text }));
    if (d.path.endsWith(".metadata.json")) body.unshift(h("div", { class: "banner info", text: "Iceberg table metadata: schema, partition specs, snapshot list and the pointer to each snapshot's manifest list. A commit writes a new one of these and swaps the catalog pointer to it." }));
  } else if (p.kind === "avro") {
    body.push(h("div", { class: "banner info", text: p.role === "manifest list"
      ? "Avro manifest list (snap-*.avro): one per snapshot, it lists the manifests of that snapshot with partition summaries so readers can skip whole manifests."
      : "Avro manifest: lists data files with their partition values, record counts and column min/max stats." }),
    d.table ? link(`#/iceberg/${d.table}`, "See the decoded manifests in the table view →") : null);
  } else body.push(h("p", { class: "muted", text: "Binary file: no preview." }));
  app.replaceChildren(page(...head, h("h1", { class: "mono", text: d.path.split("/").pop() }), kv([["size", `${fmt.bytes(d.bytes)} (${fmt.n(d.bytes)} bytes)`], ["modified", d.modified], ["full path", h("code", { text: `${d.base}/${d.path}` })]]), body));
}

// ---- view: Iceberg table ------------------------------------------------------------------------

function layoutDiagram(t) {
  const cur = t.snapshots.find((x) => x.id === t.current_snapshot);
  const rel = (p) => p.replace(/^s3:\/\/lake\/?/, "");
  const manifestBytes = (t.manifests || []).reduce((a, m) => a + m.bytes, 0);
  const steps = [
    { title: "metadata.json", sub: `format v${t.format_version} · ${t.snapshots.length} snapshots kept`, href: `#/files/s3/${rel(t.metadata_location)}`, store: "local", n: 1 },
    { title: "manifest list", sub: cur ? `snapshot ${cur.id.slice(0, 8)}… · ${cur.operation}` : "no snapshot", href: cur ? `#/files/s3/${rel(cur.manifest_list)}` : null, n: 1 },
    { title: `${fmt.n((t.manifests || []).length)} manifests`, sub: `${fmt.bytes(manifestBytes)} Avro`, href: `#/files/s3/${rel(t.location)}/metadata`, n: Math.min(4, (t.manifests || []).length) },
    { title: `${fmt.n(t.files_total)} data files`, sub: `${fmt.bytes(t.data_bytes)} Parquet`, href: `#/files/s3/${rel(t.location)}/data`, n: Math.min(5, t.files_total || 0) },
  ];
  const W = 880, bw = 170, gap = (W - steps.length * bw) / (steps.length - 1);
  const svg = s("svg", { class: "chart", viewBox: `0 0 ${W} 96`, width: "100%", style: { maxWidth: `${W}px` } },
    s("defs", {}, s("marker", { id: "arr2", viewBox: "0 0 10 10", refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: "auto" }, s("path", { d: "M0,1 L9,5 L0,9 z", fill: "var(--line-strong)" }))));
  steps.forEach((st, i) => {
    const x = i * (bw + gap);
    const g = s("g", { class: "node", onclick: () => st.href && (location.hash = st.href) });
    for (let k = Math.max(1, st.n) - 1; k >= 1; k--) g.append(s("rect", { class: "box", x: x + k * 4, y: 14 - k * 4, width: bw, height: 54, rx: 6 }));
    g.append(s("rect", { class: "box", x, y: 14, width: bw, height: 54, rx: 6 }), s("rect", { x, y: 14, width: 4, height: 54, rx: 2, fill: "var(--store-s3)" }),
      s("text", { x: x + 12, y: 36, text: st.title, style: { fontWeight: 600 } }), s("text", { class: "t2", x: x + 12, y: 54, text: fmt.short(st.sub, 27) }));
    tipOn(g, [st.title, st.sub, st.href ? "click to open in the file browser" : ""]);
    svg.append(g);
    if (i < steps.length - 1) svg.append(s("path", { class: "edge", d: `M${x + bw + 6},41 L${x + bw + gap - 4},41`, "marker-end": "url(#arr2)" }));
  });
  svg.append(s("text", { class: "t2", x: 0, y: 90, text: "Catalog → metadata.json → current snapshot's manifest list → manifests (pruned by partition summaries) → only the data files a query needs." }));
  return svg;
}

async function viewIceberg(name) {
  const [t, F] = await Promise.all([api("iceberg", { table: name, limit: 20 }), getFlow()]);
  const n = F.byId[t.node];
  const rel = t.location.replace(/^s3:\/\/lake\/?/, "");
  const parts = (t.partitions || []).slice(-40);
  const smallFiles = (t.size_histogram || []).slice(0, 2).reduce((a, b) => a + b.files, 0);
  app.replaceChildren(page(
    h("div", { class: "crumbs" }, link("#/", "Flow map"), " / ", h("span", { text: `Iceberg · ${name.split(".")[0]}` })),
    h("div", { class: "row" }, h("h1", { text: name }), storeBadge("s3"), h("span", { class: "spacer" }), link(`#/files/s3/${rel}`, "Browse files →")),
    h("p", { class: "mono muted", text: t.location }),
    tiles([["rows (current snapshot)", fmt.n(n?.stats?.records)], ["live data files", fmt.n(t.files_total)], ["data size", fmt.bytes(t.data_bytes)],
      ["manifests", fmt.n((t.manifests || []).length)], ["snapshots kept", fmt.n(t.snapshots.length)], ["partition fields", fmt.n(t.partition_spec.length) || "none"]]),
    n ? h("div", { class: "row small" }, h("span", { class: "muted", text: "upstream:" }), F.edges.filter((e) => e.to === n.id).map((e) => link(nodeHref(F.byId[e.from]) || "#/", F.byId[e.from].label)),
      h("span", { class: "muted", text: " downstream:" }), F.edges.filter((e) => e.from === n.id).map((e) => link(nodeHref(F.byId[e.to]) || "#/", F.byId[e.to].label))) : null,
    h("h2", { text: "How this table is stored" }), t.current_snapshot ? layoutDiagram(t) : h("p", { class: "muted", text: "No snapshot yet: the table is empty." }),
    t.size_histogram ? h("div", {}, h("h2", { text: "Data file sizes" }),
      h("p", { class: "ink2", text: smallFiles > 0.5 * t.files_total && t.files_total > 20
        ? `${fmt.n(smallFiles)} of ${fmt.n(t.files_total)} files are under 256 KB: the small-file pattern of micro-batch streaming. Compaction (rewrite_data_files in the maintenance DAG) merges them.`
        : "File count per size bucket, all live data files." }),
      hbars(t.size_histogram, { value: (b) => b.files, label: (b) => b.label, sub: (b) => `${fmt.n(b.files)} files · ${fmt.bytes(b.bytes)}` })) : null,
    parts.length > 1 ? h("div", {}, h("h2", { text: `Partitions (${t.partitions.length}${t.partitions.length > 40 ? ", last 40 shown" : ""})` }),
      hbars(parts, { value: (p) => p.bytes, label: (p) => String(p.partition), sub: (p) => `${fmt.bytes(p.bytes)} · ${fmt.n(p.files)} files · ${fmt.n(p.records)} rows` })) : null,
    h("h2", { text: "Schema" }), grid(["id", "column", "type", "required"], t.schema.map((f) => [f.id, f.name, f.type, f.required ? "yes" : ""])),
    h("h2", { text: "Partition spec" }), t.partition_spec.length ? grid(["partition", "transform", "source column"], t.partition_spec.map((p) => [p.name, p.transform, p.source])) : h("p", { class: "muted", text: "Unpartitioned (see DECISIONS.md)." }),
    h("h2", { text: "Snapshots (newest first)" }),
    grid(["snapshot", "at", "operation", "added rows", "added files", "deleted files", "total rows", "manifest list"],
      t.snapshots.map((x) => [x.id === t.current_snapshot ? `${x.id} (current)` : x.id, x.at, x.operation, x.summary["added-records"] ?? null, x.summary["added-data-files"] ?? null,
        x.summary["deleted-data-files"] ?? null, x.summary["total-records"] ?? null, x.manifest_list.split("/").pop()]),
      { cell: (i, v, ri) => (i === 7 ? h("td", {}, link(`#/files/s3/${t.snapshots[ri].manifest_list.replace(/^s3:\/\/lake\/?/, "")}`, v)) : i >= 3 && i <= 6 && v !== null ? h("td", { class: "num", text: fmt.n(v) }) : null) }),
    t.manifests ? h("div", {}, h("h2", { text: "Manifests of the current snapshot" }),
      grid(["manifest", "size", "added files", "existing files", "deleted files"], t.manifests.map((m) => [m.path.split("/").pop(), m.bytes, m.added, m.existing, m.deleted]),
        { cell: (i, v, ri) => (i === 0 ? h("td", {}, link(`#/files/s3/${t.manifests[ri].path.replace(/^s3:\/\/lake\/?/, "")}`, v)) : i === 1 ? h("td", { class: "num", text: fmt.bytes(v) }) : null) })) : null,
    t.files ? h("div", {}, h("h2", { text: `Data files (${t.files.length} of ${fmt.n(t.files_total)})` }),
      grid(["file", "partition", "rows", "size"], t.files.map((f) => [f.path.split("/").pop(), f.partition, f.records, f.bytes]),
        { cell: (i, v, ri) => (i === 0 ? h("td", {}, link(`#/files/s3/${t.files[ri].path.replace(/^s3:\/\/lake\/?/, "")}`, fmt.short(v, 48))) : i === 3 ? h("td", { class: "num", text: fmt.bytes(v) }) : null), maxHeight: "360px" })) : null,
    t.rows ? h("div", {}, h("h2", { text: `First ${t.rows.length} rows` }), grid(t.names, t.rows)) : null,
    h("h2", { text: "Table properties" }), grid(["property", "value"], Object.entries(t.properties)),
  ));
}

// ---- view: Postgres table -----------------------------------------------------------------------

async function viewPg(full, offset = 0) {
  const [schema, ...rest] = full.split(".");
  const table = rest.join(".");
  const [t, F] = await Promise.all([api("pg", { schema, table, limit: 50, offset }), getFlow()]);
  const n = F.byId[t.node];
  const kindName = { r: "table", v: "view", m: "materialized view", p: "partitioned table" }[t.kind] || t.kind;
  const pager = h("div", { class: "row" },
    h("button", { text: "← prev", disabled: offset === 0, onclick: () => viewPg(full, Math.max(0, offset - 50)).catch(showError) }),
    h("span", { class: "muted", text: `rows ${fmt.n(offset + 1)}–${fmt.n(Math.min(offset + 50, t.total))} of ${fmt.n(t.total)}` }),
    h("button", { text: "next →", disabled: offset + 50 >= t.total, onclick: () => viewPg(full, offset + 50).catch(showError) }));
  app.replaceChildren(page(
    h("div", { class: "crumbs" }, link("#/", "Flow map"), " / ", h("span", { text: `Postgres · ${schema}` })),
    h("div", { class: "row" }, h("h1", { text: `${schema}.${table}` }), storeBadge("postgres"), n?.dbt ? h("span", { class: "badge", text: `dbt ${n.dbt}` }) : null),
    n?.description ? h("p", { class: "ink2", text: n.description }) : null,
    tiles([["rows", fmt.n(t.total)], ["size incl. indexes", fmt.bytes(t.bytes)], ["kind", kindName], ["primary key", t.primary_key.join(", ") || "–"], ["columns", fmt.n(t.columns.length)]]),
    n ? h("div", { class: "row small" }, h("span", { class: "muted", text: "upstream:" }), F.edges.filter((e) => e.to === n.id).map((e) => link(nodeHref(F.byId[e.from]) || "#/", F.byId[e.from].label)),
      h("span", { class: "muted", text: " downstream:" }), F.edges.filter((e) => e.from === n.id).map((e) => link(nodeHref(F.byId[e.to]) || "#/", F.byId[e.to].label))) : null,
    schema === "agent" && table === "chunks" ? h("div", { class: "banner info" }, "Embeddings and the tsvector are hidden here; open ", link("#/agent", "Agent knowledge"), " to read chunks, their vectors and their nearest neighbours.") : null,
    t.hidden_columns.length ? h("p", { class: "muted small", text: `hidden (binary / vector types): ${t.hidden_columns.join(", ")}` }) : null,
    h("h2", { text: "Columns" }), grid(["column", "type", "nullable"], t.columns.map((c) => [c.column_name, c.data_type, c.is_nullable])),
    h("h2", { text: "Rows" }), pager, grid(t.names, t.rows, { maxHeight: "640px" })));
}

// ---- view: agent knowledge (documents & chunks) -------------------------------------------------

const agentState = { kind: "", ticker: "", q: "" };

async function viewAgent() {
  const d = await api("agent/documents", { ...agentState, limit: 500 });
  const all = agentState.kind || agentState.ticker || agentState.q ? (await api("agent/documents", { limit: 500 })).documents : d.documents;
  const tickers = [...new Set(d.facets.filter((f) => f.ticker).map((f) => f.ticker))].sort();
  const sheets = all.filter((x) => x.kind === "fact_sheet");
  const periods = [...new Set(sheets.map((x) => `${x.fiscal_year}-${x.fiscal_period}`))].sort();
  const byKey = Object.fromEntries(sheets.map((x) => [`${x.ticker}|${x.fiscal_year}-${x.fiscal_period}`, x]));
  const heat = h("table", { class: "heat" },
    h("tr", {}, h("th"), periods.map((p, i) => h("th", { text: i % 4 === 0 || i === periods.length - 1 ? p.slice(0, 4) : "", style: { textAlign: "left" } }))),
    [...new Set(sheets.map((x) => x.ticker))].sort().map((tk) => h("tr", {}, h("th", { text: tk }), periods.map((p) => {
      const doc = byKey[`${tk}|${p}`];
      const c = h("div", { class: `cell${doc ? " on" : ""}`, onclick: doc && (() => { location.hash = `#/agent/doc/${encodeURIComponent(doc.doc_id)}`; }) });
      tipOn(c, doc ? [doc.doc_id, doc.title, `${doc.chunks} chunks · ${fmt.n(doc.chars)} chars`] : [`${tk} ${p}`, "no fact sheet"]);
      return h("td", {}, c);
    }))));
  const sel = (key, options, label) => h("select", { onchange: (e) => { agentState[key] = e.target.value; render(); } },
    h("option", { value: "", text: label }), options.map((o) => h("option", { value: o, text: o, selected: agentState[key] === o })));
  const q = h("input", { type: "search", placeholder: "doc id or title…", value: agentState.q, onchange: (e) => { agentState.q = e.target.value; render(); } });
  app.replaceChildren(page(
    h("h1", { text: "Agent knowledge base" }),
    h("p", { class: "ink2", text: "What the QA and triage agents can retrieve: documents in agent.documents, split into chunks in agent.chunks, each with a 384-dim embedding (HNSW) and a tsvector (GIN) for hybrid search." }),
    tiles([["documents", fmt.n(d.totals.documents)], ["chunks", fmt.n(d.totals.chunks)], ["fact sheets", fmt.n(sheets.length)], ["runbooks", fmt.n(all.filter((x) => x.kind === "runbook").length)], ["tickers", fmt.n(tickers.length)]]),
    h("h2", { text: "Fact-sheet coverage (ticker × fiscal period)" }),
    h("p", { class: "muted small", text: "Each filled cell is one fact sheet. Gaps are quarters with no filed fundamentals." }),
    h("div", { class: "card", style: { overflowX: "auto" } }, heat),
    h("h2", { text: "Runbooks" }),
    h("div", { class: "row" }, all.filter((x) => x.kind === "runbook").map((x) => h("a", { class: "badge", href: `#/agent/doc/${encodeURIComponent(x.doc_id)}`, text: `${x.title} · ${x.chunks} chunks` }))),
    h("h2", { text: "Documents" }),
    h("div", { class: "row" }, sel("kind", ["fact_sheet", "runbook"], "all kinds"), sel("ticker", tickers, "all tickers"), q, h("span", { class: "muted", text: `${fmt.n(d.documents.length)} shown` })),
    h("div", { style: { height: "8px" } }),
    grid(["doc_id", "title", "kind", "ticker", "FY", "period", "chunks", "chars", "updated"],
      d.documents.map((x) => [x.doc_id, x.title, x.kind, x.ticker, x.fiscal_year, x.fiscal_period, x.chunks, x.chars, x.updated_at]),
      { onRow: (i) => { location.hash = `#/agent/doc/${encodeURIComponent(d.documents[i].doc_id)}`; }, maxHeight: "520px" })));
}

function embeddingBars(head) { // first dims of the vector, diverging around 0
  const W = 120, H = 26, bw = W / head.length, max = Math.max(0.05, ...head.map(Math.abs));
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, width: W, height: H, class: "chart" }, s("line", { class: "axis", x1: 0, x2: W, y1: H / 2, y2: H / 2 }));
  head.forEach((v, i) => {
    const hh = ((H / 2 - 1) * Math.abs(v)) / max;
    const r = s("rect", { x: i * bw + 1, y: v >= 0 ? H / 2 - hh : H / 2, width: bw - 2, height: Math.max(1, hh), rx: 1.5, fill: v >= 0 ? "var(--div-pos)" : "var(--div-neg)" });
    tipOn(r, [`dimension ${i + 1}`, v.toFixed(4)]);
    svg.append(r);
  });
  return svg;
}

async function viewDoc(docId) {
  const d = await api("agent/document", { doc_id: docId });
  const x = d.document;
  app.replaceChildren(page(
    h("div", { class: "crumbs" }, link("#/agent", "Agent knowledge"), " / ", h("span", { text: x.kind })),
    h("h1", { text: x.title }), h("p", { class: "mono muted", text: x.doc_id }),
    kv([["kind", x.kind], ["ticker / period", x.ticker ? `${x.ticker} · FY${x.fiscal_year} ${x.fiscal_period}` : "–"],
      ["source file", d.local_file ? link(`#/files/local/${d.local_file}`, d.local_file) : x.path], ["content hash", h("code", { text: x.content_hash })],
      ["updated", x.updated_at], ["metadata", h("code", { text: String(x.metadata) })]]),
    h("h2", { text: `Retrieved by agent runs (${d.retrieved_by.length})` }),
    d.retrieved_by.length ? grid(["run", "agent", "step", "tool", "at"], d.retrieved_by.map((r) => [r.run_id, r.agent, r.step, r.tool_name, r.at]), { onRow: (i) => { location.hash = `#/runs/${d.retrieved_by[i].run_id}`; } })
      : h("p", { class: "muted", text: "No logged run retrieved this document (runs logged before the run-log fix can't be matched)." }),
    h("h2", { text: `Chunks (${d.chunks.length})` }),
    h("p", { class: "ink2", text: "A chunk is the unit that gets embedded and retrieved. The bars show the first 8 of 384 embedding dimensions; vectors are normalized (norm ≈ 1), so cosine similarity is a dot product." }),
    d.chunks.map((c) => h("div", { class: "card chunk" },
      h("div", { class: "row" }, h("b", { text: `#${c.chunk_no} · ${c.section || "(no section)"}` }), h("span", { class: "spacer" }),
        h("span", { class: "muted small", text: `${fmt.n(c.chars)} chars · ${fmt.n(c.lexemes)} lexemes · norm ${c.norm}` }), embeddingBars(c.head),
        link(`#/agent/chunk/${encodeURIComponent(c.chunk_id)}`, "nearest neighbours →")),
      h("pre", { text: c.content })))));
}

async function viewChunk(chunkId) {
  const d = await api("agent/chunk", { chunk_id: chunkId });
  const c = d.chunk;
  app.replaceChildren(page(
    h("div", { class: "crumbs" }, link("#/agent", "Agent knowledge"), " / ", link(`#/agent/doc/${encodeURIComponent(c.doc_id)}`, c.doc_id), " / ", h("span", { text: `#${c.chunk_no}` })),
    h("h1", { text: `${c.title} · ${c.section || ""}` }), h("p", { class: "mono muted", text: c.chunk_id }),
    h("pre", { text: c.content }),
    h("h2", { text: "Nearest neighbours by embedding" }),
    h("p", { class: "ink2", text: "The vector half of hybrid search, computed with the stored embedding and the same HNSW index (no model call). Full-text rank and RRF fusion add to this at query time." }),
    hbars(d.neighbours, { value: (n) => n.cosine, label: (n) => n.chunk_id, sub: (n) => `cosine ${n.cosine.toFixed(3)} · ${fmt.short(n.section, 40)}`, W: 760 }),
    grid(["chunk", "document", "section", "cosine"], d.neighbours.map((n) => [n.chunk_id, n.doc_id, n.section, n.cosine]), { onRow: (i) => { location.hash = `#/agent/chunk/${encodeURIComponent(d.neighbours[i].chunk_id)}`; } })));
}

// ---- view: agent runs ---------------------------------------------------------------------------

async function viewRuns(params) {
  const agent = params.get("agent") || "";
  const d = await api("agent/runs", { agent, limit: 200 });
  const sel = h("select", { onchange: (e) => { location.hash = e.target.value ? `#/runs?agent=${e.target.value}` : "#/runs"; } },
    ["", "qa", "triage"].map((a) => h("option", { value: a, text: a || "all agents", selected: a === agent })));
  const legacy = d.runs.filter((r) => r.legacy).length;
  app.replaceChildren(page(
    h("h1", { text: "Agent runs" }),
    h("p", { class: "ink2", text: "Every run of the QA and triage agents from agent.run_log: the question, each model step, each tool call and the chunks each search returned." }),
    h("div", { class: "row" }, sel, h("span", { class: "muted", text: `${fmt.n(d.runs.length)} runs` })),
    legacy ? h("div", { class: "banner", text: `${legacy} older runs were logged before the run-log fix: their tool results were scrubbed into flat text, so their retrieved chunks can't be shown. New runs log structured results.` }) : null,
    grid(["started", "agent", "question / incident", "steps", "tool calls", "searches", "tokens", "stopped by", "log"],
      d.runs.map((r) => [r.started_at, r.agent, fmt.short(r.question, 90), r.steps, r.tool_calls, r.searches,
        r.prompt_tokens === null ? null : r.prompt_tokens + r.completion_tokens, r.stopped_by, r.legacy ? "before fix" : "structured"]),
      { onRow: (i) => { location.hash = `#/runs/${d.runs[i].run_id}`; }, maxHeight: "calc(100vh - 260px)",
        cell: (i, v) => (i === 8 ? h("td", {}, h("span", { class: `badge ${v === "structured" ? "st-ok" : "st-warn"}`, text: v })) : null) })));
}

function eventSummary(e) {
  const p = e.payload || {};
  if (p.text && Object.keys(p).length === 1) return fmt.short(p.text, 220);
  if (e.event === "start") return p.user;
  if (e.event === "llm") return p.tool_calls?.length ? `→ calls ${p.tool_calls.map((c) => c.function?.name).join(", ")}${p.content ? " · " + fmt.short(p.content, 120) : ""}` : fmt.short(p.content || "(reasoning only)", 220);
  if (e.event === "tool") return Array.isArray(p.output) ? `${p.output.length} results · args ${JSON.stringify(p.arguments)}` : `args ${JSON.stringify(p.arguments)} → ${fmt.short(typeof p.output === "string" ? p.output : JSON.stringify(p.output), 160)}`;
  if (e.event === "final") return `${p.stopped_by} · ${fmt.short(p.answer, 220)}`;
  if (e.event === "guardrail") return p.reason;
  return "";
}

async function viewRun(runId) {
  const d = await api("agent/run", { run_id: runId });
  const start = d.events.find((e) => e.event === "start")?.payload || {};
  const fin = d.events.find((e) => e.event === "final")?.payload || {};
  const calls = Object.values(d.retrieved.reduce((acc, r) => { (acc[r.event_id] ||= { ...r, hits: [] }).hits.push(r); return acc; }, {}));
  app.replaceChildren(page(
    h("div", { class: "crumbs" }, link("#/runs", "Agent runs"), " / ", h("span", { class: "mono", text: runId })),
    h("div", { class: "row" }, h("h1", { text: `${d.agent} run` }), storeBadge("app")),
    h("pre", { text: start.user || start.text || "(question not logged)" }),
    tiles([["model steps", fmt.n(d.events.filter((e) => e.event === "llm").length)], ["tool calls", fmt.n(d.events.filter((e) => e.event === "tool").length)],
      ["chunks retrieved", fmt.n(d.retrieved.length)], ["tokens", fin.usage ? fmt.n(fin.usage.prompt_tokens + fin.usage.completion_tokens) : "–"],
      ["seconds", fin.seconds ?? "–"], ["stopped by", fin.stopped_by || "–"]]),
    d.legacy ? h("div", { class: "banner", text: "This run was logged before the run-log fix: tool results were scrubbed as flat text, so the retrieved chunks can't be matched. The raw events are below." }) : null,
    fin.answer ? h("div", {}, h("h2", { text: "Answer" }), h("pre", { text: fin.answer })) : null,
    h("h2", { text: `Retrieved chunks (${d.retrieved.length})` }),
    calls.length ? calls.map((c) => h("div", { class: "card chunk" },
      h("div", { class: "row" }, h("b", { text: `step ${c.step} · ${c.tool}` }), h("code", { text: JSON.stringify(c.query) })),
      grid(["rank", "document", "section", "RRF score", "chunk", "chars"], c.hits.map((x) => [x.rank, x.doc_id, x.section, x.score ?? null, x.chunk_id || "not in index now", x.chars]),
        { cell: (i, v, ri) => (i === 4 && c.hits[ri].chunk_id ? h("td", {}, link(`#/agent/chunk/${encodeURIComponent(v)}`, v)) : i === 1 ? h("td", {}, link(`#/agent/doc/${encodeURIComponent(v)}`, v)) : null) })))
      : h("p", { class: "muted", text: d.legacy ? "Not recoverable for this run." : "This run made no search calls." }),
    h("h2", { text: `Timeline (${d.events.length} events)` }),
    d.events.map((e) => h("div", { class: `ev e-${e.event}` },
      h("div", { class: "row" }, h("span", { class: "badge", text: e.event }), e.tool_name ? h("b", { class: "mono", text: e.tool_name }) : null,
        h("span", { class: "muted small", text: `step ${e.step ?? "–"} · ${e.at}` })),
      h("div", { class: "ink2", text: eventSummary(e) }),
      h("details", {}, h("summary", { text: "payload" }), h("pre", { text: JSON.stringify(e.payload, null, 2) }))))));
}

// ---- router -------------------------------------------------------------------------------------

function showError(err) {
  app.replaceChildren(page(h("div", { class: "banner error" }, h("b", { text: "Couldn't load this view. " }), err.message)));
}

async function render() {
  hideTip();
  const raw = location.hash.replace(/^#\/?/, "");
  const [pathPart, query = ""] = raw.split("?");
  const parts = pathPart.split("/").map(decodeURIComponent);
  const params = new URLSearchParams(query);
  const tab = { "": "map", storage: "storage", files: "files", iceberg: "files", pg: "map", agent: "agent", runs: "runs" }[parts[0]] || "map";
  document.querySelectorAll("#nav a").forEach((a) => a.classList.toggle("on", a.dataset.tab === tab));
  app.replaceChildren(page(h("p", { class: "muted", text: "loading…" })));
  try {
    switch (parts[0]) {
      case "": return await viewMap();
      case "storage": return await viewStorage();
      case "files": return await viewFiles(parts[1] || "s3", parts.slice(2).join("/"));
      case "iceberg": return await viewIceberg(parts[1]);
      case "pg": return await viewPg(parts[1]);
      case "agent": return parts[1] === "doc" ? await viewDoc(parts.slice(2).join("/")) : parts[1] === "chunk" ? await viewChunk(parts.slice(2).join("/")) : await viewAgent();
      case "runs": return parts[1] ? await viewRun(parts[1]) : await viewRuns(params);
      default: return showError(new Error(`unknown page ${parts[0]}`));
    }
  } catch (err) { showError(err); }
}

addEventListener("hashchange", render);
render();
