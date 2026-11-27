"""Read-only local data browser: a lineage map of the lakehouse with drill-down into every store.

    make browse   ->  http://localhost:8501

The UI is static files in scripts/data_browser_ui/ (vanilla JS + inline SVG, no build step). This server
serves them plus a read-only JSON API:

  /api/flow                          lineage map, live stats per node, latest reconciliation per hop
  /api/pg?schema=&table=             Postgres columns, size, first rows
  /api/iceberg?table=ns.name         schema, partition spec, snapshots, manifests, partitions, files, rows
  /api/files?root=s3|local&path=     directory listing, or a file preview (Parquet footer + rows, text)
  /api/dirsize?root=&path=           recursive object count and size under a prefix
  /api/agent/documents?kind=&ticker=&q=     /api/agent/document?doc_id=     /api/agent/chunk?chunk_id=
  /api/agent/runs?agent=             /api/agent/run?run_id=  (event timeline + the chunks each search returned)

Laptop tool only: no auth, bound to 127.0.0.1 by docker-compose. Postgres sessions are read-only
(default_transaction_read_only) with a statement timeout; table names are checked against the catalog
first; local paths can't leave data/. The page itself never renders data as HTML (textContent only).
"""

import json
import math
import mimetypes
import os
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import psycopg
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.rows import dict_row
from pyarrow import fs

from lakehouse.config import settings
from lakehouse.iceberg.catalog import catalog
from lakehouse.lineage import attach_reconciliation, build_graph

REPO = Path(__file__).resolve().parents[1]
UI_DIR = Path(__file__).resolve().parent / "data_browser_ui"
UI_FILES = {"index.html", "app.js", "app.css"}
MANIFEST = Path(os.getenv("DBT_MANIFEST", REPO / "dbt" / "target" / "manifest.json"))
DATA_DIR = Path(settings().data_dir).resolve()
BUCKET = "lake"
PG_SCHEMAS = ("ops", "lake_silver", "staging", "intermediate", "marts", "agent")
CELL_MAX = 400
FLOW_TTL_S = 15
DIRSIZE_MAX = 200_000


class NotFound(Exception):
    pass


class Forbidden(Exception):
    pass


# ---- helpers ------------------------------------------------------------------------------------

def pg() -> psycopg.Connection:
    s = settings()
    return psycopg.connect(
        host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user="dbt", password=os.environ["DBT_PASSWORD"],
        autocommit=True, connect_timeout=5, row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
    )


def cell(v: object, limit: int = CELL_MAX) -> object:
    """One value -> something JSON can carry. Long text is cut (the full value has its own endpoint)."""
    if v is None or isinstance(v, bool | int):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else str(v)  # NaN/inf are not valid JSON
    if isinstance(v, Decimal):
        return float(v) if v.is_finite() else str(v)
    if isinstance(v, bytes):
        return f"<{len(v):,} bytes>"
    if isinstance(v, dict | list):
        v = json.dumps(v, default=str)
    t = str(v)
    return t if len(t) <= limit else t[:limit] + f"… ({len(t):,} chars)"


def rows_out(rows: list[dict] | list[tuple], cols: list[str]) -> list[list]:
    return [[cell(r[c] if isinstance(r, dict) else r[i]) for i, c in enumerate(cols)] for r in rows]


def iso(ms: int | None) -> str | None:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="seconds") if ms else None


def filesystem(root: str) -> tuple[fs.FileSystem, str]:
    if root == "local":
        return fs.LocalFileSystem(), str(DATA_DIR)
    if root != "s3":
        raise NotFound(f"unknown root {root!r}")
    s = settings()
    return fs.S3FileSystem(endpoint_override=s.s3_endpoint, access_key=s.s3_access_key,
                           secret_key=s.s3_secret_key.get_secret_value(), region=s.s3_region), BUCKET


def resolve(root: str, path: str) -> tuple[fs.FileSystem, str, str]:
    fsys, base = filesystem(root)
    path = path.strip("/")
    full = f"{base}/{path}" if path else base
    if root == "local" and not Path(full).resolve().is_relative_to(DATA_DIR):
        raise Forbidden("outside data/")
    return fsys, base, full


# ---- flow map -----------------------------------------------------------------------------------

_flow_cache: dict = {"at": 0.0, "value": None}
_flow_lock = threading.Lock()


def flow() -> dict:
    with _flow_lock:
        if _flow_cache["value"] is None or time.monotonic() - _flow_cache["at"] > FLOW_TTL_S:
            _flow_cache.update(value=_build_flow(), at=time.monotonic())
        return _flow_cache["value"]


def _build_flow() -> dict:
    manifest = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else None
    g = build_graph(manifest)
    nodes = {n["id"]: n for n in g["nodes"]}
    warn = g["warnings"]

    def guarded(label: str, fn) -> None:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - one unreachable store must not blank the whole map
            warn.append(f"{label}: {type(exc).__name__}: {str(exc)[:200]}")

    def iceberg_stats() -> None:
        cat = catalog()
        for nid, n in nodes.items():
            if n["store"] != "s3":
                continue
            name = nid.removeprefix("iceberg:")
            if not cat.table_exists(name):
                n["stats"] = {"missing": True}
                continue
            t = cat.load_table(name)
            snap = t.current_snapshot()
            summ = snap.summary.additional_properties if snap and snap.summary else {}
            n["stats"] = {"records": int(summ.get("total-records", 0)), "files": int(summ.get("total-data-files", 0)),
                          "bytes": int(summ.get("total-files-size", 0)), "snapshots": len(t.metadata.snapshots),
                          "updated": iso(snap.timestamp_ms if snap else None)}
            n["location"] = t.location()

    def pg_stats() -> None:
        with pg() as conn:
            rels = conn.execute(
                "select n.nspname as schema, c.relname as name, c.relkind as kind, "
                "pg_total_relation_size(c.oid) as bytes from pg_class c join pg_namespace n on n.oid = c.relnamespace "
                "where n.nspname = any(%s) and c.relkind in ('r', 'v', 'm', 'p')", (list(PG_SCHEMAS),)).fetchall()
            for r in rels:
                nid = f"pg:{r['schema']}.{r['name']}"
                if nid not in nodes:
                    continue
                n = conn.execute(sql.SQL("select count(*) as n from {}.{}").format(
                    sql.Identifier(r["schema"]), sql.Identifier(r["name"]))).fetchone()["n"]
                nodes[nid]["stats"] = {"records": n, "bytes": r["bytes"],
                                       "kind": {"r": "table", "v": "view", "m": "matview", "p": "table"}[r["kind"]]}
            for r in conn.execute(
                    "select topic, max(written_at) as last, sum(events) filter (where written_at > now() - "
                    "interval '15 minutes') as events_15m, sum(late_events) filter (where written_at > now() - "
                    "interval '15 minutes') as late_15m from ops.stream_metrics group by topic"):
                n = nodes.get(f"kafka:{r['topic']}")
                if n:
                    n["stats"] = {"updated": cell(r["last"]), "events_15m": int(r["events_15m"] or 0),
                                  "late_15m": int(r["late_15m"] or 0)}
            for nid, source in (("ext:yfinance_daily", "yfinance"), ("ext:sec_edgar", "sec_edgar")):
                r = conn.execute("select status, finished_at, dag_id from ops.pipeline_run where source = %s "
                                 "order by started_at desc limit 1", (source,)).fetchone()
                if r:
                    nodes[nid]["stats"] = {"last_run": r["status"], "updated": cell(r["finished_at"]),
                                           "dag": r["dag_id"]}
            for r in conn.execute("select agent, count(*) as runs, max(created_at) as last from agent.run_log "
                                  "where event = 'start' group by agent"):
                n = nodes.get(f"app:{r['agent']}")
                if n:
                    n["stats"] = {"runs": r["runs"], "updated": cell(r["last"])}
            latest = conn.execute(
                "select distinct on (source, entity, layer_from, layer_to) source, entity, layer_from, layer_to, "
                "count_from, count_to, missing, duplicates, late, status, checked_at::text as checked_at, "
                "count(*) filter (where status <> 'ok' and checked_at > now() - interval '7 days') "
                "over (partition by source, entity, layer_from, layer_to) as failed_7d "
                "from ops.reconciliation order by source, entity, layer_from, layer_to, checked_at desc").fetchall()
            attach_reconciliation(g["edges"], latest)

    def local_stats() -> None:
        for nid in ("file:raw", "file:fact_sheets"):
            d = DATA_DIR / nid.removeprefix("file:")
            files = [p for p in d.rglob("*") if p.is_file()] if d.exists() else []
            nodes[nid]["stats"] = {"files": len(files), "bytes": sum(p.stat().st_size for p in files)}
            nodes[nid]["path"] = nid.removeprefix("file:")

    guarded("Iceberg", iceberg_stats)
    guarded("Postgres", pg_stats)
    guarded("local files", local_stats)
    g["generated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    return g


# ---- Postgres -----------------------------------------------------------------------------------

def pg_table(schema: str, table: str, limit: int, offset: int) -> dict:
    if schema not in PG_SCHEMAS:
        raise NotFound("schema not browsable")
    with pg() as conn:
        cols = conn.execute(
            "select column_name, data_type, is_nullable from information_schema.columns where table_schema = %s "
            "and table_name = %s order by ordinal_position", (schema, table)).fetchall()
        if not cols:
            raise NotFound("no such table")
        ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        names = [c["column_name"] for c in cols if c["data_type"] != "USER-DEFINED"]  # skip raw vectors/tsvector
        select = sql.SQL(", ").join(sql.Identifier(c) for c in names)
        rows = conn.execute(sql.SQL("select {} from {} limit {} offset {}").format(
            select, ident, sql.Literal(limit), sql.Literal(offset))).fetchall()
        total = conn.execute(sql.SQL("select count(*) as n from {}").format(ident)).fetchone()["n"]
        meta = conn.execute(
            "select pg_total_relation_size(c.oid) as bytes, c.relkind as kind, obj_description(c.oid) as comment "
            "from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = %s and c.relname = %s",
            (schema, table)).fetchone()
        keys = conn.execute(
            "select a.attname as col from pg_index i join pg_class c on c.oid = i.indrelid join pg_namespace n "
            "on n.oid = c.relnamespace join pg_attribute a on a.attrelid = c.oid and a.attnum = any(i.indkey) "
            "where i.indisprimary and n.nspname = %s and c.relname = %s", (schema, table)).fetchall()
    hidden = [c["column_name"] for c in cols if c["data_type"] == "USER-DEFINED"]
    return {"schema": schema, "table": table, "columns": cols, "primary_key": [k["col"] for k in keys],
            "total": total, "bytes": meta["bytes"], "kind": meta["kind"], "comment": meta["comment"],
            "hidden_columns": hidden, "offset": offset, "names": names, "rows": rows_out(rows, names),
            "node": f"pg:{schema}.{table}"}


# ---- Iceberg ------------------------------------------------------------------------------------

SIZE_BINS = ((64 << 10, "< 64 KB"), (256 << 10, "64–256 KB"), (1 << 20, "256 KB–1 MB"), (8 << 20, "1–8 MB"),
             (64 << 20, "8–64 MB"), (512 << 20, "64–512 MB"), (None, "≥ 512 MB"))


def size_histogram(sizes: list[int]) -> list[dict]:
    """Data files per size bucket: many tiny files = the streaming small-file problem compaction fixes."""
    bins = [{"label": label, "files": 0, "bytes": 0} for _, label in SIZE_BINS]
    for s in sizes:
        i = next(i for i, (upper, _) in enumerate(SIZE_BINS) if upper is None or s < upper)
        bins[i]["files"] += 1
        bins[i]["bytes"] += s
    return bins

def iceberg_table(name: str, limit: int) -> dict:
    cat = catalog()
    if not cat.table_exists(name):
        raise NotFound("no such table")
    t = cat.load_table(name)
    sch = t.schema()
    out: dict = {
        "table": name, "node": f"iceberg:{name}", "location": t.location(), "format_version": t.format_version,
        "metadata_location": t.metadata_location, "properties": dict(t.properties),
        "schema": [{"id": f.field_id, "name": f.name, "type": str(f.field_type), "required": f.required}
                   for f in sch.fields],
        "partition_spec": [{"name": f.name, "transform": str(f.transform), "source": sch.find_field(f.source_id).name}
                           for f in t.spec().fields],
        "sort_order": [str(f) for f in t.sort_order().fields],
        "snapshots": [{"id": str(s.snapshot_id), "parent": str(s.parent_snapshot_id or ""), "at": iso(s.timestamp_ms),
                       "operation": s.summary.operation.value if s.summary else "",
                       "manifest_list": s.manifest_list,
                       "summary": dict(s.summary.additional_properties) if s.summary else {}}
                      for s in reversed(t.metadata.snapshots[-30:])],
        "current_snapshot": str(t.current_snapshot().snapshot_id) if t.current_snapshot() else None,
    }
    if not t.current_snapshot():
        return out
    m = t.inspect.manifests()
    out["manifests"] = [{"path": p, "bytes": n, "added": a, "existing": e, "deleted": d}
                        for p, n, a, e, d in zip(*(m.column(c).to_pylist() for c in (
                            "path", "length", "added_data_files_count", "existing_data_files_count",
                            "deleted_data_files_count")), strict=True)]
    try:
        p = t.inspect.partitions().to_pylist()
        out["partitions"] = sorted(
            ({"partition": cell(r.get("partition"), 120), "records": r["record_count"], "files": r["file_count"],
              "bytes": r["total_data_file_size_in_bytes"], "updated": cell(r.get("last_updated_at"))}
             for r in p), key=lambda r: str(r["partition"]))[-200:]
    except Exception as exc:  # noqa: BLE001
        out["partitions_error"] = f"{type(exc).__name__}: {exc}"[:300]
    f = t.inspect.files().select(["file_path", "file_format", "record_count", "file_size_in_bytes", "partition"])
    out["files_total"] = f.num_rows
    sizes = f.column("file_size_in_bytes").to_pylist()
    out["data_bytes"] = sum(sizes)
    out["size_histogram"] = size_histogram(sizes)
    out["files"] = [{"path": r["file_path"], "format": r["file_format"], "records": r["record_count"],
                     "bytes": r["file_size_in_bytes"], "partition": cell(r["partition"], 120)}
                    for r in f.slice(0, 300).to_pylist()]
    rows = t.scan(limit=limit).to_arrow()
    out["names"] = rows.column_names
    out["rows"] = [[cell(v) for v in r.values()] for r in rows.to_pylist()]
    return out


# ---- files --------------------------------------------------------------------------------------

def _owner_table(root: str, full: str) -> str | None:
    """The Iceberg table whose location contains this S3 path (from the cached flow map)."""
    if root != "s3":
        return None
    s3 = f"s3://{full}"
    for n in flow()["nodes"]:
        loc = n.get("location")
        if loc and (s3 == loc or s3.startswith(loc.rstrip("/") + "/")):
            return n["id"].removeprefix("iceberg:")
    return None


def files(root: str, path: str, limit: int) -> dict:
    fsys, base, full = resolve(root, path)
    info = fsys.get_file_info(full)
    rel = full.removeprefix(base).strip("/")
    out = {"root": root, "base": base, "path": rel, "table": _owner_table(root, full)}
    if info.type == fs.FileType.Directory:
        entries = sorted(fsys.get_file_info(fs.FileSelector(full)),
                         key=lambda e: (e.type != fs.FileType.Directory, e.base_name))
        out["type"] = "dir"
        out["total"] = len(entries)
        out["entries"] = [{"name": e.base_name, "dir": e.type == fs.FileType.Directory,
                           "path": e.path.removeprefix(base).strip("/"),
                           "bytes": None if e.type == fs.FileType.Directory else e.size,
                           "modified": e.mtime.isoformat(timespec="seconds") if e.mtime else None}
                          for e in entries[:1000]]
        return out
    if info.type != fs.FileType.File:
        raise NotFound("no such file")
    out.update(type="file", bytes=info.size, modified=info.mtime.isoformat(timespec="seconds") if info.mtime else None)
    name = full.rsplit("/", 1)[-1]
    if name.endswith(".parquet"):
        out["preview"] = _parquet(fsys, full, limit)
    elif name.endswith((".json", ".csv", ".jsonl", ".txt", ".md", ".log")) or "/checkpoints/" in f"/{rel}/":
        with fsys.open_input_stream(full) as f:
            raw = f.read(64_000)
        text = raw.decode("utf-8", "replace")
        kind = "text"
        if name.endswith(".json"):
            try:
                text, kind = json.dumps(json.loads(text), indent=2)[:200_000], "json"
            except ValueError:
                pass
        out["preview"] = {"kind": kind, "text": text, "cut": info.size > 64_000}
    elif name.endswith(".avro"):
        out["preview"] = {"kind": "avro", "role": "manifest list" if name.startswith("snap-") else "manifest"}
    else:
        out["preview"] = {"kind": "binary"}
    return out


def _parquet(fsys: fs.FileSystem, full: str, limit: int) -> dict:
    with fsys.open_input_file(full) as f:
        pf = pq.ParquetFile(f)
        md = pf.metadata
        groups = []
        for i in range(min(md.num_row_groups, 50)):
            rg = md.row_group(i)
            groups.append({"rows": rg.num_rows, "bytes": rg.total_byte_size,
                           "compressed": sum(rg.column(j).total_compressed_size for j in range(rg.num_columns))})
        columns = []
        if md.num_row_groups:
            rg = md.row_group(0)
            for j in range(rg.num_columns):
                c = rg.column(j)
                st = c.statistics if c.is_stats_set else None
                columns.append({"name": c.path_in_schema, "type": str(c.physical_type), "codec": c.compression,
                                "encodings": [str(e) for e in c.encodings], "compressed": c.total_compressed_size,
                                "uncompressed": c.total_uncompressed_size,
                                "min": cell(st.min, 60) if st is not None and st.has_min_max else None,
                                "max": cell(st.max, 60) if st is not None and st.has_min_max else None,
                                "nulls": st.null_count if st is not None else None})
        batch = next(pf.iter_batches(batch_size=limit), None)
        return {"kind": "parquet", "rows_total": md.num_rows, "row_groups": groups, "num_row_groups": md.num_row_groups,
                "created_by": md.created_by, "columns": columns, "names": batch.schema.names if batch else [],
                "rows": [[cell(v) for v in r.values()] for r in batch.to_pylist()] if batch else []}


def dirsize(root: str, path: str) -> dict:
    fsys, _, full = resolve(root, path)
    started = time.monotonic()
    infos = fsys.get_file_info(fs.FileSelector(full, recursive=True))
    files_ = [i for i in infos if i.type == fs.FileType.File]
    by_ext: dict[str, list[int]] = {}
    for i in files_[:DIRSIZE_MAX]:
        ext = i.base_name.rsplit(".", 1)[-1] if "." in i.base_name else "(none)"
        acc = by_ext.setdefault(ext, [0, 0])
        acc[0] += 1
        acc[1] += i.size
    return {"files": len(files_), "bytes": sum(i.size for i in files_), "capped": len(files_) > DIRSIZE_MAX,
            "by_ext": sorted(({"ext": k, "files": v[0], "bytes": v[1]} for k, v in by_ext.items()),
                             key=lambda r: -r["bytes"]), "seconds": round(time.monotonic() - started, 2)}


# ---- agent: documents, chunks, runs -------------------------------------------------------------

def agent_documents(kind: str | None, ticker: str | None, q: str | None, limit: int) -> dict:
    where, args = ["true"], {}
    if kind:
        where.append("d.kind = %(kind)s")
        args["kind"] = kind
    if ticker:
        where.append("d.ticker = %(ticker)s")
        args["ticker"] = ticker.upper()
    if q:
        where.append("(d.doc_id ilike %(q)s or d.title ilike %(q)s)")
        args["q"] = f"%{q}%"
    args["limit"] = limit
    with pg() as conn:
        docs = conn.execute(
            "select d.doc_id, d.kind, d.ticker, d.fiscal_year, d.fiscal_period, d.title, d.path, "
            "d.updated_at::text as updated_at, count(c.chunk_id) as chunks, "
            "coalesce(sum(length(c.content)), 0) as chars "
            "from agent.documents d left join agent.chunks c using (doc_id) where " + " and ".join(where) +
            " group by d.doc_id order by d.kind, d.ticker nulls first, d.fiscal_year desc, d.fiscal_period desc "
            "limit %(limit)s", args).fetchall()
        facets = conn.execute("select kind, ticker, count(*) as n from agent.documents group by 1, 2 "
                              "order by 1, 2").fetchall()
        totals = conn.execute("select (select count(*) from agent.documents) as documents, "
                              "(select count(*) from agent.chunks) as chunks").fetchone()
    return {"documents": docs, "facets": facets, "totals": totals}


def agent_document(doc_id: str) -> dict:
    with pg() as conn:
        doc = conn.execute("select doc_id, kind, ticker, fiscal_year, fiscal_period, title, path, content_hash, "
                           "metadata, updated_at::text as updated_at from agent.documents where doc_id = %s",
                           (doc_id,)).fetchone()
        if not doc:
            raise NotFound("no such document")
        chunks = conn.execute(
            "select chunk_id, chunk_no, section, content, length(content) as chars, "
            "array_length(regexp_split_to_array(tsv::text, ' '), 1) as lexemes, "
            "round(vector_norm(embedding)::numeric, 4) as norm, (embedding::real[])[1:8] as head "
            "from agent.chunks where doc_id = %s order by chunk_no", (doc_id,)).fetchall()
        used = conn.execute(
            "select l.run_id, l.agent, l.step, l.tool_name, l.created_at::text as at from agent.run_log l "
            "where l.event = 'tool' and jsonb_typeof(l.payload->'output') = 'array' "
            "and l.payload->'output' @> jsonb_build_array(jsonb_build_object('doc_id', %s::text)) "
            "order by l.id desc limit 50", (doc_id,)).fetchall()
    local = doc["path"] if doc["kind"] == "fact_sheet" and (DATA_DIR / doc["path"]).exists() else None
    return {"document": {k: cell(v, 20_000) if k == "metadata" else v for k, v in doc.items()},
            "chunks": [{**c, "norm": float(c["norm"]), "head": [round(x, 4) for x in c["head"]]} for c in chunks],
            "retrieved_by": used, "local_file": local}


def agent_chunk(chunk_id: str) -> dict:
    with pg() as conn:
        c = conn.execute("select c.chunk_id, c.doc_id, c.chunk_no, c.section, c.content, d.title, d.kind, d.ticker "
                         "from agent.chunks c join agent.documents d using (doc_id) where c.chunk_id = %s",
                         (chunk_id,)).fetchone()
        if not c:
            raise NotFound("no such chunk")
        # Nearest neighbours by the stored embedding: no model needed, the same HNSW index the agents use.
        near = conn.execute(
            "select c.chunk_id, c.doc_id, c.section, round((1 - (c.embedding <=> t.embedding))::numeric, 4) as cosine "
            "from agent.chunks c, (select embedding from agent.chunks where chunk_id = %s) t "
            "where c.chunk_id <> %s order by c.embedding <=> t.embedding limit 8", (chunk_id, chunk_id)).fetchall()
    return {"chunk": c, "neighbours": [{**n, "cosine": float(n["cosine"])} for n in near]}


RUNS_SQL = """
select run_id, agent, min(created_at)::text as started_at, max(created_at)::text as ended_at,
       coalesce(max(payload->>'user') filter (where event = 'start'),
                max(left(payload->>'text', 300)) filter (where event = 'start')) as question,
       max(payload->>'stopped_by') filter (where event = 'final') as stopped_by,
       max(step) as steps,
       count(*) filter (where event = 'tool') as tool_calls,
       count(*) filter (where event = 'tool' and tool_name like 'search%%') as searches,
       max(case when payload #>> '{usage,prompt_tokens}' ~ '^[0-9]+$'
                then (payload #>> '{usage,prompt_tokens}')::bigint end) filter (where event = 'final') as prompt_tokens,
       max(case when payload #>> '{usage,completion_tokens}' ~ '^[0-9]+$'
                then (payload #>> '{usage,completion_tokens}')::bigint end) filter (where event = 'final')
           as completion_tokens,
       bool_or(event = 'tool' and not payload ? 'output_chars') as legacy
from agent.run_log
where %(agent)s::text is null or agent = %(agent)s::text
group by run_id, agent
order by min(created_at) desc
limit %(limit)s
"""


def agent_runs(agent: str | None, limit: int) -> dict:
    with pg() as conn:
        runs = conn.execute(RUNS_SQL, {"agent": agent or None, "limit": limit}).fetchall()
    return {"runs": runs}


def agent_run(run_id: str) -> dict:
    with pg() as conn:
        events = conn.execute("select id, agent, step, event, tool_name, payload, created_at::text as at "
                              "from agent.run_log where run_id = %s order by id", (run_id,)).fetchall()
        if not events:
            raise NotFound("no such run")
        retrieved = []
        for e in events:
            out = (e["payload"] or {}).get("output") if isinstance(e["payload"], dict) else None
            if e["event"] == "tool" and (e["tool_name"] or "").startswith("search") and isinstance(out, list):
                args = e["payload"].get("arguments")
                for rank, hit in enumerate(out, start=1):
                    if isinstance(hit, dict) and "doc_id" in hit:
                        retrieved.append({"event_id": e["id"], "step": e["step"], "tool": e["tool_name"],
                                          "query": args, "rank": rank, "doc_id": hit.get("doc_id"),
                                          "section": hit.get("section"), "score": hit.get("score")})
        if retrieved:  # (doc_id, section) is unique in agent.chunks, so it identifies the chunk
            pairs = conn.execute(
                "select c.chunk_id, c.doc_id, c.section, length(c.content) as chars from agent.chunks c "
                "join unnest(%s::text[], %s::text[]) as k(doc_id, section) using (doc_id, section)",
                ([r["doc_id"] for r in retrieved], [r["section"] or "" for r in retrieved])).fetchall()
            ids = {(p["doc_id"], p["section"]): p for p in pairs}
            for r in retrieved:
                p = ids.get((r["doc_id"], r["section"]))
                r["chunk_id"], r["chars"] = (p["chunk_id"], p["chars"]) if p else (None, None)
    legacy = any(e["event"] == "tool" and isinstance(e["payload"], dict) and "output_chars" not in e["payload"]
                 for e in events)
    return {"run_id": run_id, "agent": events[0]["agent"], "events": events,
            "retrieved": retrieved, "legacy": legacy}


# ---- HTTP ---------------------------------------------------------------------------------------

LEGACY = {"/pg": lambda q: f"#/pg/{q.get('schema', '')}.{q.get('table', '')}",
          "/iceberg": lambda q: f"#/iceberg/{q.get('table', '')}",
          "/files": lambda q: f"#/files/{q.get('root', 's3')}/{q.get('path', '')}"}


def route(path: str, q: dict[str, str]) -> object:
    limit = max(1, min(int(q.get("limit", 25)), 500))
    match path:
        case "/api/flow":
            return flow()
        case "/api/pg":
            return pg_table(q.get("schema", ""), q.get("table", ""), limit, max(0, int(q.get("offset", 0))))
        case "/api/iceberg":
            return iceberg_table(q.get("table", ""), limit)
        case "/api/files":
            return files(q.get("root", "s3"), q.get("path", ""), limit)
        case "/api/dirsize":
            return dirsize(q.get("root", "s3"), q.get("path", ""))
        case "/api/agent/documents":
            return agent_documents(q.get("kind"), q.get("ticker"), q.get("q"), max(limit, 400))
        case "/api/agent/document":
            return agent_document(q.get("doc_id", ""))
        case "/api/agent/chunk":
            return agent_chunk(q.get("chunk_id", ""))
        case "/api/agent/runs":
            return agent_runs(q.get("agent"), max(limit, 200))
        case "/api/agent/run":
            return agent_run(q.get("run_id", ""))
    raise NotFound(path)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # quieter: only errors
        if args and str(args[1])[:1] in "45":
            super().log_message(fmt, *args)

    def send(self, status: int, body: bytes, ctype: str, extra: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; base-uri 'none'")
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path in LEGACY:  # old bookmarks and the runbooks' links keep working
            return self.send(302, b"", "text/plain", {"Location": "/" + quote(LEGACY[url.path](q), safe="#/.:=&?")})
        if url.path == "/" or url.path.startswith("/static/"):
            name = "index.html" if url.path == "/" else url.path.removeprefix("/static/")
            if name not in UI_FILES:
                return self.send(404, b"not found", "text/plain")
            ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
            return self.send(200, (UI_DIR / name).read_bytes(), f"{ctype}; charset=utf-8")
        try:
            status, body = 200, route(url.path, q)
        except NotFound as exc:
            status, body = 404, {"error": f"not found: {exc}"}
        except Forbidden as exc:
            status, body = 403, {"error": f"forbidden: {exc}"}
        except Exception as exc:  # noqa: BLE001 - show the error in the page instead of a dropped connection
            status, body = 500, {"error": f"{type(exc).__name__}: {str(exc)[:2000]}"}
        self.send(status, json.dumps(body, default=str).encode(), "application/json")


if __name__ == "__main__":
    port = int(os.getenv("BROWSER_PORT", "8501"))
    print(f"data browser on http://localhost:{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
