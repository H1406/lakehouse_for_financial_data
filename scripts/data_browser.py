"""Read-only local data browser: Postgres tables, Iceberg tables and the files behind them.

    make browse   ->  http://localhost:8501

Pages: /  overview · /pg?schema=&table=  · /iceberg?table=ns.name  · /files?root=s3|local&path=
Laptop tool only: no auth, bound to 127.0.0.1 by docker-compose. Postgres sessions are read-only
(default_transaction_read_only) with a statement timeout; table names are checked against the catalog first.
"""

import html
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import psycopg
import pyarrow.parquet as pq
from psycopg import sql
from pyarrow import fs

from lakehouse.config import settings
from lakehouse.iceberg.catalog import catalog

LIMIT = 20
CELL_MAX = 160
PG_SCHEMAS = ("ops", "lake_silver", "staging", "intermediate", "marts")
DATA_DIR = Path(settings().data_dir).resolve()
BUCKET = "lake"

CSS = """body{font:13px/1.4 system-ui,sans-serif;margin:20px;color:#222}a{color:#0b5cad;text-decoration:none}
table{border-collapse:collapse;margin:8px 0 18px}td,th{border:1px solid #ddd;padding:3px 7px;text-align:left;
vertical-align:top;max-width:420px;overflow-wrap:anywhere}th{background:#f3f4f6}h2{margin-top:26px}
code,pre{background:#f6f8fa;padding:1px 4px}pre{padding:8px;overflow:auto;max-height:480px}.muted{color:#777}"""


def pg() -> psycopg.Connection:
    s = settings()
    return psycopg.connect(
        host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user="dbt", password=os.environ["DBT_PASSWORD"],
        autocommit=True, connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
    )


def esc(v: object) -> str:
    t = "" if v is None else str(v)
    return html.escape(t if len(t) <= CELL_MAX else t[:CELL_MAX] + f"… ({len(t):,} chars)")


def grid(cols: list[str], rows: list) -> str:
    head = "".join(f"<th>{esc(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{esc(v)}</td>" for v in r) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>" if rows else "<p class=muted>(no rows)</p>"


def link(href: str, text: str) -> str:
    return f'<a href="{html.escape(href)}">{esc(text)}</a>'


# ---- pages --------------------------------------------------------------------------------------

def overview() -> str:
    out = ["<h2>Postgres <code>warehouse</code></h2>"]
    with pg() as conn:
        tables = conn.execute(
            "select table_schema, table_name, table_type from information_schema.tables "
            "where table_schema = any(%s) order by 1, 2", (list(PG_SCHEMAS),)).fetchall()
        rows = []
        for schema, name, kind in tables:
            try:
                n = conn.execute(sql.SQL("select count(*) from {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(name))).fetchone()[0]
            except psycopg.Error:
                n = "?"
            rows.append((link(f"/pg?schema={schema}&table={name}", f"{schema}.{name}"), kind.lower(), n))
    out.append(grid_raw(["table", "type", "rows"], rows))

    out.append("<h2>Iceberg (REST catalog <code>lake</code>)</h2>")
    cat, rows = catalog(), []
    for ns in sorted(cat.list_namespaces()):
        for ident in sorted(cat.list_tables(ns)):
            name = ".".join(ident)
            snap = cat.load_table(name).current_snapshot()
            summ = snap.summary.additional_properties if snap else {}
            rows.append((link(f"/iceberg?table={name}", name), summ.get("total-records", 0),
                         summ.get("total-data-files", 0), f"{int(summ.get('total-files-size', 0)) / 1e6:,.1f} MB"))
    out.append(grid_raw(["table", "records", "data files", "size"], rows))
    out.append(f"<h2>Files</h2><p>{link('/files?root=s3&path=', 's3://lake (SeaweedFS)')} · "
               f"{link('/files?root=local&path=', 'local data/ (staged raw API responses)')}</p>")
    return page("Lakehouse data browser", "".join(out))


def grid_raw(cols: list[str], rows: list) -> str:
    """Like grid() but the first column is pre-rendered HTML (a link)."""
    head = "".join(f"<th>{esc(c)}</th>" for c in cols)
    body = "".join(f"<tr><td>{r[0]}</td>" + "".join(f"<td>{esc(v)}</td>" for v in r[1:]) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def pg_table(schema: str, table: str, limit: int) -> str:
    with pg() as conn:
        cols = conn.execute(
            "select column_name, data_type from information_schema.columns where table_schema = %s "
            "and table_name = %s order by ordinal_position", (schema, table)).fetchall()
        if schema not in PG_SCHEMAS or not cols:
            return page("Not found", "<p>No such table.</p>", 404)
        cur = conn.execute(sql.SQL("select * from {}.{} limit {}").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Literal(limit)))
        rows = cur.fetchall()
    body = (f"<h2>Columns</h2>{grid(['column', 'type'], cols)}"
            f"<h2>First {limit} rows</h2>{grid([d.name for d in cur.description], rows)}")
    return page(f"{schema}.{table}", body)


def iceberg_table(name: str, limit: int) -> str:
    cat = catalog()
    if not cat.table_exists(name):
        return page("Not found", "<p>No such table.</p>", 404)
    t = cat.load_table(name)
    schema = [(f.field_id, f.name, str(f.field_type)) for f in t.schema().fields]
    spec = [(f.name, str(f.transform), t.schema().find_field(f.source_id).name) for f in t.spec().fields]
    snaps = [(s.snapshot_id, s.timestamp_ms, s.summary.operation.value if s.summary else "",
              (s.summary.additional_properties.get("added-records") if s.summary else ""))
             for s in reversed(t.metadata.snapshots[-10:])]
    body = [f"<p>location <code>{esc(t.location())}</code> · format v{t.format_version}</p>",
            "<h2>Schema</h2>", grid(["id", "column", "type"], schema),
            "<h2>Partition spec</h2>", grid(["partition", "transform", "source column"], spec),
            "<h2>Last 10 snapshots</h2>", grid(["snapshot id", "timestamp ms", "operation", "added records"], snaps)]
    if t.current_snapshot():
        files = t.inspect.files().select(["file_path", "record_count", "file_size_in_bytes"]).slice(0, 50)
        frows = [(link(f"/files?root=s3&path={quote(p.removeprefix(f's3://{BUCKET}/'))}", p.rsplit('/', 1)[-1]),
                  n, f"{b / 1e3:,.1f} KB")
                 for p, n, b in zip(*(files.column(c).to_pylist() for c in files.column_names), strict=True)]
        rows = t.scan(limit=limit).to_arrow()
        body += ["<h2>Data files (first 50)</h2>", grid_raw(["file", "records", "size"], frows),
                 f"<h2>First {limit} rows</h2>", grid(rows.column_names, [tuple(r.values()) for r in rows.to_pylist()])]
    return page(f"iceberg: {name}", "".join(body))


def filesystem(root: str) -> tuple[fs.FileSystem, str]:
    if root == "local":
        return fs.LocalFileSystem(), str(DATA_DIR)
    s = settings()
    return fs.S3FileSystem(endpoint_override=s.s3_endpoint, access_key=s.s3_access_key,
                           secret_key=s.s3_secret_key.get_secret_value(), region=s.s3_region), BUCKET


def files(root: str, path: str, limit: int) -> str:
    fsys, base = filesystem(root)
    full = f"{base}/{path}".rstrip("/") if path else base
    if root == "local" and not Path(full).resolve().is_relative_to(DATA_DIR):
        return page("Forbidden", "<p>Outside data/.</p>", 403)
    info = fsys.get_file_info(full)
    crumbs = link(f"/files?root={root}&path=", base)
    acc = ""
    for part in filter(None, path.split("/")):
        acc = f"{acc}/{part}".lstrip("/")
        crumbs += " / " + link(f"/files?root={root}&path={quote(acc)}", part)
    if info.type == fs.FileType.Directory:
        entries = sorted(fsys.get_file_info(fs.FileSelector(full)), key=lambda e: (e.type != fs.FileType.Directory,
                                                                                    e.base_name))
        rows = [(link(f"/files?root={root}&path={quote(e.path.removeprefix(base + '/'))}",
                      e.base_name + ("/" if e.type == fs.FileType.Directory else "")),
                 "" if e.type == fs.FileType.Directory else f"{e.size / 1e3:,.1f} KB",
                 e.mtime.strftime("%Y-%m-%d %H:%M:%S") if e.mtime else "") for e in entries[:500]]
        return page(f"files: {full}", f"<p>{crumbs}</p>" + grid_raw(["name", "size", "modified"], rows))
    if info.type != fs.FileType.File:
        return page("Not found", f"<p>{crumbs}</p><p>No such file.</p>", 404)
    body = f"<p>{crumbs}</p><p>{info.size:,} bytes</p>"
    if full.endswith(".parquet"):
        with fsys.open_input_file(full) as f:
            pf = pq.ParquetFile(f)
            batch = next(pf.iter_batches(batch_size=limit), None)
            body += f"<p>{pf.metadata.num_rows:,} rows · {pf.metadata.num_row_groups} row groups</p>"
            if batch is not None:
                body += grid(batch.schema.names, [tuple(r.values()) for r in batch.to_pylist()])
    elif full.endswith((".json", ".csv", ".jsonl", ".txt", ".md")):
        with fsys.open_input_stream(full) as f:
            text = f.read(64_000).decode("utf-8", "replace")
        body += f"<h2>First 64 KB</h2><pre>{html.escape(text)}</pre>"
    else:
        body += "<p class=muted>Binary file (e.g. Avro manifest): no preview.</p>"
    return page(f"file: {full.rsplit('/', 1)[-1]}", body)


def page(title: str, body: str, status: int = 200) -> tuple[int, str]:
    return status, (f"<!doctype html><meta charset=utf-8><title>{esc(title)}</title><style>{CSS}</style>"
                    f"<p>{link('/', '← overview')}</p><h1>{esc(title)}</h1>{body}")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        limit = min(int(q.get("limit", LIMIT)), 500)
        try:
            if url.path == "/":
                result = overview()
            elif url.path == "/pg":
                result = pg_table(q.get("schema", ""), q.get("table", ""), limit)
            elif url.path == "/iceberg":
                result = iceberg_table(q.get("table", ""), limit)
            elif url.path == "/files":
                result = files(q.get("root", "s3"), q.get("path", "").strip("/"), limit)
            else:
                result = page("Not found", "", 404)
            if isinstance(result, str):
                result = (200, result)
        except Exception as exc:  # noqa: BLE001 - show the error instead of a dropped connection
            result = page("Error", f"<pre>{esc(type(exc).__name__)}: {html.escape(str(exc)[:2000])}</pre>", 500)
        status, text = result
        data = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    port = int(os.getenv("BROWSER_PORT", "8501"))
    print(f"data browser on http://localhost:{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
