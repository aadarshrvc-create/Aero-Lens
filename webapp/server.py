#!/usr/bin/env python3
"""
Local web UI for asloglatency.

Upload an Aerospike server log, a rotated/compressed log, or a whole
collectinfo / support bundle (tar, tar.gz, zip). The server finds every file
that contains "histogram dump:" lines, discovers which histograms each file
has, then runs asloglatency (log_latency.py) per file+histogram and returns
compact time series, per-operation aggregates and a cumulative summary.

Stdlib only. Start with:

    python3 server.py            # http://127.0.0.1:8787
    python3 server.py --port 9000 --tool /path/to/log_latency.py
"""

import argparse
import bz2
import calendar
import collections
import datetime
import gzip
import json
import lzma
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.parse
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import errors as error_analysis
import fetch as log_fetch
import fetch_ansible as log_fetch_ansible
import fetch_jobs as log_fetch_jobs
from analysis_jobs import AnalysisJobs, JobLimitError
import ingest as log_ingest

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(HERE, "static")
DEFAULT_TOOL = os.path.join(os.path.dirname(HERE), "asloglatency", "log_latency.py")
UI_VERSION = "2026.11.23.3"

# Saved reports live outside the session tree so they survive a restart and a session
# reap. Each report is two files: a small <id>.meta.json for the listing, and the full
# <id>.json payload, so listing stays fast when there are many of them.
REPORTS_DIR = os.path.join(os.path.expanduser("~"), "aerospike-log-reports")
REPORT_VERSION = 1
INGEST_DIR = os.path.join(os.path.expanduser("~"), "aerospike-log-store")
INGEST_TOKEN = ""
FETCH_CLUSTERS_PATH = os.path.join(HERE, "fetch_clusters.json")
FETCH_TOKEN = ""
MAX_INGEST_BODY_BYTES = 16 * 1024 ** 2

MAX_UPLOAD_BYTES = 8 * 1024 ** 3
MAX_EXTRACTED_BYTES = 12 * 1024 ** 3
MAX_ARCHIVE_MEMBERS = 50000
MAX_SCAN_FILES = 2000
EXTRACT_DEPTH = 3
TOOL_TIMEOUT_SECONDS = 900
RAW_PREVIEW_LINES = 80

LOG_DT_FMT = "%b %d %Y %H:%M:%S"
TS_RE = re.compile(r"^([A-Z][a-z]{2} \d{2} \d{4} \d{2}:\d{2}:\d{2})")
HIST_RE = re.compile(r"histogram dump: (\S+) \((\d+) total\)(?:\s+(\w+))?")

NODE_ID_RE = re.compile(r"NODE-ID\s+([0-9a-fA-F]{8,20})", re.IGNORECASE)
CLUSTER_NAME_RE = re.compile(r"CLUSTER-NAME\s+([\w.\-]+)", re.IGNORECASE)
CLUSTER_SIZE_RE = re.compile(r"CLUSTER-SIZE\s+(\d+)", re.IGNORECASE)
BUILD_RE = re.compile(r"(?:Aerospike|build)\s+(?:Enterprise Edition\s+build\s+)?([EC]?-?\d+\.\d+\.\d+(?:\.\d+)?)")
IPV4_RE = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3})\b")
ENDPOINT_HINT_RE = re.compile(r"access-address|service-address|endpoint|heartbeat|fabric|hb\.c|socket\.c", re.IGNORECASE)
# asadm "Network Information" rows: 10.12.76.128:3000|*BB9CC1772BD4560|10.12.76.128:3000|E-8.1.0.2|...
NODE_TABLE_RE = re.compile(
    r"^\s*((?:\d{1,3}\.){3}\d{1,3}):(\d+)\s*\|\s*\*?\s*([0-9A-Fa-f]{8,20})\s*\|(.*)$")

HEADER_RE = re.compile(r"^slice-to \(sec\)(.*)$")
UNIT_RE = re.compile(r"%>\s*\((\w+)\)")
DATE_LINE_RE = re.compile(r"^[A-Z][a-z]{2} \d{2} \d{4} \d{2}:\d{2}:\d{2}$")
ROW_RE = re.compile(r"^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+((?:-?[\d.]+\s*)+)$")
AGG_RE = re.compile(r"^\s*(avg|max)\s+((?:-?[\d.]+\s*)+)$")
# asloglatency prints fixed-width columns, so a wide value (a hugely negative
# ops/sec after a counter reset) swallows the separating space: "0.00-1121517886.3".
# Pull numbers out by pattern rather than splitting on whitespace.
NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")

COUNT_HIST_RE = re.compile(r"(rec-count|writes-per-mrt)$")
SIZE_HIST_RE = re.compile(r"(read-size|write-size)$")

# histogram -> operation label used by the overview charts
OP_PATTERNS = [
    (re.compile(r"^\{(?P<ns>[^}]+)\}-read$"), "read"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-write$"), "write"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-batch-sub-read$"), "batch read"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-batch-sub-write$"), "batch write"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-batch-sub-udf$"), "batch udf"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-read-touch$"), "read touch"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-udf$"), "udf"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-(?:pi|si)-query$"), "query"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-re-repl$"), "re-replicate"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-repl-write$"), "replica write"),
    (re.compile(r"^\{(?P<ns>[^}]+)\}-proxy$"), "proxy"),
    (re.compile(r"^batch-index$"), "batch index"),
    (re.compile(r"^info$"), "info"),
]
OP_ORDER = ["read", "write", "batch read", "batch write", "batch index", "batch udf",
            "udf", "query", "read touch", "replica write", "re-replicate", "proxy", "info", "other"]

SESSIONS = {}
SESSION_LOCK = threading.Lock()
ANALYSIS_JOBS = AnalysisJobs()

# Static files are re-read per request but Python code is not, so an edited server.py
# silently keeps serving the old logic until the process is restarted. Remember what
# was loaded so /api/health can say when the running code is out of date.
SOURCE_FILES = [os.path.join(HERE, name) for name in ("server.py", "errors.py", "analysis_jobs.py")]
LOADED_MTIMES = {path: os.path.getmtime(path) for path in SOURCE_FILES if os.path.exists(path)}
STARTED_AT = time.time()


def stale_sources():
    changed = []
    for path, mtime in LOADED_MTIMES.items():
        try:
            if os.path.getmtime(path) > mtime:
                changed.append(os.path.basename(path))
        except OSError:
            pass
    return changed


# --------------------------------------------------------------------------
# uploads: archives, compressed members, plain logs
# --------------------------------------------------------------------------

def safe_name(name):
    name = os.path.basename(name or "upload.bin").strip()
    name = re.sub(r"[^\w.\-+ ]", "_", name)
    return name or "upload.bin"


def is_probably_text(path, probe=8192):
    try:
        with open(path, "rb") as fh:
            chunk = fh.read(probe)
    except OSError:
        return False
    return bool(chunk) and b"\x00" not in chunk


def decompress_single(path):
    openers = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open, ".lzma": lzma.open}
    ext = os.path.splitext(path)[1].lower()
    opener = openers.get(ext)
    if not opener:
        return None
    target = path[: -len(ext)]
    if os.path.exists(target):
        target += ".decompressed"
    try:
        with opener(path, "rb") as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
    except Exception:
        if os.path.exists(target):
            os.remove(target)
        return None
    return target


def extract_archive(path, dest):
    os.makedirs(dest, exist_ok=True)
    try:
        if tarfile.is_tarfile(path):
            with tarfile.open(path, "r:*") as tar:
                members, total = [], 0
                for member in tar:
                    if len(members) >= MAX_ARCHIVE_MEMBERS:
                        break
                    if not (member.isfile() or member.isdir()):
                        continue
                    total += max(member.size, 0)
                    if total > MAX_EXTRACTED_BYTES:
                        break
                    members.append(member)
                try:
                    tar.extractall(dest, members=members, filter="data")
                except TypeError:  # python < 3.12
                    safe = [m for m in members
                            if not os.path.isabs(m.name) and ".." not in m.name.split("/")]
                    tar.extractall(dest, members=safe)
            return True
    except (tarfile.TarError, OSError):
        pass
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as zf:
                total = 0
                for info in zf.infolist()[:MAX_ARCHIVE_MEMBERS]:
                    total += info.file_size
                    if total > MAX_EXTRACTED_BYTES:
                        break
                    if info.is_dir() or os.path.isabs(info.filename) or ".." in info.filename.split("/"):
                        continue
                    zf.extract(info, dest)
            return True
    except (zipfile.BadZipFile, OSError):
        pass
    return False


def collect_candidates(root, upload_path):
    pending = [(upload_path, 0)]
    plain = []
    while pending:
        path, depth = pending.pop()
        if not os.path.isfile(path):
            continue
        if depth <= EXTRACT_DEPTH:
            dest = os.path.join(root, "extracted", "%s-%d" % (os.path.basename(path), depth))
            if extract_archive(path, dest):
                for dirpath, _dirnames, filenames in os.walk(dest):
                    for name in filenames:
                        pending.append((os.path.join(dirpath, name), depth + 1))
                continue
            decompressed = decompress_single(path)
            if decompressed:
                pending.append((decompressed, depth + 1))
                continue
        base = os.path.basename(path)
        if base.startswith("aerospike.log") or is_probably_text(path):
            plain.append(path)
    return plain[:MAX_SCAN_FILES]


# --------------------------------------------------------------------------
# scanning: histograms, node identity, cluster node table
# --------------------------------------------------------------------------

def scan_file(path):
    """One pass over a text file: histogram inventory plus identity hints.

    Server logs run to millions of lines, so the hot loop uses substring tests and
    only falls back to regexes for lines that can plausibly match.
    """
    hists = {}
    first_ts = last_ts = None
    node_id = cluster_name = build = None
    cluster_size = None
    dumps = 0
    ip_hits = collections.Counter()
    node_table = {}
    identity_done = False

    with open(path, "r", errors="replace") as fh:
        for line in fh:
            if "histogram dump:" in line:
                hist_match = HIST_RE.search(line)
                if hist_match:
                    ts_match = TS_RE.match(line)
                    if ts_match:
                        last_ts = ts_match.group(1)
                        if first_ts is None:
                            first_ts = last_ts
                    dumps += 1
                    name, total, unit = hist_match.group(1), int(hist_match.group(2)), hist_match.group(3)
                    entry = hists.setdefault(name, {
                        "name": name, "dumps": 0, "unit": unit or "",
                        "first_ts": last_ts, "last_ts": last_ts, "last_total": total,
                    })
                    entry["dumps"] += 1
                    entry["last_ts"] = last_ts
                    entry["last_total"] = total
                continue

            if not identity_done and ("NODE-ID" in line or "node-id" in line):
                if node_id is None:
                    found = NODE_ID_RE.search(line)
                    if found:
                        node_id = found.group(1).lower()
                if cluster_name is None:
                    found = CLUSTER_NAME_RE.search(line)
                    if found:
                        cluster_name = found.group(1)
                if cluster_size is None:
                    found = CLUSTER_SIZE_RE.search(line)
                    if found:
                        cluster_size = int(found.group(1))
                identity_done = bool(node_id and cluster_name and cluster_size)
            elif build is None and ("build" in line or "Build" in line):
                found = BUILD_RE.search(line)
                if found:
                    build = found.group(1)

            if line[:1].isdigit():
                table_match = NODE_TABLE_RE.match(line)
                # A node-id always contains hex letters; this keeps statistics tables,
                # whose second column is a plain decimal, out of the node inventory.
                if table_match and re.search(r"[A-Fa-f]", table_match.group(3)):
                    ip, port, table_node_id, rest = table_match.groups()
                    columns = [c.strip() for c in rest.split("|")]
                    record = {"ip": ip, "port": int(port), "node_id": table_node_id.lower()}
                    for column in columns:
                        if re.match(r"^[EC]-\d+\.\d+\.\d+", column):
                            record["build"] = column
                        elif re.match(r"^\d+:\d{2}:\d{2}$", column):
                            record["uptime"] = column
                    node_table[record["node_id"]] = record
                    ip_hits[ip] += 1
                    continue

            if len(ip_hits) < 8 and ("address" in line or "hb.c" in line
                                     or "socket.c" in line or "fabric" in line):
                if ENDPOINT_HINT_RE.search(line):
                    for ip in IPV4_RE.findall(line):
                        if not ip.startswith(("0.", "127.", "255.")):
                            ip_hits[ip] += 1

    if first_ts is None:  # no histogram lines: fall back to the first timestamped line
        with open(path, "r", errors="replace") as fh:
            for line in fh:
                ts_match = TS_RE.match(line)
                if ts_match:
                    if first_ts is None:
                        first_ts = ts_match.group(1)
                    last_ts = ts_match.group(1)

    return {
        "dumps": dumps,
        "histograms": sorted(hists.values(), key=lambda h: (-h["dumps"], h["name"])),
        "first_ts": first_ts,
        "last_ts": last_ts,
        "node_id": node_id,
        "cluster_name": cluster_name,
        "cluster_size": cluster_size,
        "build": build,
        "ip_hits": ip_hits,
        "node_table": node_table,
    }


ARCHIVE_EXT_RE = re.compile(r"\.(tar\.gz|tar\.bz2|tar\.xz|tgz|tbz2|txz|tar|zip|gz|bz2|xz)$", re.IGNORECASE)
# Support bundles name per-node archives with the IP flattened: ..._10_12_76_226_...
IPV4_UNDERSCORE_RE = re.compile(r"(?<!\d)(\d{1,3})_(\d{1,3})_(\d{1,3})_(\d{1,3})(?!\d)")


def pretty_label(root, path):
    """Relative path with the extraction scaffolding removed but archive names kept.

    Bundles often ship one archive per node, so the inner archive name is the only
    thing that identifies which node a log came from.
    """
    segments = os.path.relpath(path, root).split(os.sep)
    out = []
    for index, segment in enumerate(segments):
        if segment in ("uploads", "extracted"):
            continue
        if index < len(segments) - 1:
            segment = re.sub(r"-\d+$", "", segment)
            segment = ARCHIVE_EXT_RE.sub("", segment)
        out.append(segment)
    return "/".join(out)


def ip_from_path(label):
    for part in reversed(label.split("/")):
        found = IPV4_RE.search(part)
        if found:
            return found.group(1)
        found = IPV4_UNDERSCORE_RE.search(part)
        if found:
            octets = [int(o) for o in found.groups()]
            if all(o <= 255 for o in octets):
                return ".".join(str(o) for o in octets)
    return None


def apply_identity(session):
    """Resolve node ip/build for every uploaded log, joining on the cluster node table."""
    table = session["node_table"]
    for entry in session["files"].values():
        scan = entry["scan"]
        node_id = scan["node_id"]
        file_table = scan.get("node_table") or {}
        record = None
        if node_id:
            record = file_table.get(node_id) or table.get(node_id)
        ip = source = None
        if record:
            ip, source = record["ip"], "collectinfo node table"
        if not ip:
            found = ip_from_path(entry["label"])
            if found:
                ip, source = found, "file path"
        if not ip and scan["ip_hits"]:
            ip, source = scan["ip_hits"].most_common(1)[0][0], "log line"
        basename = entry["label"].split("/")[-1]
        entry["node"] = {
            "label": entry["label"],
            "name": "%s %s" % (ip, basename) if ip else entry["label"],
            "key": ip or node_id or entry["label"],
            "ip": ip,
            "ip_source": source,
            "port": (record or {}).get("port"),
            "node_id": node_id,
            "build": (record or {}).get("build") or scan["build"],
            "uptime": (record or {}).get("uptime"),
            "cluster_name": scan["cluster_name"],
            "cluster_size": scan["cluster_size"],
        }


def session_cluster(session):
    names = {f["scan"]["cluster_name"] for f in session["files"].values() if f["scan"]["cluster_name"]}
    sizes = {f["scan"]["cluster_size"] for f in session["files"].values() if f["scan"]["cluster_size"]}
    return {
        "name": sorted(names)[0] if names else None,
        "size": max(sizes) if sizes else None,
        "known_nodes": len(session["node_table"]),
        "logs": len(session["files"]),
    }


def public_file(entry):
    scan = entry["scan"]
    return {
        "id": entry["id"],
        "label": entry["label"],
        "size": entry["size"],
        "dumps": scan["dumps"],
        "histograms": scan["histograms"],
        "errors_only": not scan["histograms"] and bool(scan["first_ts"]),
        "first_iso": iso(scan["first_ts"]),
        "last_iso": iso(scan["last_ts"]),
        "node": entry["node"],
    }


def iso(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.strptime(ts, LOG_DT_FMT).isoformat(sep=" ")
    except ValueError:
        return None


def report_slug(name):
    """A filename that is readable in a file manager and safe on every OS."""
    slug = re.sub(r"[^\w.-]+", "-", (name or "report").strip()).strip("-.")
    return (slug or "report")[:60]


def report_paths(report_id):
    base = os.path.join(REPORTS_DIR, report_id)
    return base + ".json", base + ".meta.json"


def write_report(name, cluster, payload):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    report_id = "%s_%s" % (report_slug(name or cluster), stamp)
    full_path, meta_path = report_paths(report_id)

    latency, errors = payload.get("latency"), payload.get("errors")
    window = (errors or latency or {}).get("window") or {}
    meta = {
        "id": report_id,
        "name": name or cluster or report_id,
        "cluster": cluster,
        "saved_at": iso_epoch(time.time()),
        "has_latency": bool(latency),
        "has_errors": bool(errors),
        "window": {k: window.get(k) for k in ("mode", "start", "end", "from", "to")},
        "nodes": payload.get("nodes") or [],
        "logs": payload.get("logs") or [],
    }
    document = dict(meta)
    document.update({"kind": "aerospike-log-report", "version": REPORT_VERSION,
                     "latency": latency, "errors": errors})

    # Write then rename, so a crash mid-write cannot leave a half-parsed report behind.
    for path, body in ((full_path, document), (meta_path, meta)):
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(body, fh)
        os.replace(tmp, path)

    meta["bytes"] = os.path.getsize(full_path)
    meta["path"] = full_path
    return meta


def list_reports():
    if not os.path.isdir(REPORTS_DIR):
        return []
    out = []
    for name in os.listdir(REPORTS_DIR):
        if not name.endswith(".meta.json"):
            continue
        report_id = name[: -len(".meta.json")]
        full_path, meta_path = report_paths(report_id)
        try:
            with open(meta_path) as fh:
                meta = json.load(fh)
            meta["bytes"] = os.path.getsize(full_path)
            meta["path"] = full_path
        except (OSError, ValueError):
            continue
        out.append(meta)
    out.sort(key=lambda m: m.get("saved_at") or "", reverse=True)
    return out


def iso_epoch(ts):
    """Epoch -> "YYYY-MM-DD HH:MM:SS" GMT, the one stamp format the UI localizes."""
    return datetime.datetime.fromtimestamp(
        ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def epoch(dt):
    return calendar.timegm(dt.timetuple())


def iso_now():
    return datetime.datetime.fromtimestamp(STARTED_AT).strftime("%H:%M:%S")


# --------------------------------------------------------------------------
# classification
# --------------------------------------------------------------------------

def classify_kind(hist, log_unit):
    name = hist.split("}-")[-1]
    if COUNT_HIST_RE.search(name):
        return "count"
    if SIZE_HIST_RE.search(name):
        return "size"
    if log_unit and log_unit.lower() not in ("msec", "usec", "ms", "us"):
        return "count" if log_unit.lower().startswith("count") else "size"
    return "latency"


def classify_op(hist):
    for pattern, label in OP_PATTERNS:
        match = pattern.match(hist)
        if match:
            return label, (match.groupdict().get("ns") if match.groupdict() else None)
    namespace = None
    ns_match = re.match(r"^\{([^}]+)\}-", hist)
    if ns_match:
        namespace = ns_match.group(1)
    return "other", namespace


def pick_primary(thresholds, target):
    if not thresholds:
        return 0
    return min(range(len(thresholds)), key=lambda i: (abs(thresholds[i] - target), i))


def threshold_in_unit(alert_ms, unit):
    """The alert threshold is entered in ms; buckets are µs when the namespace
    runs with microsecond-histograms enabled."""
    return alert_ms * 1000.0 if (unit or "").lower() in ("us", "usec") else alert_ms


# --------------------------------------------------------------------------
# running asloglatency and parsing its output
# --------------------------------------------------------------------------

def build_command(tool, log_path, hist, opts):
    cmd = [sys.executable, tool, "-l", log_path, "-h", hist,
           "-t", str(opts["slice_seconds"]),
           "-n", str(opts["num_buckets"]), "-e", str(opts["every_nth"])]
    start, end = opts.get("from_dt"), opts.get("to_dt")
    cmd += ["-f", start.strftime(LOG_DT_FMT) if start else "head"]
    if start and end and end > start:
        seconds = int((end - start).total_seconds())
        cmd += ["-d", "%d:%02d:%02d" % (seconds // 3600, (seconds % 3600) // 60, seconds % 60)]
    return cmd


def parse_tool_output(text):
    unit, thresholds, extra_columns = "ms", [], []
    base_date, prev_time = None, None
    times, ops, overs_columns, slice_seconds = [], [], None, []
    aggregates, messages, resets = {}, [], []

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        unit_match = UNIT_RE.search(stripped)
        if unit_match:
            unit = unit_match.group(1)
            continue
        if DATE_LINE_RE.match(stripped):
            try:
                base_date = datetime.datetime.strptime(stripped, LOG_DT_FMT)
            except ValueError:
                base_date = None
            continue
        header_match = HEADER_RE.match(stripped)
        if header_match:
            columns = header_match.group(1).split()
            thresholds = [int(c) for c in columns if c.isdigit()]
            extra_columns = [c for c in columns if not c.isdigit() and c != "ops/sec"]
            overs_columns = [[] for _ in thresholds]
            continue
        if set(stripped) <= set("- "):
            continue

        row_match = ROW_RE.match(line)
        if row_match and thresholds:
            values = [float(v) for v in NUM_RE.findall(row_match.group(3))]
            clock = row_match.group(1)
            if base_date:
                hh, mm, ss = (int(p) for p in clock.split(":"))
                candidate = base_date.replace(hour=hh, minute=mm, second=ss)
                if prev_time and candidate < prev_time:
                    candidate += datetime.timedelta(days=1)
                    base_date = candidate.replace(hour=0, minute=0, second=0)
                prev_time = candidate
                times.append(epoch(candidate))
            else:
                times.append(None)
            for index in range(len(thresholds)):
                overs_columns[index].append(round(values[index], 2) if index < len(values) else 0.0)
            rest = values[len(thresholds):]
            rate = rest[0] if rest else 0.0
            # A negative rate means the histogram's running total went backwards, which
            # only happens when asd restarted. The slice spans the outage, so its rate
            # and percentages describe nothing real.
            if rate < 0:
                resets.append(len(times) - 1)
                rate = 0.0
            ops.append(round(rate, 1))
            slice_seconds.append(int(row_match.group(2)))
            continue

        agg_match = AGG_RE.match(line)
        if agg_match and thresholds:
            values = [float(v) for v in NUM_RE.findall(agg_match.group(2))]
            aggregates[agg_match.group(1)] = {
                "overs": values[: len(thresholds)],
                "ops": values[len(thresholds)] if len(values) > len(thresholds) else 0.0,
            }
            continue
        messages.append(stripped)

    status = "ok" if times else "no-data"
    for message in messages:
        if "can't find histogram" in message:
            status = "not-present"
    return {
        "status": status, "unit": unit, "thresholds": thresholds,
        "extra_columns": extra_columns, "times": times, "ops": ops,
        "overs": overs_columns or [], "slice_seconds": slice_seconds,
        "aggregates": aggregates, "messages": messages, "resets": resets,
    }


def summarize(parsed, hist, node, opts, log_unit):
    thresholds = parsed["thresholds"]
    times = parsed["times"]
    kind = classify_kind(hist, log_unit)
    op, namespace = classify_op(hist)
    unit = parsed["unit"] if kind == "latency" else (log_unit or kind)
    summary = {
        "histogram": hist, "node": node["name"], "ip": node["ip"],
        "status": parsed["status"], "kind": kind, "op": op, "namespace": namespace,
        "unit": unit, "thresholds": thresholds, "slice_count": len(times),
        "window_start": times[0] if times else None,
        "window_end": times[-1] if times else None,
        "avg": parsed["aggregates"].get("avg"), "max": parsed["aggregates"].get("max"),
        "alert_pct": opts["alert_pct"], "min_ops_rate": opts["min_ops_rate"],
        "breach_count": 0, "severity": "info", "worst": None, "peak_ops": None,
        "total_ops": 0, "idle": False,
        "warnings": [m for m in parsed["messages"] if m.upper().startswith("WARNING")],
        "reset_count": len(parsed["resets"]),
        "reset_times": [times[i] for i in parsed["resets"] if i < len(times)],
    }
    if not times or not thresholds:
        return summary

    primary = pick_primary(thresholds, threshold_in_unit(opts["alert_threshold"], unit))
    overs = parsed["overs"][primary] if primary < len(parsed["overs"]) else []
    ops = parsed["ops"]
    slice_seconds = parsed["slice_seconds"]

    summary["primary_index"] = primary
    summary["primary_threshold"] = thresholds[primary]
    summary["total_ops"] = sum(o * s for o, s in zip(ops, slice_seconds))
    summary["idle"] = not any(ops)

    peak_index = max(range(len(ops)), key=lambda i: ops[i])
    summary["peak_ops"] = {"ops": ops[peak_index], "t": times[peak_index]}

    # Rate, not a per-slice count: a count gate would mean 10 ops/sec at 10s slices
    # and 0.3 ops/sec at 5m slices, so the same number would hide or expose noise
    # depending only on the slice width.
    reset_slices = set(parsed["resets"])
    busy = [i for i in range(len(overs))
            if ops[i] >= opts["min_ops_rate"] and i not in reset_slices]
    summary["rated_slice_count"] = len(busy)
    pool = busy or [i for i in range(len(overs)) if i not in reset_slices] or list(range(len(overs)))
    worst_index = max(pool, key=lambda i: overs[i])
    summary["worst"] = {"threshold": thresholds[primary], "pct": overs[worst_index],
                        "t": times[worst_index], "ops": ops[worst_index]}

    if kind != "latency":
        summary["severity"] = "idle" if summary["idle"] else "info"
        return summary

    breaches = [i for i in busy if overs[i] > opts["alert_pct"]]
    summary["breach_count"] = len(breaches)
    summary["breach_samples"] = [{"t": times[i], "pct": overs[i], "ops": ops[i]} for i in breaches[:50]]
    if summary["idle"]:
        summary["severity"] = "idle"
    elif not busy:
        summary["severity"] = "info"
    elif len(breaches) > max(1, len(busy) // 20):
        summary["severity"] = "critical"
    elif breaches:
        summary["severity"] = "warning"
    else:
        summary["severity"] = "ok"
    return summary


def failed_run(entry, hist, exc):
    """Placeholder so one broken histogram shows up as a row instead of a 500."""
    op, namespace = classify_op(hist)
    return {
        "id": uuid.uuid4().hex[:10], "histogram": hist, "file": entry["label"],
        "node": entry["node"], "kind": "latency", "op": op, "namespace": namespace,
        "unit": "ms", "thresholds": [], "extra_columns": [],
        "series": {"t": [], "ops": [], "overs": [], "slice_seconds": []},
        "aggregates": {}, "messages": ["analysis failed: %s" % exc],
        "summary": {"histogram": hist, "node": entry["node"].get("name"),
                    "ip": entry["node"].get("ip"), "status": "failed", "kind": "latency",
                    "op": op, "namespace": namespace, "unit": "ms", "thresholds": [],
                    "slice_count": 0, "severity": "info", "breach_count": 0,
                    "total_ops": 0, "idle": False, "worst": None, "peak_ops": None,
                    "warnings": [], "reset_count": 0, "reset_times": [],
                    "reason": "asloglatency output could not be read: %s" % exc},
        "coverage": {"start": iso(entry["scan"]["first_ts"]), "end": iso(entry["scan"]["last_ts"])},
        "command": "", "exit_code": -1, "raw_preview": "", "raw_line_count": 0, "raw": "",
    }


def empty_reason(scan, opts):
    """asloglatency only echoes the histogram name when it finds no rows, so say why."""
    start, end = iso(scan["first_ts"]), iso(scan["last_ts"])
    coverage = "log covers %s → %s GMT" % (start, end) if start else "log has no timestamps"
    window_start = opts["from_dt"].isoformat(sep=" ") if opts["from_dt"] else None
    window_end = opts["to_dt"].isoformat(sep=" ") if opts["to_dt"] else None
    if (window_start and end and end < window_start) or (window_end and start and start > window_end):
        return "outside the selected window — " + coverage
    return "no traffic on this histogram in the window — " + coverage


def run_one(tool, entry, hist, log_unit, opts):
    cmd = build_command(tool, entry["path"], hist, opts)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=TOOL_TIMEOUT_SECONDS)
        raw = (proc.stdout or "") + (proc.stderr or "")
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        raw, exit_code = "asloglatency timed out after %ds" % TOOL_TIMEOUT_SECONDS, -1

    try:
        parsed = parse_tool_output(raw)
    except Exception as exc:
        # One unreadable histogram must not sink the whole analysis.
        parsed = {"status": "parse-error", "unit": log_unit or "ms", "thresholds": [],
                  "extra_columns": [], "times": [], "ops": [], "overs": [],
                  "slice_seconds": [], "aggregates": {}, "resets": [],
                  "messages": ["could not read asloglatency output: %s" % exc]}
    summary = summarize(parsed, hist, entry["node"], opts, log_unit)
    if summary["status"] != "ok":
        summary["reason"] = empty_reason(entry["scan"], opts)
    raw_lines = raw.splitlines()
    return {
        "id": uuid.uuid4().hex[:10],
        "histogram": hist,
        "file": entry["label"],
        "node": entry["node"],
        "kind": summary["kind"],
        "op": summary["op"],
        "namespace": summary["namespace"],
        "unit": summary["unit"],
        "thresholds": parsed["thresholds"],
        "extra_columns": parsed["extra_columns"],
        "series": {"t": parsed["times"], "ops": parsed["ops"], "overs": parsed["overs"],
                   "slice_seconds": parsed["slice_seconds"][:1]},
        "aggregates": parsed["aggregates"],
        "messages": parsed["messages"],
        "summary": summary,
        "coverage": {"start": iso(entry["scan"]["first_ts"]), "end": iso(entry["scan"]["last_ts"])},
        "command": " ".join(cmd),
        "exit_code": exit_code,
        "raw_preview": "\n".join(raw_lines[:RAW_PREVIEW_LINES]),
        "raw_line_count": len(raw_lines),
        "raw": raw,
    }


# --------------------------------------------------------------------------
# cumulative rollups
# --------------------------------------------------------------------------

SEVERITY_RANK = {"ok": 0, "info": 0, "idle": 0, "warning": 1, "critical": 2}
# A batch-index measurement is the parent of the batch-sub operations it fans out to,
# so counting both would double count the work a node did.
PARENT_OPS = {"batch index"}


def bin_sum(results, key_fn, start, step, buckets):
    """Sum ops/sec onto a shared time grid, grouped by key_fn."""
    out = {}
    for result in results:
        series = result["series"]
        target = out.setdefault(key_fn(result), [0.0] * buckets)
        for index, timestamp in enumerate(series["t"]):
            if timestamp is None:
                continue
            slot = int((timestamp - start) // step)
            if 0 <= slot < buckets:
                target[slot] += series["ops"][index]
    return out


def explain_empty(results, opts):
    """Say why a histogram produced nothing: wrong window, or simply no traffic."""
    window_start = opts["from_dt"].isoformat(sep=" ") if opts["from_dt"] else None
    window_end = opts["to_dt"].isoformat(sep=" ") if opts["to_dt"] else None
    outside, absent = {}, []
    for result in results:
        if result["summary"]["status"] == "ok":
            continue
        coverage = result.get("coverage") or {}
        start, end = coverage.get("start"), coverage.get("end")
        off_window = bool(window_start and end and end < window_start) or \
                     bool(window_end and start and start > window_end)
        if off_window:
            bucket = outside.setdefault(result["file"], {
                "log": result["file"], "node": result["node"]["ip"] or result["node"]["name"],
                "start": start, "end": end, "runs": 0})
            bucket["runs"] += 1
        else:
            absent.append({"histogram": result["histogram"],
                           "node": result["node"]["ip"] or result["node"]["name"],
                           "message": (result["messages"] or ["no rows"])[0]})
    return list(outside.values()), absent


def build_rollups(results, opts):
    """One pass that produces the overview charts and the cumulative tables."""
    latency = [r for r in results if r["kind"] == "latency" and r["series"]["t"]]
    if not latency:
        outside, absent = explain_empty(results, opts)
        headline = []
        if outside:
            headline.append("none of the selected logs cover the chosen window; %s covers %s → %s GMT" % (
                outside[0]["node"], outside[0]["start"], outside[0]["end"]))
        return None, {"by_op": [], "by_node": [], "headline": headline, "outside_window": outside,
                      "not_present": absent[:10], "not_present_count": len(results),
                      "distributions": []}

    step = opts["slice_seconds"]
    start = min(r["series"]["t"][0] for r in latency)
    end = max(r["series"]["t"][-1] for r in latency)
    buckets = max(1, int((end - start) // step) + 1)
    if buckets > 20000:  # keep the payload sane for very long windows
        step = int((end - start) / 20000) + 1
        buckets = int((end - start) // step) + 1
    grid = [start + i * step for i in range(buckets)]
    window_seconds = max(step, end - start + step)

    ops_by_op = bin_sum(latency, lambda r: r["op"], start, step, buckets)
    ops_by_node = bin_sum(latency, lambda r: r["node"]["key"], start, step, buckets)
    counted = [r for r in latency if r["op"] not in PARENT_OPS]
    total_line = bin_sum(counted, lambda r: "total", start, step, buckets).get("total", [0.0] * buckets)
    node_totals = bin_sum(counted, lambda r: r["node"]["key"], start, step, buckets)

    # ops-weighted percentage over the alert threshold, so a busy node is not
    # averaged away by an idle one
    slow_by_op = {op: [0.0] * buckets for op in ops_by_op}
    weight_by_op = {op: [0.0] * buckets for op in ops_by_op}
    for result in latency:
        primary = result["summary"].get("primary_index", 0)
        overs = result["series"]["overs"][primary] if primary < len(result["series"]["overs"]) else []
        slow, weight = slow_by_op[result["op"]], weight_by_op[result["op"]]
        for index, timestamp in enumerate(result["series"]["t"]):
            if timestamp is None or index >= len(overs):
                continue
            slot = int((timestamp - start) // step)
            if not 0 <= slot < buckets:
                continue
            ops_value = result["series"]["ops"][index]
            slow[slot] += overs[index] * ops_value
            weight[slot] += ops_value
    pct_by_op = {op: [slow / weight_by_op[op][i] if weight_by_op[op][i] else 0.0
                      for i, slow in enumerate(series)]
                 for op, series in slow_by_op.items()}

    order = [op for op in OP_ORDER if op in ops_by_op] + [op for op in ops_by_op if op not in OP_ORDER]
    aggregate = {
        "t": grid, "step": step,
        "ops": {op: [round(v, 1) for v in ops_by_op[op]] for op in order},
        "pct": {op: pct_by_op[op] for op in order},
        "total": [round(v, 1) for v in total_line],
        "order": order,
        "threshold": latency[0]["summary"].get("primary_threshold"),
        "unit": latency[0]["unit"],
        "window_seconds": window_seconds,
        "parent_ops": sorted(PARENT_OPS & set(order)),
    }

    # ---- per operation -------------------------------------------------
    by_op = {}
    for result in latency:
        summary = result["summary"]
        entry = by_op.setdefault(result["op"], {
            "op": result["op"], "nodes": set(), "namespaces": set(), "total_ops": 0.0,
            "worst_pct": 0.0, "worst_at": None, "worst_node": None, "breaches": 0,
            "threshold": summary.get("primary_threshold"), "unit": summary["unit"],
            "severity": "ok",
        })
        entry["nodes"].add(result["node"]["key"])
        if result["namespace"]:
            entry["namespaces"].add(result["namespace"])
        entry["total_ops"] += summary["total_ops"]
        entry["breaches"] += summary["breach_count"]
        worst = summary.get("worst") or {}
        if worst.get("pct", 0) > entry["worst_pct"]:
            entry.update({"worst_pct": worst["pct"], "worst_at": worst.get("t"),
                          "worst_ops": worst.get("ops"),
                          "worst_node": result["node"]["ip"] or result["node"]["name"]})
        if SEVERITY_RANK.get(summary["severity"], 0) > SEVERITY_RANK.get(entry["severity"], 0):
            entry["severity"] = summary["severity"]

    op_rows = []
    for op in order:
        entry = by_op.get(op)
        if not entry:
            continue
        entry["nodes"] = len(entry["nodes"])
        entry["namespaces"] = sorted(entry["namespaces"])
        entry["total_ops"] = round(entry["total_ops"])
        entry["worst_pct"] = round(entry["worst_pct"], 2)
        entry["avg_ops"] = round(entry["total_ops"] / window_seconds, 1)
        entry["peak_ops"] = round(max(ops_by_op[op]), 1)
        op_rows.append(entry)

    # ---- per node (rotated logs of one node are grouped) ---------------
    by_node = {}
    for result in latency:
        summary = result["summary"]
        node = result["node"]
        entry = by_node.setdefault(node["key"], {
            "key": node["key"], "name": node["ip"] or node["name"], "ip": node["ip"],
            "node_id": node["node_id"], "build": node["build"], "files": set(),
            "histograms": 0, "total_ops": 0.0, "worst_pct": 0.0, "worst_op": None,
            "breaches": 0, "severity": "ok",
        })
        entry["files"].add(result["file"])
        entry["histograms"] += 1
        if result["op"] not in PARENT_OPS:
            entry["total_ops"] += summary["total_ops"]
        entry["breaches"] += summary["breach_count"]
        worst = summary.get("worst") or {}
        if worst.get("pct", 0) > entry["worst_pct"]:
            entry.update({"worst_pct": worst["pct"], "worst_op": result["op"]})
        if SEVERITY_RANK.get(summary["severity"], 0) > SEVERITY_RANK.get(entry["severity"], 0):
            entry["severity"] = summary["severity"]

    node_rows = []
    for key, entry in by_node.items():
        entry["files"] = len(entry["files"])
        entry["total_ops"] = round(entry["total_ops"])
        entry["avg_ops"] = round(entry["total_ops"] / window_seconds, 1)
        entry["peak_ops"] = round(max(node_totals.get(key, [0.0])), 1)
        entry["worst_pct"] = round(entry["worst_pct"], 2)
        node_rows.append(entry)
    node_rows.sort(key=lambda n: (-n["worst_pct"], n["name"]))

    # ---- headline ------------------------------------------------------
    headline = []
    if not any(r["breaches"] for r in op_rows):
        used = sorted({"%s %s" % (r["summary"]["primary_threshold"], r["unit"]) for r in latency})
        headline.append("no operation type exceeded %.1f%% over %s in this window" % (
            opts["alert_pct"], " / ".join(used)))
    offenders = sorted(op_rows, key=lambda r: -r["worst_pct"])[:2]
    for entry in offenders:
        if entry["worst_pct"] > 0:
            when = entry.get("worst_at")
            headline.append("%s peaked at %.2f%% of operations slower than %s %s on %s%s%s" % (
                entry["op"], entry["worst_pct"], entry["threshold"], entry["unit"],
                entry["worst_node"] or "unknown node",
                " at %s GMT" % iso_epoch(when) if when else "",
                " while serving %.0f ops/sec" % entry["worst_ops"]
                if entry.get("worst_ops") is not None else ""))
    worst_nodes = [n for n in node_rows if n["breaches"]]
    if len(worst_nodes) == 1 and len(node_rows) > 1:
        headline.append("only %s breached the alert threshold; the other %d node(s) stayed clean" % (
            worst_nodes[0]["name"], len(node_rows) - 1))
    elif len(worst_nodes) > 1:
        headline.append("%d of %d nodes breached the alert threshold: %s" % (
            len(worst_nodes), len(node_rows), ", ".join(n["name"] for n in worst_nodes[:6])))

    restarted = {}
    for result in results:
        for when in result["summary"].get("reset_times") or []:
            node = result["node"].get("ip") or result["node"].get("name")
            restarted.setdefault(node, when)
    if restarted:
        headline.append(
            "%d node(s) restarted during this window, so their counters reset and those "
            "slices are excluded: %s" % (len(restarted), ", ".join(
                "%s around %s GMT" % (node, iso_epoch(when))
                for node, when in sorted(restarted.items())[:4])))

    failed = [r for r in results if r["summary"]["status"] == "failed"]
    if failed:
        headline.append("%d histogram run(s) could not be parsed: %s" % (
            len(failed), failed[0]["messages"][0][:120]))

    warned = [r for r in results if r["summary"].get("warnings")]
    if warned:
        headline.append("asloglatency warned on %d run(s): %s" % (
            len(warned), warned[0]["summary"]["warnings"][0]))

    outside, absent = explain_empty(results, opts)
    if outside:
        runs = sum(o["runs"] for o in outside)
        headline.append("%d log(s) sit outside the selected window and were skipped (%d run(s)): %s" % (
            len(outside), runs, ", ".join("%s covers %s → %s GMT" % (o["node"], o["start"], o["end"])
                                          for o in outside[:2])))

    skipped = [r for r in results if r["summary"]["status"] != "ok"]
    cumulative = {
        "by_op": op_rows,
        "by_node": node_rows,
        "headline": headline,
        "outside_window": outside,
        "not_present": absent[:10],
        "not_present_count": len(skipped),
        "absent_count": len(absent),
        "distributions": [{"histogram": r["histogram"], "node": r["node"]["name"], "unit": r["unit"]}
                          for r in results if r["kind"] != "latency"],
    }
    return aggregate, cumulative


# --------------------------------------------------------------------------
# sessions
# --------------------------------------------------------------------------

SESSION_TTL_SECONDS = 6 * 3600


def reap_sessions(keep):
    """Drop extracted bundles from abandoned sessions; a run can leave GBs behind."""
    cutoff = time.time() - SESSION_TTL_SECONDS
    with SESSION_LOCK:
        stale = [sid for sid, s in SESSIONS.items() if sid != keep and s["created"] < cutoff]
        dead = [SESSIONS.pop(sid) for sid in stale]
    for session in dead:
        shutil.rmtree(session["root"], ignore_errors=True)
    return len(dead)


def get_session(sid, create=False):
    with SESSION_LOCK:
        session = SESSIONS.get(sid)
        if session is None and create:
            root = tempfile.mkdtemp(prefix="asloglatency-web-")
            session = {"id": sid, "root": root, "files": {}, "node_table": {},
                       "raw": {}, "error_cache": {}, "created": time.time()}
            SESSIONS[sid] = session
        return session


def upload_result_payload(session, sid, started, added, skipped):
    apply_identity(session)
    session["error_cache"].clear()
    files = [public_file(f) for f in session["files"].values()]
    hist_count = sum(1 for f in files if f["histograms"])
    err_only = sum(1 for f in files if f.get("errors_only"))
    hint = None
    if not added and skipped:
        empty = sum(1 for s in skipped if "empty file" in s.get("reason", ""))
        if empty:
            hint = ("Found nested aerospike.log-*.gz files but they decompress to 0 bytes "
                    "(%d empty). The bundle MANIFEST may list only rotated names while those "
                    "archives are empty on disk — include the active aerospike.log and "
                    "non-zero .gz files from /var/log/aerospike." % empty)
        else:
            hint = ("This archive looks like collectinfo/support metadata only — no aerospike.log "
                    "with timestamped lines was found. Upload per-node aerospike.log (or a bundle "
                    "that includes them). Latency needs histogram dump lines in those logs.")
    elif not hist_count and err_only:
        hint = ("Loaded %d error-only log(s) (no histogram dumps). Use the Errors tab; "
                "Latency needs logs with 'histogram dump:' lines." % err_only)
    return {
        "session": sid,
        "scan_seconds": round(time.time() - started, 2),
        "added": added,
        "skipped": skipped[:50],
        "upload_hint": hint,
        "latency_logs": hist_count,
        "error_only_logs": err_only,
        "cluster": session_cluster(session),
        "files": files,
    }


def ingest_upload_target(session, target):
    """Scan one on-disk path (log or archive) into the session; same rules as /api/upload."""
    added, skipped = [], []
    for path in collect_candidates(session["root"], target):
        label = pretty_label(session["root"], path)
        try:
            size = os.path.getsize(path)
        except OSError:
            size = 0
        if size == 0:
            skipped.append({"file": label, "reason": "empty file (0 bytes after extract/decompress)"})
            continue
        scan = scan_file(path)
        session["node_table"].update(scan["node_table"])
        if not scan["histograms"]:
            if scan["first_ts"]:
                fid = uuid.uuid4().hex[:12]
                session["files"][fid] = {"id": fid, "path": path, "label": label,
                                         "size": os.path.getsize(path), "scan": scan, "node": {}}
                added.append(fid)
                continue
            reason = "node table only (%d nodes)" % len(scan["node_table"]) \
                if scan["node_table"] else "no 'histogram dump:' lines"
            skipped.append({"file": label, "reason": reason})
            continue
        fid = uuid.uuid4().hex[:12]
        session["files"][fid] = {"id": fid, "path": path, "label": label,
                                 "size": os.path.getsize(path), "scan": scan, "node": {}}
        added.append(fid)
    return added, skipped


def parse_dt_param(value):
    if not value:
        return None
    value = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", LOG_DT_FMT):
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "asloglatency-web"
    protocol_version = "HTTP/1.1"
    tool_path = DEFAULT_TOOL

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def send_payload(self, body, ctype="application/json; charset=utf-8", status=200):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, payload, status=200):
        self.send_payload(json.dumps(payload, separators=(",", ":")), status=status)

    def send_static(self, relpath):
        path = os.path.normpath(os.path.join(STATIC_DIR, relpath.lstrip("/")))
        if not path.startswith(STATIC_DIR) or not os.path.isfile(path):
            self.send_json({"error": "not found"}, 404)
            return
        types = {".html": "text/html", ".js": "application/javascript",
                 ".css": "text/css", ".svg": "image/svg+xml"}
        ctype = types.get(os.path.splitext(path)[1], "application/octet-stream")
        with open(path, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", "%s; charset=utf-8" % ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def read_body(self, limit=MAX_UPLOAD_BYTES):
        length = int(self.headers.get("Content-Length") or 0)
        if length > limit:
            raise ValueError("upload too large")
        return self.rfile.read(length) if length else b""

    def require_ingest_token(self):
        token = self.headers.get("X-Ingest-Token") or ""
        if not log_ingest.check_token(token, INGEST_TOKEN):
            raise PermissionError("invalid or missing X-Ingest-Token")

    def require_fetch_token(self, cfg):
        expected = log_fetch_ansible.fetch_token(cfg) or FETCH_TOKEN
        if not expected:
            return
        token = self.headers.get("X-Aero-Lens-Token") or ""
        if token != expected:
            raise PermissionError("invalid or missing X-Aero-Lens-Token")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self.send_static("index.html")
        elif parsed.path.startswith("/static/"):
            self.send_static(parsed.path[len("/static/"):])
        elif parsed.path == "/api/health":
            self.send_json({"ok": True, "tool": self.tool_path,
                            "tool_exists": os.path.isfile(self.tool_path),
                            "stale_sources": stale_sources(),
                            "started": iso_now(),
                            "ui_version": UI_VERSION,
                            "static_dir": STATIC_DIR,
                            "ingest_dir": INGEST_DIR,
                            "ingest_auth": bool(INGEST_TOKEN),
                            "fetch_config": FETCH_CLUSTERS_PATH,
                            "fetch_enabled": bool(log_fetch.load_config(FETCH_CLUSTERS_PATH))})
        elif parsed.path.startswith("/api/analysis/jobs/"):
            job_id = parsed.path[len("/api/analysis/jobs/"):]
            job = ANALYSIS_JOBS.get(job_id)
            self.send_json(job or {"error": "Analysis job not found or expired. The server may have restarted."},
                           200 if job else 404)
        elif parsed.path == "/api/fetch/clusters":
            cfg = log_fetch.load_config(FETCH_CLUSTERS_PATH)
            if not cfg:
                self.send_json({"error": "fetch not configured (copy fetch_clusters.example.json "
                                 "to fetch_clusters.json)"}, 503)
                return
            self.send_json(log_fetch_ansible.list_clusters(cfg))
        elif parsed.path == "/api/ingest/inventory":
            self.send_json(log_ingest.inventory(INGEST_DIR))
        elif parsed.path == "/api/reports":
            self.send_json({"dir": REPORTS_DIR, "reports": list_reports()})
        elif parsed.path.startswith("/api/reports/"):
            report_id = urllib.parse.unquote(parsed.path[len("/api/reports/"):])
            full_path, _ = report_paths(report_id)
            if os.path.basename(full_path) != report_id + ".json" or not os.path.isfile(full_path):
                self.send_json({"error": "no such report"}, 404)
                return
            with open(full_path, "rb") as fh:
                self.send_payload(fh.read(), "application/json; charset=utf-8")
        elif parsed.path == "/api/raw":
            query = urllib.parse.parse_qs(parsed.query)
            session = get_session((query.get("session") or [""])[0])
            run_id = (query.get("run") or [""])[0]
            raw = (session or {}).get("raw", {}).get(run_id)
            if raw is None:
                self.send_json({"error": "no raw output for that run"}, 404)
            else:
                self.send_payload(raw, "text/plain; charset=utf-8")
        elif parsed.path.startswith("/api/fetch/jobs/"):
            cfg = log_fetch.load_config(FETCH_CLUSTERS_PATH)
            if not cfg:
                self.send_json({"error": "fetch not configured"}, 503)
                return
            job_id = urllib.parse.unquote(parsed.path[len("/api/fetch/jobs/"):]).strip()
            job = log_fetch_jobs.load_job(cfg, job_id)
            if not job:
                self.send_json({"error": "unknown job"}, 404)
                return
            out = dict(job)
            session = get_session(job.get("session"))
            if session and job.get("status") == "completed":
                files = [public_file(f) for f in session["files"].values()]
                out["files"] = files
                out["cluster"] = session_cluster(session)
                out["latency_logs"] = sum(1 for f in files if f.get("histograms"))
            self.send_json(out)
        else:
            self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        try:
            if self.path == "/api/upload":
                self.handle_upload()
            elif self.path == "/api/analysis/jobs":
                self.handle_analysis_job()
            elif self.path == "/api/analyze":
                self.handle_analyze()
            elif self.path == "/api/errors":
                self.handle_errors()
            elif self.path == "/api/reset":
                self.handle_reset()
            elif self.path == "/api/reports":
                self.handle_save_report()
            elif self.path == "/api/reports/delete":
                self.handle_delete_report()
            elif self.path == "/api/ingest/v1/batch":
                self.handle_ingest_batch()
            elif self.path == "/api/ingest/errors":
                self.handle_ingest_errors()
            elif self.path == "/api/ingest/analyze":
                self.handle_ingest_analyze()
            elif self.path == "/api/fetch":
                self.handle_fetch()
            elif self.path == "/api/fetch/complete":
                self.handle_fetch_complete()
            else:
                self.send_json({"error": "not found"}, 404)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
            self.log_message("client disconnected during %s", self.path)
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, 401)
        except Exception as exc:
            self.log_message("error handling %s: %s", self.path, exc)
            self.send_json({"error": str(exc)}, 500)

    def handle_analysis_job(self):
        body = json.loads(self.read_body(limit=1024 * 1024) or b"{}")
        operations = {"/api/analyze": self.handle_analyze,
                      "/api/errors": self.handle_errors,
                      "/api/ingest/analyze": self.handle_ingest_analyze,
                      "/api/ingest/errors": self.handle_ingest_errors}
        operation = operations.get(body.get("endpoint"))
        request = body.get("request")
        if operation is None or not isinstance(request, dict):
            self.send_json({"error": "Valid analysis endpoint and request are required"}, 400)
            return
        key = json.dumps(body, sort_keys=True)
        try:
            job = ANALYSIS_JOBS.submit(
                lambda respond, progress: operation(request=request, respond=respond, progress=progress),
                key=key, session=request.get("session"))
        except JobLimitError as exc:
            self.send_json({"error": str(exc)}, 429)
            return
        job["poll"] = "/api/analysis/jobs/" + job["job_id"]
        self.send_json(job, 202)

    def handle_upload(self):
        sid = self.headers.get("X-Session") or uuid.uuid4().hex
        filename = safe_name(self.headers.get("X-Filename"))
        reap_sessions(keep=sid)
        session = get_session(sid, create=True)
        upload_dir = os.path.join(session["root"], "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        target = os.path.join(upload_dir, filename)
        base, ext = os.path.splitext(target)
        counter = 1
        while os.path.exists(target):
            target = "%s-%d%s" % (base, counter, ext)
            counter += 1

        remaining = int(self.headers.get("Content-Length") or 0)
        with open(target, "wb") as fh:
            while remaining > 0:
                chunk = self.rfile.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                fh.write(chunk)
                remaining -= len(chunk)

        started = time.time()
        added, skipped = ingest_upload_target(session, target)
        self.send_json(upload_result_payload(session, sid, started, added, skipped))

    def handle_fetch(self):
        cfg = log_fetch.load_config(FETCH_CLUSTERS_PATH)
        if not cfg:
            self.send_json({"error": "fetch not configured (copy fetch_clusters.example.json "
                             "to fetch_clusters.json)"}, 503)
            return
        request = json.loads(self.read_body() or b"{}")
        cluster_id = (request.get("cluster") or "").strip()
        if not cluster_id:
            self.send_json({"error": "cluster is required"}, 400)
            return
        try:
            incident = log_fetch.parse_incident_date(request.get("date") or "")
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return
        log_fetch.check_retention(incident, int(cfg.get("retention_days", 30)))

        mode = log_fetch_ansible.fetch_mode(cfg)
        if mode == "ansible":
            self.handle_fetch_ansible(cfg, request, cluster_id, incident)
        else:
            self.handle_fetch_direct(cfg, request, cluster_id, incident)

    def handle_fetch_ansible(self, cfg, request, cluster_id, incident):
        sid = request.get("session") or self.headers.get("X-Session") or uuid.uuid4().hex
        reap_sessions(keep=sid)
        get_session(sid, create=True)
        os.makedirs(log_fetch_jobs.spool_root(cfg), exist_ok=True)

        job = log_fetch_jobs.new_job(cfg, cluster_id, incident.isoformat(), sid)
        _, basenames = log_fetch.remote_files_for_date(cfg, incident)
        log_fetch_ansible.start_ansible_job(cfg, job, log_fn=self.log_message)

        self.send_json({
            "status": "collecting",
            "job_id": job["job_id"],
            "session": sid,
            "cluster": cluster_id,
            "date": incident.isoformat(),
            "resolved_pattern": basenames[0],
            "poll": "/api/fetch/jobs/%s" % job["job_id"],
            "fetch_mode": "ansible",
        }, 202)

    def handle_fetch_direct(self, cfg, request, cluster_id, incident):
        sid = request.get("session") or self.headers.get("X-Session") or uuid.uuid4().hex
        reap_sessions(keep=sid)
        session = get_session(sid, create=True)
        upload_dir = os.path.join(session["root"], "uploads")
        os.makedirs(upload_dir, exist_ok=True)

        fetch_started = time.time()
        try:
            fetch_info = log_fetch.fetch_cluster(cfg, cluster_id, incident, upload_dir)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        scan_started = time.time()
        added, skipped = [], []
        for node in fetch_info["nodes_fetched"]:
            if not node.get("ok"):
                for miss in node.get("attempted") or [{"basename": "?", "error": node.get("error")}]:
                    skipped.append({
                        "file": "%s:%s" % (node.get("host"), miss.get("basename", "?")),
                        "reason": miss.get("error") or node.get("error") or "fetch failed",
                    })
                continue
            for item in node.get("files") or []:
                local = item["local_path"]
                a, s = ingest_upload_target(session, local)
                added.extend(a)
                skipped.extend(s)

        payload = upload_result_payload(session, sid, scan_started, added, skipped)
        payload["status"] = "completed" if fetch_info["nodes_ok"] else "partial"
        payload["fetch_seconds"] = round(time.time() - fetch_started, 2)
        payload["fetch_mode"] = "direct"
        payload["fetch"] = {
            "cluster": fetch_info["cluster"],
            "date": fetch_info["date"],
            "resolved_pattern": fetch_info["resolved_pattern"],
            "nodes_ok": fetch_info["nodes_ok"],
            "nodes_total": fetch_info["nodes_total"],
            "nodes_fetched": fetch_info["nodes_fetched"],
        }
        if fetch_info["nodes_ok"] == 0:
            self.send_json(payload, 502)
            return
        self.send_json(payload)

    def handle_fetch_complete(self):
        cfg = log_fetch.load_config(FETCH_CLUSTERS_PATH)
        if not cfg:
            self.send_json({"error": "fetch not configured"}, 503)
            return
        self.require_fetch_token(cfg)
        request = json.loads(self.read_body() or b"{}")
        job_id = (request.get("job_id") or "").strip()
        bundle = (request.get("bundle") or "").strip()
        if not job_id or not bundle:
            self.send_json({"error": "job_id and bundle are required"}, 400)
            return
        bundle = os.path.basename(bundle)
        job = log_fetch_jobs.load_job(cfg, job_id)
        if not job:
            self.send_json({"error": "unknown job"}, 404)
            return

        bundle_path = os.path.join(log_fetch_jobs.incoming_dir(cfg), bundle)
        if not os.path.isfile(bundle_path):
            self.send_json({"error": "bundle not found at %s" % bundle_path}, 404)
            return

        sid = job["session"]
        session = get_session(sid, create=True)
        upload_dir = os.path.join(session["root"], "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        staged = os.path.join(upload_dir, safe_name(bundle))
        shutil.copy2(bundle_path, staged)

        scan_started = time.time()
        added, skipped = ingest_upload_target(session, staged)
        payload = upload_result_payload(session, sid, scan_started, added, skipped)
        payload["status"] = "completed"
        payload["job_id"] = job_id
        payload["fetch_mode"] = "ansible"
        log_fetch_jobs.patch_job(
            cfg, job_id, status="completed", bundle=bundle, error=None,
            latency_logs=payload.get("latency_logs"), scan_seconds=payload.get("scan_seconds"),
        )
        self.send_json(payload)

    def handle_analyze(self, request=None, respond=None, progress=None):
        if request is None:
            request = json.loads(self.read_body() or b"{}")
        respond = respond or self.send_json
        session = get_session(request.get("session"))
        if not session:
            return respond({"error": "unknown session; upload a log first"}, 400)

        file_ids = request.get("files") or list(session["files"].keys())
        selected = [session["files"][fid] for fid in file_ids if fid in session["files"]]
        if not selected:
            return respond({"error": "no matching uploaded files"}, 400)

        wanted = request.get("histograms") or []
        opts = {
            "slice_seconds": max(1, int(request.get("slice_seconds") or 10)),
            "num_buckets": max(1, min(17, int(request.get("num_buckets") or 6))),
            "every_nth": max(1, min(8, int(request.get("every_nth") or 1))),
            "alert_pct": float(request.get("alert_pct") or 1.0),
            "alert_threshold": float(request.get("alert_threshold") or 8),
            "min_ops_rate": float(request.get("min_ops_rate") or 50),
            "from_dt": parse_dt_param(request.get("from")),
            "to_dt": parse_dt_param(request.get("to")),
        }

        jobs = []
        for entry in selected:
            units = {h["name"]: h.get("unit") or "" for h in entry["scan"]["histograms"]}
            names = [h for h in wanted if h in units] if wanted else list(units.keys())
            for hist in names:
                jobs.append((entry, hist, units[hist]))
        if not jobs:
            return respond({"error": "selected histograms are not present in the selected logs"}, 400)

        if progress:
            progress(0, len(jobs))
        started = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=min(6, (os.cpu_count() or 2))) as pool:
            futures = {pool.submit(run_one, self.tool_path, entry, hist, unit, opts):
                       (entry, hist) for entry, hist, unit in jobs}
            for future in as_completed(futures):
                entry, hist = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    self.log_message("run failed for %s %s: %s", entry["label"], hist, exc)
                    results.append(failed_run(entry, hist, exc))
                if progress:
                    progress(len(results), len(jobs))

        results.sort(key=lambda r: (OP_ORDER.index(r["op"]) if r["op"] in OP_ORDER else 99,
                                    r["file"], r["histogram"]))
        session["raw"] = {r["id"]: r.pop("raw") for r in results}
        aggregate, cumulative = build_rollups(results, opts)

        return respond({
            "session": session["id"],
            "elapsed_seconds": round(time.time() - started, 2),
            "cluster": session_cluster(session),
            "options": {
                "slice_seconds": opts["slice_seconds"], "num_buckets": opts["num_buckets"],
                "every_nth": opts["every_nth"], "alert_pct": opts["alert_pct"],
                "alert_threshold": opts["alert_threshold"], "min_ops_rate": opts["min_ops_rate"],
                "from": opts["from_dt"].isoformat(sep=" ") if opts["from_dt"] else None,
                "to": opts["to_dt"].isoformat(sep=" ") if opts["to_dt"] else None,
                "mode": "window" if opts["from_dt"] else "complete log",
            },
            "totals": {
                "runs": len(results),
                "with_data": sum(1 for r in results if r["summary"]["status"] == "ok"),
                "logs": len(selected),
                "slices": sum(len(r["series"]["t"]) for r in results),
            },
            "aggregate": aggregate,
            "cumulative": cumulative,
            "results": results,
        })

    def handle_errors(self, request=None, respond=None, progress=None):
        if request is None:
            request = json.loads(self.read_body() or b"{}")
        respond = respond or self.send_json
        session = get_session(request.get("session"))
        if not session:
            return respond({"error": "unknown session; upload a log first"}, 400)

        file_ids = request.get("files") or list(session["files"].keys())
        selected = [session["files"][fid] for fid in file_ids if fid in session["files"]]
        if not selected:
            return respond({"error": "no matching uploaded files"}, 400)

        from_dt = parse_dt_param(request.get("from"))
        to_dt = parse_dt_param(request.get("to"))
        window = (epoch(from_dt) if from_dt else None, epoch(to_dt) if to_dt else None)

        starts = [epoch(datetime.datetime.strptime(e["scan"]["first_ts"], LOG_DT_FMT))
                  for e in selected if e["scan"]["first_ts"]]
        ends = [epoch(datetime.datetime.strptime(e["scan"]["last_ts"], LOG_DT_FMT))
                for e in selected if e["scan"]["last_ts"]]
        if not starts:
            return respond({"error": "selected logs have no timestamped lines"}, 400)
        coverage = (min(starts), max(ends or starts))

        # The scan is seconds per gigabyte, so repeat views of the same window are cached.
        key = json.dumps([sorted(f["id"] for f in selected), window], sort_keys=True)
        cached = session.get("error_cache", {}).get(key)
        if cached is not None:
            return respond(dict(cached, cached=True))

        entries = [(f["path"], f["node"]) for f in selected]
        report = error_analysis.analyze_errors(entries, window, coverage)
        report["session"] = session["id"]
        report["cluster"] = session_cluster(session)
        report["cached"] = False
        session.setdefault("error_cache", {})[key] = report
        return respond(report)

    def handle_save_report(self):
        request = json.loads(self.read_body() or b"{}")
        if not request.get("latency") and not request.get("errors"):
            self.send_json({"error": "nothing to save; run an analysis first"}, 400)
            return
        meta = write_report(request.get("name"), request.get("cluster"), request)
        self.send_json({"saved": meta, "dir": REPORTS_DIR})

    def handle_delete_report(self):
        request = json.loads(self.read_body() or b"{}")
        report_id = request.get("id") or ""
        full_path, meta_path = report_paths(report_id)
        if os.path.basename(full_path) != report_id + ".json":
            self.send_json({"error": "bad report id"}, 400)
            return
        for path in (full_path, meta_path):
            try:
                os.remove(path)
            except OSError:
                pass
        self.send_json({"ok": True, "reports": list_reports()})

    def handle_reset(self):
        request = json.loads(self.read_body() or b"{}")
        if request.get("session") and ANALYSIS_JOBS.active_for_session(request["session"]):
            self.send_json({"error": "Analysis is still running. Wait for it to finish before clearing logs."}, 409)
            return
        with SESSION_LOCK:
            session = SESSIONS.pop(request.get("session"), None)
        if session:
            shutil.rmtree(session["root"], ignore_errors=True)
        self.send_json({"ok": True})

    def handle_ingest_batch(self):
        self.require_ingest_token()
        request = json.loads(self.read_body(MAX_INGEST_BODY_BYTES) or b"{}")
        lines = request.get("lines") or []
        result = log_ingest.append_batch(
            INGEST_DIR,
            request.get("cluster"),
            request.get("node"),
            lines,
            hostname=request.get("hostname"),
        )
        self.send_json({"ok": True, **result})

    def materialize_ingest_nodes(self, cluster, nodes, from_dt, to_dt):
        """Build temp log files for each node; caller must unlink paths in `cleanup`."""
        start_epoch = epoch(from_dt) if from_dt else None
        end_epoch = epoch(to_dt) if to_dt else None
        cleanup = []
        built = []
        try:
            for node in nodes:
                path, hint = log_ingest.materialize_slice(
                    INGEST_DIR, cluster, node, start_epoch, end_epoch)
                cleanup.append(path)
                scan = scan_file(path)
                if not scan["first_ts"] and hint.get("first_ts"):
                    scan["first_ts"] = hint["first_ts"]
                    scan["last_ts"] = hint.get("last_ts")
                built.append({
                    "path": path,
                    "node": hint["node"],
                    "scan": scan,
                    "label": "%s / %s" % (cluster, node),
                })
            return built, cleanup
        except Exception:
            for path in cleanup:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            raise

    def handle_ingest_errors(self, request=None, respond=None, progress=None):
        if request is None:
            request = json.loads(self.read_body() or b"{}")
        respond = respond or self.send_json
        cluster = request.get("cluster")
        nodes = request.get("nodes") or []
        if not cluster or not nodes:
            return respond({"error": "cluster and nodes[] are required"}, 400)

        from_dt = parse_dt_param(request.get("from"))
        to_dt = parse_dt_param(request.get("to"))
        window = (epoch(from_dt) if from_dt else None, epoch(to_dt) if to_dt else None)

        built, cleanup = self.materialize_ingest_nodes(cluster, nodes, from_dt, to_dt)
        try:
            starts = [epoch(datetime.datetime.strptime(e["scan"]["first_ts"], LOG_DT_FMT))
                      for e in built if e["scan"]["first_ts"]]
            ends = [epoch(datetime.datetime.strptime(e["scan"]["last_ts"], LOG_DT_FMT))
                    for e in built if e["scan"]["last_ts"]]
            if not starts:
                return respond({"error": "no timestamped lines in ingested slice"}, 400)
            coverage = (min(starts), max(ends or starts))
            entries = [(e["path"], e["node"]) for e in built]
            report = error_analysis.analyze_errors(entries, window, coverage)
            report["source"] = "ingest"
            report["cluster"] = {"name": cluster, "nodes": len(nodes)}
            report["ingest"] = {"cluster": cluster, "nodes": nodes, "dir": INGEST_DIR}
            return respond(report)
        finally:
            for path in cleanup:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    def handle_ingest_analyze(self, request=None, respond=None, progress=None):
        if request is None:
            request = json.loads(self.read_body() or b"{}")
        respond = respond or self.send_json
        cluster = request.get("cluster")
        nodes = request.get("nodes") or []
        if not cluster or not nodes:
            return respond({"error": "cluster and nodes[] are required"}, 400)

        from_dt = parse_dt_param(request.get("from"))
        to_dt = parse_dt_param(request.get("to"))
        wanted = request.get("histograms") or []
        opts = {
            "slice_seconds": max(1, int(request.get("slice_seconds") or 10)),
            "num_buckets": max(1, min(17, int(request.get("num_buckets") or 6))),
            "every_nth": max(1, min(8, int(request.get("every_nth") or 1))),
            "alert_pct": float(request.get("alert_pct") or 1.0),
            "alert_threshold": float(request.get("alert_threshold") or 8),
            "min_ops_rate": float(request.get("min_ops_rate") or 50),
            "from_dt": from_dt,
            "to_dt": to_dt,
        }

        built, cleanup = self.materialize_ingest_nodes(cluster, nodes, from_dt, to_dt)
        try:
            jobs = []
            for entry in built:
                units = {h["name"]: h.get("unit") or "" for h in entry["scan"]["histograms"]}
                names = [h for h in wanted if h in units] if wanted else list(units.keys())
                file_entry = {"path": entry["path"], "label": entry["label"],
                              "scan": entry["scan"], "node": entry["node"]}
                for hist in names:
                    jobs.append((file_entry, hist, units[hist]))
            if not jobs:
                return respond({"error": "no histogram dumps in ingested slice for selected nodes"}, 400)

            if progress:
                progress(0, len(jobs))
            started = time.time()
            results = []
            with ThreadPoolExecutor(max_workers=min(6, (os.cpu_count() or 2))) as pool:
                futures = {pool.submit(run_one, self.tool_path, entry, hist, unit, opts):
                           (entry, hist) for entry, hist, unit in jobs}
                for future in as_completed(futures):
                    entry, hist = futures[future]
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        self.log_message("ingest run failed for %s %s: %s", entry["label"], hist, exc)
                        results.append(failed_run(entry, hist, exc))
                    if progress:
                        progress(len(results), len(jobs))

            results.sort(key=lambda r: (OP_ORDER.index(r["op"]) if r["op"] in OP_ORDER else 99,
                                        r["file"], r["histogram"]))
            raw_map = {r["id"]: r.pop("raw") for r in results}
            aggregate, cumulative = build_rollups(results, opts)
            return respond({
                "source": "ingest",
                "elapsed_seconds": round(time.time() - started, 2),
                "cluster": {"name": cluster, "nodes": len(nodes)},
                "ingest": {"cluster": cluster, "nodes": nodes, "dir": INGEST_DIR},
                "options": {
                    "slice_seconds": opts["slice_seconds"], "num_buckets": opts["num_buckets"],
                    "every_nth": opts["every_nth"], "alert_pct": opts["alert_pct"],
                    "alert_threshold": opts["alert_threshold"], "min_ops_rate": opts["min_ops_rate"],
                    "from": opts["from_dt"].isoformat(sep=" ") if opts["from_dt"] else None,
                    "to": opts["to_dt"].isoformat(sep=" ") if opts["to_dt"] else None,
                    "mode": "window" if opts["from_dt"] else "complete log",
                },
                "totals": {
                    "runs": len(results),
                    "with_data": sum(1 for r in results if r["summary"]["status"] == "ok"),
                    "logs": len(built),
                    "slices": sum(len(r["series"]["t"]) for r in results),
                },
                "aggregate": aggregate,
                "cumulative": cumulative,
                "results": results,
            })
        finally:
            for path in cleanup:
                try:
                    os.unlink(path)
                except OSError:
                    pass


def main():
    global REPORTS_DIR          # --reports-dir overrides the module default
    global INGEST_DIR, INGEST_TOKEN, FETCH_CLUSTERS_PATH, FETCH_TOKEN
    parser = argparse.ArgumentParser(description="Web UI for asloglatency")
    parser.add_argument("--host", default="127.0.0.1",
                        help="listen address (use 0.0.0.0 to receive forwarders)")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--tool", default=DEFAULT_TOOL, help="path to log_latency.py")
    parser.add_argument("--reports-dir", default=REPORTS_DIR,
                        help="where saved reports are written (default: %s)" % REPORTS_DIR)
    parser.add_argument("--ingest-dir", default=INGEST_DIR,
                        help="where forwarded logs are stored (default: %s)" % INGEST_DIR)
    parser.add_argument("--ingest-token", default=INGEST_TOKEN,
                        help="require X-Ingest-Token on POST /api/ingest/v1/batch (empty = open)")
    parser.add_argument("--fetch-config", default=FETCH_CLUSTERS_PATH,
                        help="cluster catalog for POST /api/fetch (default: webapp/fetch_clusters.json)")
    args = parser.parse_args()

    REPORTS_DIR = os.path.abspath(os.path.expanduser(args.reports_dir))
    os.makedirs(REPORTS_DIR, exist_ok=True)
    INGEST_DIR = os.path.abspath(os.path.expanduser(args.ingest_dir))
    os.makedirs(INGEST_DIR, exist_ok=True)
    INGEST_TOKEN = (args.ingest_token or "").strip()
    FETCH_CLUSTERS_PATH = os.path.abspath(os.path.expanduser(args.fetch_config))
    _fcfg = log_fetch.load_config(FETCH_CLUSTERS_PATH) or {}
    FETCH_TOKEN = (log_fetch_ansible.fetch_token(_fcfg) or "").strip()

    tool = os.path.abspath(os.path.expanduser(args.tool))
    if not os.path.isfile(tool):
        sys.exit("asloglatency not found at %s (use --tool)" % tool)
    Handler.tool_path = tool

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print("asloglatency web UI on http://%s:%d" % (args.host, args.port))
    print("using tool: %s" % tool)
    print("saved reports: %s" % REPORTS_DIR)
    print("ingest store: %s%s" % (INGEST_DIR, " (token required)" if INGEST_TOKEN else ""))
    if log_fetch.load_config(FETCH_CLUSTERS_PATH):
        listed = log_fetch.list_clusters(log_fetch.load_config(FETCH_CLUSTERS_PATH))
        print("fetch catalog: %s (%d cluster(s))" % (FETCH_CLUSTERS_PATH, len(listed["clusters"])))
    else:
        print("fetch: disabled (no %s)" % FETCH_CLUSTERS_PATH)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        httpd.server_close()
        with SESSION_LOCK:
            for session in SESSIONS.values():
                shutil.rmtree(session["root"], ignore_errors=True)


if __name__ == "__main__":
    main()
