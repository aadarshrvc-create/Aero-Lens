"""Splunk-style log ingest: append-only hourly files per cluster/node."""

import collections
import datetime
import json
import os
import re
import tempfile
import threading
import time

LOG_DT_FMT = "%b %d %Y %H:%M:%S"
TS_RE = re.compile(r"^([A-Z][a-z]{2} \d{2} \d{4} \d{2}:\d{2}:\d{2})")
SAFE_NAME_RE = re.compile(r"[^\w.\-]+")

INGEST_LOCK = threading.Lock()
REGISTRY_NAME = "registry.json"
MAX_BATCH_LINES = 50000
MAX_LINE_BYTES = 65536


def safe_segment(name):
    name = (name or "unknown").strip()
    out = SAFE_NAME_RE.sub("_", name)
    return out[:120] or "unknown"


def store_root(ingest_dir):
    return os.path.abspath(os.path.expanduser(ingest_dir))


def registry_path(ingest_dir):
    return os.path.join(store_root(ingest_dir), REGISTRY_NAME)


def load_registry(ingest_dir):
    path = registry_path(ingest_dir)
    if not os.path.isfile(path):
        return {"version": 1, "clusters": {}}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_registry(ingest_dir, registry):
    root = store_root(ingest_dir)
    os.makedirs(root, exist_ok=True)
    path = registry_path(ingest_dir)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(registry, fh, separators=(",", ":"), sort_keys=True)
    os.replace(tmp, path)


def hour_key_from_line(line, fallback_dt):
    match = TS_RE.match(line)
    if match:
        try:
            dt = datetime.datetime.strptime(match.group(1), LOG_DT_FMT)
            return dt.strftime("%Y-%m-%d-%H"), dt
        except ValueError:
            pass
    return fallback_dt.strftime("%Y-%m-%d-%H"), None


def chunk_path(ingest_dir, cluster, node, hour_key):
    return os.path.join(
        store_root(ingest_dir),
        safe_segment(cluster),
        safe_segment(node),
        "%s.log" % hour_key,
    )


def append_batch(ingest_dir, cluster, node, lines, hostname=None):
    """Persist raw log lines; bucket by Aerospike GMT hour when parseable."""
    if not cluster or not node:
        raise ValueError("cluster and node are required")
    if not lines:
        return {"lines": 0, "bytes": 0, "chunks": []}
    if len(lines) > MAX_BATCH_LINES:
        raise ValueError("batch exceeds %d lines" % MAX_BATCH_LINES)

    now = datetime.datetime.utcnow()
    now_epoch = int(time.time())
    by_hour = collections.defaultdict(list)
    bytes_written = 0
    first_dt = last_dt = None

    for raw in lines:
        if not isinstance(raw, str):
            raw = str(raw)
        line = raw.rstrip("\r\n") + "\n"
        if len(line) > MAX_LINE_BYTES:
            line = line[:MAX_LINE_BYTES] + "\n"
        hour_key, parsed = hour_key_from_line(line, now)
        by_hour[hour_key].append(line)
        bytes_written += len(line.encode("utf-8", errors="replace"))
        if parsed:
            if first_dt is None or parsed < first_dt:
                first_dt = parsed
            if last_dt is None or parsed > last_dt:
                last_dt = parsed

    touched = []
    with INGEST_LOCK:
        for hour_key, chunk_lines in by_hour.items():
            path = chunk_path(ingest_dir, cluster, node, hour_key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8", errors="replace") as fh:
                fh.writelines(chunk_lines)
            touched.append(hour_key)

        registry = load_registry(ingest_dir)
        clusters = registry.setdefault("clusters", {})
        centry = clusters.setdefault(cluster, {"nodes": {}})
        nentry = centry["nodes"].setdefault(node, {
            "hostname": hostname or node,
            "first_seen": now_epoch,
            "last_seen": now_epoch,
            "bytes": 0,
            "first_log": None,
            "last_log": None,
        })
        if hostname:
            nentry["hostname"] = hostname
        nentry["last_seen"] = now_epoch
        nentry["bytes"] = nentry.get("bytes", 0) + bytes_written
        if first_dt:
            iso = first_dt.strftime("%Y-%m-%d %H:%M:%S")
            if not nentry.get("first_log") or iso < nentry["first_log"]:
                nentry["first_log"] = iso
        if last_dt:
            iso = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            if not nentry.get("last_log") or iso > nentry["last_log"]:
                nentry["last_log"] = iso
        save_registry(ingest_dir, registry)

    return {"lines": len(lines), "bytes": bytes_written, "chunks": sorted(touched)}


def list_hour_keys(start_epoch, end_epoch):
    """Inclusive UTC hour keys covering [start_epoch, end_epoch]."""
    if start_epoch is None and end_epoch is None:
        return None
    start = datetime.datetime.utcfromtimestamp(start_epoch or 0)
    end = datetime.datetime.utcfromtimestamp(end_epoch or int(time.time()))
    start = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)
    keys = []
    cur = start
    while cur <= end:
        keys.append(cur.strftime("%Y-%m-%d-%H"))
        cur += datetime.timedelta(hours=1)
    return keys


def iter_node_chunks(ingest_dir, cluster, node, hour_keys=None):
    base = os.path.join(store_root(ingest_dir), safe_segment(cluster), safe_segment(node))
    if not os.path.isdir(base):
        return []
    names = sorted(fn for fn in os.listdir(base) if fn.endswith(".log"))
    if hour_keys is not None:
        wanted = set("%s.log" % h for h in hour_keys)
        names = [n for n in names if n in wanted]
    return [os.path.join(base, n) for n in names]


def materialize_slice(ingest_dir, cluster, node, start_epoch=None, end_epoch=None):
    """Concatenate stored chunks into one temp file; return (path, scan_hints)."""
    hour_keys = list_hour_keys(start_epoch, end_epoch)
    chunks = iter_node_chunks(ingest_dir, cluster, node, hour_keys)
    if not chunks:
        raise FileNotFoundError("no ingested logs for %s / %s in range" % (cluster, node))

    fd, path = tempfile.mkstemp(prefix="ingest-%s-" % safe_segment(node), suffix=".log")
    os.close(fd)
    first_ts = last_ts = None
    with open(path, "wb") as out:
        for chunk in chunks:
            with open(chunk, "rb") as fh:
                while True:
                    block = fh.read(1024 * 1024)
                    if not block:
                        break
                    out.write(block)
                    if first_ts is None:
                        text = block.decode("utf-8", errors="replace")
                        for line in text.splitlines():
                            match = TS_RE.match(line)
                            if match:
                                first_ts = match.group(1)
                                break
    # last timestamp: scan tail only
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - 256 * 1024))
        tail = fh.read().decode("utf-8", errors="replace")
    for line in reversed(tail.splitlines()):
        match = TS_RE.match(line)
        if match:
            last_ts = match.group(1)
            break

    node_info = {"ip": node, "name": node}
    registry = load_registry(ingest_dir)
    meta = registry.get("clusters", {}).get(cluster, {}).get("nodes", {}).get(node, {})
    if meta.get("hostname"):
        node_info["name"] = meta["hostname"]

    return path, {
        "first_ts": first_ts,
        "last_ts": last_ts,
        "node": node_info,
        "chunks": len(chunks),
        "cluster": cluster,
    }


def inventory(ingest_dir):
    registry = load_registry(ingest_dir)
    clusters = []
    for cluster, centry in sorted(registry.get("clusters", {}).items()):
        nodes = []
        for node, meta in sorted(centry.get("nodes", {}).items()):
            nodes.append({
                "node": node,
                "hostname": meta.get("hostname") or node,
                "first_seen": meta.get("first_seen"),
                "last_seen": meta.get("last_seen"),
                "first_log": meta.get("first_log"),
                "last_log": meta.get("last_log"),
                "bytes": meta.get("bytes", 0),
            })
        clusters.append({"cluster": cluster, "nodes": nodes, "node_count": len(nodes)})
    return {"dir": store_root(ingest_dir), "clusters": clusters}


def check_token(header_token, configured_token):
    if not configured_token:
        return True
    return header_token == configured_token
