#!/usr/bin/env python3
"""
Tail aerospike.log and push batches to the analyzer ingest API (Splunk UF style).

    python3 forwarder.py --config forwarder.json

See forwarder.example.json for fields.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    required = ("url", "cluster", "node", "log_path")
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise SystemExit("config missing: %s" % ", ".join(missing))
    cfg.setdefault("batch_max_lines", 400)
    cfg.setdefault("batch_max_bytes", 512 * 1024)
    cfg.setdefault("poll_seconds", 1.0)
    cfg.setdefault("state_path", os.path.join(os.path.dirname(path), "forwarder-state.json"))
    cfg.setdefault("hostname", os.uname().nodename)
    if not cfg["url"].rstrip("/").endswith("/api/ingest/v1/batch"):
        base = cfg["url"].rstrip("/")
        cfg["url"] = base + "/api/ingest/v1/batch"
    return cfg


def load_state(path):
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(path, state):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)


def file_identity(path):
    st = os.stat(path)
    return {"path": os.path.abspath(path), "inode": st.st_ino, "size": st.st_size}


def read_new_lines(cfg, state):
    path = cfg["log_path"]
    if not os.path.isfile(path):
        return [], state

    ident = file_identity(path)
    key = ident["path"]
    saved = state.get(key, {})
    offset = saved.get("offset", 0)
    if saved.get("inode") != ident["inode"]:
        # rotation or new file: if file shrank, read from 0; else continue if same path
        if ident["size"] < offset:
            offset = 0

    if ident["size"] <= offset:
        state[key] = {"inode": ident["inode"], "offset": offset}
        return [], state

    lines = []
    bytes_read = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        fh.seek(offset)
        while True:
            line = fh.readline()
            if not line:
                break
            lines.append(line.rstrip("\r\n"))
            bytes_read += len(line.encode("utf-8", errors="replace"))
            if len(lines) >= cfg["batch_max_lines"] or bytes_read >= cfg["batch_max_bytes"]:
                break
        offset = fh.tell()

    state[key] = {"inode": ident["inode"], "offset": offset}
    return lines, state


def post_batch(cfg, lines):
    body = json.dumps({
        "cluster": cfg["cluster"],
        "node": cfg["node"],
        "hostname": cfg.get("hostname"),
        "lines": lines,
    }).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    token = cfg.get("token")
    if token:
        headers["X-Ingest-Token"] = token
    req = urllib.request.Request(cfg["url"], data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Aerospike log forwarder")
    parser.add_argument("--config", required=True, help="path to forwarder.json")
    parser.add_argument("--once", action="store_true", help="send one batch and exit")
    args = parser.parse_args()

    cfg = load_config(os.path.abspath(args.config))
    state = load_state(cfg["state_path"])
    print("forwarding %s -> %s cluster=%s node=%s" % (
        cfg["log_path"], cfg["url"], cfg["cluster"], cfg["node"]), flush=True)

    while True:
        lines, state = read_new_lines(cfg, state)
        if lines:
            try:
                result = post_batch(cfg, lines)
                save_state(cfg["state_path"], state)
                print("%s sent %d lines (%d bytes) chunks=%s" % (
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    result.get("lines", len(lines)),
                    result.get("bytes", 0),
                    ",".join(result.get("chunks") or []),
                ), flush=True)
            except urllib.error.HTTPError as exc:
                err = exc.read().decode("utf-8", errors="replace")
                print("ingest HTTP %s: %s" % (exc.code, err[:500]), file=sys.stderr, flush=True)
                if exc.code >= 400 and exc.code < 500:
                    save_state(cfg["state_path"], state)
            except Exception as exc:
                print("ingest failed: %s" % exc, file=sys.stderr, flush=True)
        else:
            save_state(cfg["state_path"], state)

        if args.once:
            break
        time.sleep(cfg["poll_seconds"])


if __name__ == "__main__":
    main()
