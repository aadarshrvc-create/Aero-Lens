"""Pull rotated aerospike.log files from cluster nodes via scp (stdlib only)."""

import datetime
import json
import os
import subprocess


def store_root(path):
    return os.path.abspath(os.path.expanduser(path)) if path else ""


def load_config(path):
    path = store_root(path)
    if not path or not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def list_clusters(cfg):
    clusters = cfg.get("clusters") or {}
    out = []
    for cid, meta in sorted(clusters.items()):
        nodes = meta.get("nodes") or []
        out.append({
            "id": cid,
            "label": meta.get("label") or cid,
            "node_count": len(nodes),
        })
    return {
        "retention_days": int(cfg.get("retention_days", 30)),
        "clusters": out,
    }


def parse_incident_date(text):
    try:
        return datetime.date.fromisoformat(text.strip())
    except ValueError as exc:
        raise ValueError("invalid date (use YYYY-MM-DD)") from exc


def check_retention(incident, retention_days, today=None):
    today = today or datetime.date.today()
    if incident > today:
        raise ValueError("date is in the future")
    age = (today - incident).days
    if age > retention_days:
        raise ValueError(
            "date older than retention (%d days); log is no longer on nodes" % retention_days
        )


def rotated_basename(incident, plus_days=1):
    """Maps calendar incident day to aerospike.log-YYYYMMDD.gz (suffix is D+1)."""
    rotated = incident + datetime.timedelta(days=int(plus_days))
    return "aerospike.log-%s.gz" % rotated.strftime("%Y%m%d")


def remote_files_for_date(cfg, incident, today=None):
    today = today or datetime.date.today()
    plus = int(cfg.get("rotation_suffix_plus_days", 1))
    log_dir = cfg.get("log_dir", "/var/log/aerospike").rstrip("/")
    names = [rotated_basename(incident, plus)]
    if incident == today:
        names.append("aerospike.log")
    return ["%s/%s" % (log_dir, n) for n in names], names


def cluster_meta(cfg, cluster_id):
    clusters = cfg.get("clusters") or {}
    if cluster_id not in clusters:
        raise ValueError("unknown cluster %r" % cluster_id)
    return clusters[cluster_id]


def scp_from_host(user, host, remote_path, local_path, ssh_options=None):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    remote = "%s@%s:%s" % (user, host, remote_path)
    cmd = ["scp", "-q"]
    if ssh_options:
        cmd.extend(ssh_options)
    else:
        cmd.extend([
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=30",
        ])
    cmd.extend([remote, local_path])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ok = proc.returncode == 0 and os.path.isfile(local_path) and os.path.getsize(local_path) > 0
    err = (proc.stderr or proc.stdout or "").strip()
    if ok:
        return True, err
    if os.path.isfile(local_path):
        try:
            os.remove(local_path)
        except OSError:
            pass
    return False, err or "scp failed (exit %d)" % proc.returncode


def fetch_cluster(cfg, cluster_id, incident, dest_dir, today=None):
    """Download log files from each node. Returns fetch result dict (no session scan)."""
    today = today or datetime.date.today()
    retention = int(cfg.get("retention_days", 30))
    check_retention(incident, retention, today=today)
    meta = cluster_meta(cfg, cluster_id)
    nodes = meta.get("nodes") or []
    if not nodes:
        raise ValueError("cluster %r has no nodes in config" % cluster_id)

    user = cfg.get("ssh_user") or "root"
    ssh_options = cfg.get("ssh_options")
    remote_paths, basenames = remote_files_for_date(cfg, incident, today=today)
    primary_name = basenames[0]

    os.makedirs(dest_dir, exist_ok=True)
    nodes_out = []
    for node in nodes:
        host = node.get("host") or node.get("ip")
        hostname = node.get("hostname") or host
        if not host:
            nodes_out.append({
                "host": hostname, "hostname": hostname, "ok": False,
                "error": "node missing host in config", "files": [],
            })
            continue

        fetched = []
        errors = []
        for remote, basename in zip(remote_paths, basenames):
            safe_host = (hostname or host).replace("/", "_")
            local = os.path.join(dest_dir, "%s__%s" % (safe_host, basename))
            ok, err = scp_from_host(user, host, remote, local, ssh_options=ssh_options)
            if ok:
                fetched.append({
                    "local_path": local,
                    "basename": basename,
                    "remote": remote,
                    "bytes": os.path.getsize(local),
                })
            else:
                errors.append({"basename": basename, "remote": remote, "error": err})

        if not fetched and errors:
            nodes_out.append({
                "host": host,
                "hostname": hostname,
                "ok": False,
                "error": errors[0].get("error", "no file fetched"),
                "files": [],
                "attempted": errors,
            })
        else:
            nodes_out.append({
                "host": host,
                "hostname": hostname,
                "ok": bool(fetched),
                "error": None if fetched else "no file fetched",
                "files": fetched,
                "attempted": errors,
            })

    ok_count = sum(1 for n in nodes_out if n["ok"])
    return {
        "cluster": cluster_id,
        "date": incident.isoformat(),
        "resolved_pattern": primary_name,
        "remote_paths": remote_paths,
        "nodes_fetched": nodes_out,
        "nodes_ok": ok_count,
        "nodes_total": len(nodes_out),
    }