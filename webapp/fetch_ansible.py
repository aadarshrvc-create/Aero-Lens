"""Trigger log collect on Ansible controller; bundle arrives via scp + /api/fetch/complete."""

import datetime
import json
import os
import subprocess
import threading

import fetch as log_fetch
import fetch_jobs


def fetch_mode(cfg):
    return (cfg.get("fetch_mode") or "direct").strip().lower()


def ansible_settings(cfg):
    return cfg.get("ansible") or {}


def delivery_settings(cfg):
    return cfg.get("delivery") or {}


def fetch_token(cfg):
    return (cfg.get("fetch_token") or "").strip()


def list_clusters(cfg):
    base = log_fetch.list_clusters(cfg)
    base["fetch_mode"] = fetch_mode(cfg)
    for entry in base["clusters"]:
        meta = (cfg.get("clusters") or {}).get(entry["id"], {})
        entry["ansible_group"] = meta.get("ansible_group")
        if not entry.get("node_count") and meta.get("ansible_group"):
            entry["node_count"] = meta.get("node_count")
    return base


def cluster_ansible_group(cfg, cluster_id):
    meta = log_fetch.cluster_meta(cfg, cluster_id)
    group = (meta.get("ansible_group") or "").strip()
    if not group:
        raise ValueError("cluster %r has no ansible_group in config" % cluster_id)
    return group


def bundle_name(job_id):
    return "%s.tar.gz" % job_id


def trigger_ansible_collect(cfg, job_id, cluster_id, incident):
    """SSH to Ansible host and run remote collect script (long-running)."""
    ansible = ansible_settings(cfg)
    host = (ansible.get("host") or "").strip()
    user = (ansible.get("user") or "dbops").strip()
    script = (ansible.get("remote_script") or "").strip()
    if not host or not script:
        raise ValueError("ansible.host and ansible.remote_script required in fetch config")

    delivery = delivery_settings(cfg)
    group = cluster_ansible_group(cfg, cluster_id)
    retention = int(cfg.get("retention_days", 30))
    log_fetch.check_retention(incident, retention)

    _, basenames = log_fetch.remote_files_for_date(cfg, incident)
    plus = int(cfg.get("rotation_suffix_plus_days", 1))
    payload = {
        "job_id": job_id,
        "cluster": cluster_id,
        "ansible_group": group,
        "date": incident.isoformat(),
        "log_dir": cfg.get("log_dir", "/var/log/aerospike"),
        "rotation_suffix_plus_days": plus,
        "log_basenames": basenames,
        "delivery": {
            "tool_host": delivery.get("tool_host") or "",
            "tool_user": delivery.get("tool_user") or "dbops",
            "incoming_dir": fetch_jobs.incoming_dir(cfg),
            "bundle_name": bundle_name(job_id),
            "callback_url": delivery.get("callback_url") or "",
        },
        "fetch_token": fetch_token(cfg),
    }

    ssh_opts = ansible.get("ssh_options") or [
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=30",
    ]
    remote = "%s@%s" % (user, host)
    jobs_dir = fetch_jobs.jobs_dir(cfg)
    local_payload = os.path.join(jobs_dir, "%s.ansible.json" % job_id)
    with open(local_payload, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)

    remote_payload = "/tmp/aero_lens_%s.json" % job_id
    scp_cmd = ["scp", "-q"]
    scp_cmd.extend(ssh_opts)
    scp_cmd.extend([local_payload, "%s:%s" % (remote, remote_payload)])
    scp_proc = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=60)
    if scp_proc.returncode != 0:
        return False, (scp_proc.stderr or scp_proc.stdout or "scp payload failed").strip(), scp_cmd

    # Remote script can take many minutes; start in background on Ansible host.
    remote_cmd = (
        "nohup %s %s </dev/null >>/tmp/aero_lens_%s.log 2>&1 & echo started"
        % (script, remote_payload, job_id)
    )
    cmd = ["ssh", "-q"]
    cmd.extend(ssh_opts)
    cmd.extend([remote, remote_cmd])

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=ansible.get("ssh_timeout", 60))
    return proc.returncode == 0 and "started" in (proc.stdout or ""), (
        (proc.stderr or proc.stdout or "").strip()
    ), cmd


def start_ansible_job(cfg, job, log_fn=None):
    """Background: trigger Ansible; job completes when /api/fetch/complete runs."""

    def runner():
        fetch_jobs.patch_job(cfg, job["job_id"], status="triggering")
        try:
            incident = log_fetch.parse_incident_date(job["date"])
            ok, err, cmd = trigger_ansible_collect(cfg, job["job_id"], job["cluster"], incident)
            fetch_jobs.patch_job(
                cfg, job["job_id"],
                status="collecting" if ok else "failed",
                error=None if ok else err,
                ansible={"trigger_ok": ok, "detail": err[:2000] if err else "", "command": " ".join(cmd[:6])},
            )
            if log_fn:
                log_fn("ansible trigger job=%s ok=%s %s" % (job["job_id"], ok, err[:200] if err else ""))
        except Exception as exc:
            fetch_jobs.patch_job(cfg, job["job_id"], status="failed", error=str(exc))
            if log_fn:
                log_fn("ansible trigger job=%s exception: %s" % (job["job_id"], exc))

    threading.Thread(target=runner, daemon=True).start()