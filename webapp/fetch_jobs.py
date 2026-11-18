"""On-disk fetch job state (Ansible collect → tar → ingest on tool host)."""

import json
import os
import threading
import time
import uuid


JOBS_LOCK = threading.Lock()


def spool_root(cfg):
    return os.path.abspath(os.path.expanduser(cfg.get("spool_dir") or "/var/spool/aero-lens"))


def jobs_dir(cfg):
    path = os.path.join(spool_root(cfg), "jobs")
    os.makedirs(path, exist_ok=True)
    return path


def incoming_dir(cfg):
    path = os.path.join(spool_root(cfg), "incoming")
    os.makedirs(path, mode=0o755, exist_ok=True)
    return path


def job_path(cfg, job_id):
    return os.path.join(jobs_dir(cfg), "%s.json" % job_id)


def save_job(cfg, job):
    path = job_path(cfg, job["job_id"])
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(job, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)


def load_job(cfg, job_id):
    path = job_path(cfg, job_id)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def new_job(cfg, cluster_id, incident_iso, session_id):
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "session": session_id,
        "cluster": cluster_id,
        "date": incident_iso,
        "status": "queued",
        "created": time.time(),
        "updated": time.time(),
        "error": None,
        "bundle": None,
        "ansible": None,
    }
    save_job(cfg, job)
    return job


def patch_job(cfg, job_id, **fields):
    with JOBS_LOCK:
        job = load_job(cfg, job_id)
        if not job:
            return None
        job.update(fields)
        job["updated"] = time.time()
        save_job(cfg, job)
        return job