# Aero Lens

Internal web UI for Aerospike log latency and error analysis (asloglatency / `log_latency.py`).

## Quick start (tool host)

```bash
python3 webapp/server.py --host 0.0.0.0 --port 8787
```

Copy `webapp/fetch_clusters.example.json` → `webapp/fetch_clusters.json` for Ansible-based log fetch.

## Ansible fetch (DB nodes reachable only from Ansible)

See [ansible/README.md](ansible/README.md).

## Layout

- `webapp/` — HTTP UI and APIs
- `asloglatency/` — `log_latency.py` histogram parser
- `forwarder/` — optional live log forwarder
- `ansible/` — collect logs and ship tarball to tool host