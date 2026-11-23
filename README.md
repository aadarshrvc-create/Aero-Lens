# Aero Lens

Internal web UI for Aerospike log latency and error analysis (asloglatency / `log_latency.py`).

## Quick start (tool host)

```bash
python3 webapp/server.py --host 0.0.0.0 --port 8787
```

Copy `webapp/fetch_clusters.example.json` → `webapp/fetch_clusters.json` for Ansible-based log fetch.

## Server runbook

### Run continuously with systemd

Running the quick-start command in an SSH terminal ties the process to that session.
If it stops after logout, the reverse proxy can return 502 or 504 errors. Use a
systemd service to keep Aero Lens running after logout, restart it after a crash,
and start it automatically after a server reboot.

The following commands run as root on the current tool host. They assume the
repository is installed at `/myntra/Aerospike/Aerospike_latency_tool`, Python is at
`/usr/bin/python3`, and the backend listens on port `8787`. Adjust these values for
another installation.

```bash
cat > /etc/systemd/system/aero-lens.service <<'EOF'
[Unit]
Description=Aero Lens
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/myntra/Aerospike/Aerospike_latency_tool
ExecStart=/usr/bin/python3 -u /myntra/Aerospike/Aerospike_latency_tool/webapp/server.py --host 0.0.0.0 --port 8787
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

Preserve any options from the existing launch command in `ExecStart`, such as
`--fetch-config`, `--reports-dir`, `--ingest-dir`, `--ingest-token`, or `--tool`.
The service user must have the same file access and SSH credentials used by the
working manual process, including access to the Ansible controller. The example
uses root to match the current manual launch. If using another service user,
preserve the report and ingest paths explicitly so existing data remains visible.

Stop the manually running copy with **Ctrl+C** in its terminal before starting the
service, so both processes do not compete for port `8787`. Then run:

```bash
systemctl daemon-reload
systemctl enable --now aero-lens
systemctl status aero-lens --no-pager
curl --fail --show-error http://127.0.0.1:8787/api/health
```

Confirm the status is `active (running)` and the health endpoint responds. The SSH
session can then be closed.

### View logs and manage the service

```bash
# Recent logs
journalctl -u aero-lens -n 100 --no-pager

# Follow logs continuously; Ctrl+C only stops following the log
journalctl -u aero-lens -f

# Restart or stop the application
systemctl restart aero-lens
systemctl stop aero-lens
```

After editing the service file, run `systemctl daemon-reload` and
`systemctl restart aero-lens` to apply its changes.

### Update the application

```bash
cd /myntra/Aerospike/Aerospike_latency_tool
git pull origin main
systemctl restart aero-lens
systemctl status aero-lens --no-pager
curl --fail --show-error http://127.0.0.1:8787/api/health
```

Python changes require a restart. Changes only to `webapp/static/` are served on
the next request and need a browser hard refresh, without a server restart.
Restarting loses active analysis sessions; save completed reports first. Saved
reports and forwarded logs remain in their configured storage directories.

If Git reports local changes that would be overwritten, inspect them with
`git status --short` and `git diff`. Commit or stash those changes before pulling.

### Troubleshoot 502 or 504 errors

Check `systemctl status aero-lens --no-pager`, the journal, and the local health
endpoint above. If the journal reports that the address is already in use, run
`ss -ltnp 'sport = :8787'` to identify the process occupying the port and stop the
old manual instance before restarting the service.

If local health succeeds but the public URL still returns an error, check the
reverse proxy upstream address and its logs. A 504 occurring only during a long
fetch or analysis may be a proxy timeout; keeping the service running addresses
logout-related failures but does not change proxy timeout settings.

## Ansible fetch (DB nodes reachable only from Ansible)

See [ansible/README.md](ansible/README.md).

## Layout

- `webapp/` — HTTP UI and APIs
- `asloglatency/` — `log_latency.py` histogram parser
- `forwarder/` — optional live log forwarder
- `ansible/` — collect logs and ship tarball to tool host
