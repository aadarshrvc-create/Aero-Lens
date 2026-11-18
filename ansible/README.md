# Aero Lens — Ansible log collect

Aero Lens (tool VM **115**) cannot SSH to DB nodes. The **Ansible controller** collects logs and ships a tarball back.

## Install on `dbeansible0`

1. Copy this `ansible/` directory under your automation tree (e.g. `/home/dbops/dbeautomation/aerospike_ansible/aero_lens/`).
2. `chmod +x run_aero_lens_fetch.sh`
3. Set **`AERO_LENS_INVENTORY`** to your inventory path if not `inventory/production` beside the playbook.
4. Ensure cluster groups exist (e.g. `aerospike_upgradetest_az_prod_ci` → `.179/.180/.181`).

## Tool host (115)

1. `sudo mkdir -p /var/spool/aero-lens/incoming /var/spool/aero-lens/jobs`
2. `sudo chown -R dbops:dbops /var/spool/aero-lens`
3. `cp webapp/fetch_clusters.example.json webapp/fetch_clusters.json` and set:
   - `fetch_mode`: `ansible`
   - `ansible.host`: Ansible controller hostname/IP
   - `ansible.remote_script`: full path to `run_aero_lens_fetch.sh`
   - `delivery.tool_host`: `10.12.72.115`
   - `delivery.callback_url`: `http://10.12.72.115:8787/api/fetch/complete`
   - `fetch_token`: shared secret (optional but recommended)
4. **SSH key**: user running `server.py` (`dbops`) must `ssh dbops@dbeansible0` without password (`BatchMode=yes`).

## Flow

1. UI/API `POST /api/fetch` on 115 → SSH starts `run_aero_lens_fetch.sh` on Ansible.
2. Playbook copies `aerospike.log-*.gz` from each host → tarball.
3. `scp` tarball to `/var/spool/aero-lens/incoming/<job_id>.tar.gz` on 115.
4. Script `POST /api/fetch/complete` → tool ingests bundle into session.
5. User **Run** analyze as usual.

## Manual test (Ansible host)

```bash
./run_aero_lens_fetch.sh '{"job_id":"manual1","cluster":"az-prod-ci-upgrade","ansible_group":"YOUR_GROUP","date":"2026-11-15","log_dir":"/var/log/aerospike","log_basenames":["aerospike.log-20261116.gz"],"delivery":{"tool_host":"10.12.72.115","tool_user":"dbops","incoming_dir":"/var/spool/aero-lens/incoming","bundle_name":"manual1.tar.gz","callback_url":"http://10.12.72.115:8787/api/fetch/complete"},"fetch_token":""}'
```