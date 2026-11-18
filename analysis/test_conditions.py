#!/usr/bin/env python3
"""Quick check that operational conditions detect proto-fd-max refusals."""
import os
import sys
import tempfile

ROOT = os.path.join(os.path.dirname(__file__), "..", "webapp")
sys.path.insert(0, ROOT)
import errors as err  # noqa: E402

SAMPLE = """
Oct 07 2026 06:32:47 GMT: INFO (config): (cfg.c:4439) proto-fd-max 15000
Oct 07 2026 06:32:47 GMT: INFO (config): (cfg.c:4471) system file descriptor limit: 100000, proto-fd-max: 15000
Oct 07 2026 08:01:15 GMT: WARNING (service): (service.c:564) refusing client connection - proto-fd-max 15000
Oct 07 2026 08:01:58 GMT: WARNING (service): (service.c:564) (repeated:5287) refusing client connection - proto-fd-max 15000
Oct 07 2026 08:02:08 GMT: INFO (info): (ticker.c:324)    fds: proto (13730,41830,28100) heartbeat (14,17,3) fabric (336,336,0)
Oct 07 2026 08:02:08 GMT: INFO (info): (ticker.c:462) {ns1} migrations: remaining (0,18,439) active (0,1,1) complete-pct 99.11
""".strip()

def main():
    path = os.path.join(tempfile.gettempdir(), "proto_fd_sample.log")
    with open(path, "w") as fh:
        fh.write(SAMPLE + "\n")
    node = {"ip": "10.0.0.1", "name": "n1"}
    binner = err.Binner(1_000_000_000, 2_000_000_000)
    state = err.new_state(binner)
    err.scan_log_errors(path, node, (None, None), binner, state)
    assert state["proto_fd_max"]["10.0.0.1"] == 15000
    assert state["os_fd_limit"]["10.0.0.1"] == 100000
    cov = (1_000_000_000, 2_000_000_000)
    report = err.analyze_errors([(path, node)], (None, None), cov)
    ids = [c["id"] for c in report["conditions"]]
    assert "proto_fd_refusal" in ids, ids
    refusal = next(c for c in report["conditions"] if c["id"] == "proto_fd_refusal")
    # 1 + 5287 + possibly grouped
    assert refusal["total"] >= 5288, refusal["total"]
    assert "migrations" in ids or report["conditions"], ids
    print("OK conditions:", ids, "refusal total", refusal["total"])

STORAGE_SAMPLE = """
Oct 19 2026 05:52:11 GMT: WARNING (storage): (storage.c:426) {mint_style} write fail: queue too deep: exceeds max 8
Oct 19 2026 05:52:12 GMT: WARNING (storage): (storage.c:426) (repeated:1) {mint_style} write fail: queue too deep: exceeds max 8
Oct 19 2026 05:57:12 GMT: WARNING (storage): (storage.c:426) (repeated:82) {mint_style} write fail: queue too deep: exceeds max 8
""".strip()


def test_storage_queue():
    path = os.path.join(tempfile.gettempdir(), "storage_queue_sample.log")
    with open(path, "w") as fh:
        fh.write(STORAGE_SAMPLE + "\n")
    node = {"ip": "10.12.74.17", "name": "n1"}
    cov = (1_000_000_000, 2_000_000_000)
    report = err.analyze_errors([(path, node)], (None, None), cov)
    ids = [c["id"] for c in report["conditions"]]
    assert "storage_queue" in ids, ids
    sq = next(c for c in report["conditions"] if c["id"] == "storage_queue")
    assert sq["total"] >= 84, sq["total"]
    ns = {n["namespace"]: n["count"] for n in sq["namespaces"]}
    assert ns.get("mint_style", 0) >= 84, ns
    titles = [e["title"] for e in report["events"]]
    assert "Device write queue full (AS_ERR_DEVICE_OVERLOAD 18)" in titles, titles
    assert any("queue too deep" in h.lower() or "device overload" in h.lower()
               for h in report["headline"]), report["headline"]
    print("OK storage_queue:", sq["total"], "headline", report["headline"][0][:80])


if __name__ == "__main__":
    main()
    test_storage_queue()
