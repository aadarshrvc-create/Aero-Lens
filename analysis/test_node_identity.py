#!/usr/bin/env python3
"""Per-node IP resolution from support-bundle path labels."""
import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..", "webapp")
sys.path.insert(0, ROOT)
import server  # noqa: E402


def test_path_ip_wins_over_heartbeat_hits():
    label = "aerospike_logs_10_12_76_25_stockv2aerospike_20261114T032329/aerospike.log"
    ip_hits = {"10.12.76.250": 900, "10.12.76.25": 7}
    session = {
        "node_table": {},
        "files": {
            "a": {
                "label": label,
                "scan": {
                    "node_id": "bb906b0fc1efbe4",
                    "node_table": {},
                    "ip_hits": __import__("collections").Counter(ip_hits),
                    "build": "8.1.2.0",
                    "cluster_name": "stockv2",
                    "cluster_size": 6,
                },
                "node": {},
            }
        },
    }
    server.apply_identity(session)
    node = session["files"]["a"]["node"]
    assert node["ip"] == "10.12.76.25", node
    assert node["ip_source"] == "file path", node


if __name__ == "__main__":
    test_path_ip_wins_over_heartbeat_hits()
    print("OK test_node_identity")
