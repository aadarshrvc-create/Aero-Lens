#!/usr/bin/env python3
"""Ingest store + analyze-from-slice smoke test."""
import os
import sys
import tempfile

ROOT = os.path.join(os.path.dirname(__file__), "..", "webapp")
sys.path.insert(0, ROOT)
import ingest  # noqa: E402
import errors as err  # noqa: E402

SAMPLE = """
Oct 19 2026 05:52:11 GMT: WARNING (storage): (storage.c:426) {mint_style} write fail: queue too deep: exceeds max 8
Oct 19 2026 05:52:12 GMT: WARNING (storage): (storage.c:426) (repeated:3) {mint_style} write fail: queue too deep: exceeds max 8
""".strip()


def main():
    store = tempfile.mkdtemp(prefix="ingest-test-")
    ingest.append_batch(store, "test-cluster", "10.0.0.1", SAMPLE.splitlines(), hostname="n1")
    inv = ingest.inventory(store)
    assert inv["clusters"][0]["node_count"] == 1
    path, hint = ingest.materialize_slice(store, "test-cluster", "10.0.0.1", None, None)
    try:
        node = hint["node"]
        report = err.analyze_errors([(path, node)], (None, None), (1, 2_000_000_000))
        ids = [c["id"] for c in report["conditions"]]
        assert "storage_queue" in ids, ids
    finally:
        os.unlink(path)
    print("OK ingest store", store)


if __name__ == "__main__":
    main()
