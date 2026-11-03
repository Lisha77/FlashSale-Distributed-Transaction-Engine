"""
WAL Telemetry Extraction
------------------------
Parses Write-Ahead Log files produced by the Distributed Flash Sale Engine and
summarizes transaction outcomes and prepare->commit latency for the
ADBMS project report's experimental evaluation section.
"""
import json
import time
import collections
import os

# Anchor log paths to the project root (flash_sale_engine/) regardless of the
# directory this script is invoked from.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def summarize_log(path, window_sec=7200):
    """Summarize WAL records written within the last `window_sec` seconds."""
    if not os.path.exists(path):
        return None

    with open(path, "r", encoding="utf-8") as f:
        recs = [json.loads(line) for line in f if line.strip()]

    now = time.time()
    recent = [r for r in recs if now - r.get("timestamp", 0) < window_sec]
    if not recent:
        return None

    counts = collections.Counter(r["type"] for r in recent)
    prepare_ts = {r["txn_id"]: r["timestamp"] for r in recent if r["type"] == "PREPARE_UPDATE"}
    latencies = sorted(
        r["timestamp"] - prepare_ts[r["txn_id"]]
        for r in recent
        if r["type"] == "COMMIT" and r["txn_id"] in prepare_ts
    )

    summary = {
        "log_file": path,
        "records": len(recent),
        "types": dict(counts),
        "unique_txns": len(set(r["txn_id"] for r in recent)),
        "committed": counts.get("COMMIT", 0),
        "aborted": counts.get("ABORT", 0),
        "avg_prepare_to_commit_ms": round(1000 * sum(latencies) / len(latencies), 2) if latencies else None,
        "p50_prepare_to_commit_ms": round(1000 * latencies[len(latencies) // 2], 2) if latencies else None,
        "max_prepare_to_commit_ms": round(1000 * max(latencies), 2) if latencies else None,
    }
    return summary


if __name__ == "__main__":
    target = os.path.join(BASE_DIR, "logs", "node_Node-1.log")
    result = summarize_log(target)
    if result is None:
        print(f"No recent WAL records found in {target}. Run main.py first.")
    else:
        print("=== WAL TELEMETRY (last 2 hours) ===")
        for key, value in result.items():
            print(f"{key}: {value}")
