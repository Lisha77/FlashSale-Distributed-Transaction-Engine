from src.node import LocalEngineDataNode
from src.coordinator import DistributedTransactionCoordinator
import os
import json

def test_recovery():
    print("=== SIMULATING CRASH AND RECOVERY ===")
    
    node_id = "Node-Test-Recovery"
    # Clean up old logs if any
    log_path = f"logs/node_{node_id}.log"
    if os.path.exists(log_path): os.remove(log_path)
    if os.path.exists("logs/coordinator.log"): os.remove("logs/coordinator.log")

    # 1. Setup Node and Coordinator
    node = LocalEngineDataNode(node_id)
    node.storage.data["LAPTOP"] = 10
    coordinator = DistributedTransactionCoordinator({node_id: node})

    # 2. Start a transaction
    txn_id = "TXN-CRASH-101"
    print(f"\n[Scenario] User trying to buy LAPTOP. Transaction {txn_id} starts...")
    
    # Manually execute prepare
    vote = node.prepare_transaction(txn_id, "LAPTOP", 1)
    print(f"[Node] Voted: {vote}")

    # Coordinator decides to COMMIT and writes to its own WAL
    coordinator.wal_logger.write_record(txn_id, 0, "COMMIT")
    print("[Coordinator] Wrote COMMIT to global log. CRASHING NODE NOW!")

    # 3. CRASH SIMULATION
    # We delete the 'node' object. It didn't get to run 'commit_transaction'
    del node 

    print("\n--- NODE REBOOTING ---")
    
    # 4. RECOVERY
    new_node = LocalEngineDataNode(node_id)
    # Restore the "Base" state (like a Checkpoint)
    base_state = {"LAPTOP": 10}
    
    print(f"[Pre-Recovery] Stock: {new_node.storage.data.get('LAPTOP', 'MISSING')}")
    
    new_node.recover(coordinator_wal_path="logs/coordinator.log", checkpoint_data=base_state)
    
    print(f"\n[Post-Recovery] Stock of LAPTOP: {new_node.storage.read_stock('LAPTOP')}")
    if new_node.storage.read_stock('LAPTOP') == 9:
        print("[SUCCESS] Recovery restored the transaction that was in-flight during crash.")
    else:
        print("[FAILURE] Recovery did not restore the state.")

if __name__ == "__main__":
    test_recovery()
