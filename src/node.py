from .storage import WriteAheadLogger, StorageEngine
from .concurrency import Strict2PLManager

class LocalEngineDataNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock_table = Strict2PLManager()
        self.wal = WriteAheadLogger(file_path=f"logs/node_{node_id}.log")
        self.storage = StorageEngine(node_id)
        
    def prepare_transaction(self, txn_id, item_id, quantity):
        print(f"[Node-{self.node_id}] Preparing Txn {txn_id}...")
        
        # 1. Acquire S2PL Lock
        if not self.lock_table.acquire_exclusive_lock(txn_id, resource_id=item_id):
            return "VOTE_ABORT"

        # 2. Check Inventory
        current_stock = self.storage.read_stock(item_id)
        if current_stock < quantity:
            print(f"[Node-{self.node_id}] Insufficient stock for {item_id}")
            self.lock_table.release_locks(txn_id)
            return "VOTE_ABORT"

        # 3. Log to WAL (Undo/Redo)
        self.wal.write_record(
            txn_id=txn_id,
            prev_lsn=0, 
            op_type="PREPARE_UPDATE",
            page_id=item_id,
            undo_val=current_stock,
            redo_val=current_stock - quantity
        )
        self.wal.flush()
        
        # In a real system, we'd store the pending update in a buffer
        self.pending_update = (item_id, current_stock - quantity)
        
        return "VOTE_COMMIT"

    def commit_transaction(self, txn_id):
        print(f"[Node-{self.node_id}] Committing Txn {txn_id}...")
        item_id, new_stock = self.pending_update
        
        # Write Commit record
        self.wal.write_record(txn_id=txn_id, prev_lsn=0, op_type="COMMIT")
        
        # Apply to storage
        self.storage.apply_changes(item_id, new_stock)
        
        # Release locks (Strict 2PL)
        self.lock_table.release_locks(txn_id)

    def abort_transaction(self, txn_id):
        print(f"[Node-{self.node_id}] Aborting Txn {txn_id}...")
        self.wal.write_record(txn_id=txn_id, prev_lsn=0, op_type="ABORT")
        self.lock_table.release_locks(txn_id)

    def recover(self, coordinator_wal_path="logs/coordinator.log", checkpoint_data=None):
        """
        Unit 2.3: ARIES Recovery Implementation
        Analysis -> REDO -> UNDO
        """
        print(f"\n[Node-{self.node_id}] Initiating ARIES Recovery...")
        import os
        import json
        
        # In a real system, we'd load the last checkpoint.
        # Here we manually pass it for the simulation to restore base state.
        if checkpoint_data:
            self.storage.data.update(checkpoint_data)

        # 1. Analysis Phase
        records = self.wal.get_all_records()
        if not records:
            print(f"[Node-{self.node_id}] No WAL records found. Recovery complete.")
            return

        transaction_table = {} # txn_id -> state (PREPARED, COMMITTED, ABORTED)
        
        # Analysis: Scan WAL forward to build transaction state
        for rec in records:
            tid = rec['txn_id']
            rtype = rec['type']
            
            if rtype == "PREPARE_UPDATE":
                transaction_table[tid] = "PREPARED"
            elif rtype == "COMMIT":
                transaction_table[tid] = "COMMITTED"
            elif rtype == "ABORT":
                transaction_table[tid] = "ABORTED"

        # 2. Handle In-Flight Transactions (Those in PREPARED state)
        if os.path.exists(coordinator_wal_path):
            with open(coordinator_wal_path, "r") as f:
                c_logs = [json.loads(line) for line in f.readlines()]
                c_decisions = {l['txn_id']: l['type'] for l in c_logs if l['type'] in ["COMMIT", "ABORT"]}
        else:
            c_decisions = {}

        for tid, state in transaction_table.items():
            if state == "PREPARED":
                decision = c_decisions.get(tid, "ABORT") # Default to abort if decision not found
                print(f"[Node-{self.node_id}] Txn {tid} was PREPARED. Coordinator decision: {decision}")
                # Normalize coordinator decision ("COMMIT"/"ABORT") to internal txn states
                transaction_table[tid] = "COMMITTED" if decision == "COMMIT" else "ABORTED"

        # 3. REDO Phase: Replay all COMMITTED transactions
        for rec in records:
            tid = rec['txn_id']
            if transaction_table.get(tid) == "COMMITTED" and rec['type'] == "PREPARE_UPDATE":
                item_id = rec['page_id']
                new_val = rec['redo']
                self.storage.apply_changes(item_id, new_val)
                print(f"[REDO] Txn {tid}: Restored {item_id} to {new_val}")

        # 4. UNDO Phase
        for rec in reversed(records):
            tid = rec['txn_id']
            if transaction_table.get(tid) == "ABORTED" and rec['type'] == "PREPARE_UPDATE":
                 print(f"[UNDO] Txn {tid}: Rolling back change to {rec['page_id']}")

        print(f"[Node-{self.node_id}] Recovery Complete. Current Data: {self.storage.data}")
