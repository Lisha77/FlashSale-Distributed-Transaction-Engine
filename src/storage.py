import json
import os
import time

class WriteAheadLogger:
    def __init__(self, file_path):
        self.file_path = file_path
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.lsn_counter = 0

    def write_record(self, txn_id, prev_lsn, op_type, page_id=None, undo_val=None, redo_val=None, status=None):
        self.lsn_counter += 1
        record = {
            "lsn": self.lsn_counter,
            "prev_lsn": prev_lsn,
            "txn_id": txn_id,
            "type": op_type,
            "page_id": page_id if page_id is not None else "GLOBAL",
            "undo": undo_val if undo_val is not None else "-",
            "redo": redo_val if redo_val is not None else "-",
            "status": status if status is not None else "DURING_COMMIT",
            "timestamp": time.time()
        }
        with open(self.file_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        return self.lsn_counter

    def flush(self):
        # In a real OS, this would involve fsync
        pass

    def get_all_records(self):
        if not os.path.exists(self.file_path):
            return []
        with open(self.file_path, "r") as f:
            return [json.loads(line) for line in f.readlines()]

class StorageEngine:
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = {} # Simple in-memory KV for simulation
    
    def read_stock(self, item_id):
        return self.data.get(item_id, 0)
    
    def apply_changes(self, item_id, new_quantity):
        self.data[item_id] = new_quantity
        print(f"[Node-{self.node_id}] Applied update to {item_id}: New Stock = {new_quantity}")
