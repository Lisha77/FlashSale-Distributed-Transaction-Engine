import threading
import time

class Strict2PLManager:
    def __init__(self):
        self.locks = {}  # resource_id -> txn_id (Exclusive locks only for this sim)
        self.lock_wait_queue = {} # resource_id -> list of txn_ids
        self.txn_locks = {} # txn_id -> list of resource_ids
        self._lock = threading.Lock()

    def acquire_exclusive_lock(self, txn_id, resource_id, timeout=2.0):
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._lock:
                if resource_id not in self.locks:
                    # Grant lock
                    self.locks[resource_id] = txn_id
                    if txn_id not in self.txn_locks:
                        self.txn_locks[txn_id] = []
                    self.txn_locks[txn_id].append(resource_id)
                    print(f"[LockManager] Txn {txn_id} acquired X-lock on {resource_id}")
                    return True
                elif self.locks[resource_id] == txn_id:
                    # Already holds lock
                    return True
            
            # Wait a bit before retrying (simple busy wait for simulation)
            time.sleep(0.1)
        
        print(f"[LockManager] Txn {txn_id} FAILED to acquire lock on {resource_id} (Timeout)")
        return False

    def release_locks(self, txn_id):
        with self._lock:
            if txn_id in self.txn_locks:
                resources = self.txn_locks[txn_id]
                for res in resources:
                    if self.locks.get(res) == txn_id:
                        del self.locks[res]
                del self.txn_locks[txn_id]
                print(f"[LockManager] Txn {txn_id} released all locks")
