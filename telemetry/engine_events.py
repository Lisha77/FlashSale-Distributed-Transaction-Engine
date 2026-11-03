"""
telemetry/engine_events.py
Wires the telemetry bus into the engine WITHOUT touching core logic.
Every WAL write, lock change, 2PC message and stock update is published as it happens.

Events: STEP, TOPOLOGY, localize, BEGIN, PREPARE_SENT, WAL, LOCK_WAIT, ACQUIRE_X,
        LOCK_TIMEOUT, VOTE_RECEIVED, DECISION_COMMIT, DECISION_ABORT, APPLY,
        RELEASE_X, RELEASE_ALL, SIM_END
"""
import os
import threading
import time
from .bus import TelemetryBus

BUS = TelemetryBus("FlashSaleEngine")
UNSAFE = False                # demo switch: skip locking to show lost updates
SLOWMO = 0.0                  # seconds of pause between protocol steps (set by main/dashboard)
_WAL_LOCK = threading.Lock()  # makes "assign LSN + write + publish" atomic


def _pause(factor=1.0):
    d = SLOWMO * factor
    if UNSAFE:
        d = max(d, 0.05)   # widen the read-to-write window so the race is visible
    if d > 0:
        time.sleep(d)


def _log_name(path):
    base = os.path.basename(path)[:-4]
    return "coordinator" if base == "coordinator" else base.replace("node_", "", 1)


def _patch_storage(module):
    orig_write = module.WriteAheadLogger.write_record
    orig_apply = module.StorageEngine.apply_changes

    def write_record(self, txn_id, prev_lsn, op_type, page_id=None,
                     undo_val=None, redo_val=None, status=None):
        # Lock fixes a race in the original: lsn_counter += 1 is not atomic across threads.
        with _WAL_LOCK:
            lsn = orig_write(self, txn_id, prev_lsn, op_type, page_id, undo_val, redo_val, status)
            BUS.emit({"event": "WAL", "log": _log_name(self.file_path), "lsn": lsn,
                      "prev_lsn": prev_lsn, "txn_id": txn_id, "op": op_type,
                      "page_id": page_id, "undo": undo_val, "redo": redo_val,
                      "timestamp": time.time()})
        return lsn

    def apply_changes(self, item_id, new_quantity):
        old = self.data.get(item_id)
        orig_apply(self, item_id, new_quantity)
        BUS.emit({"event": "APPLY", "node": self.node_id, "item_id": item_id,
                  "old": old, "new": new_quantity, "timestamp": time.time()})

    module.WriteAheadLogger.write_record = write_record
    module.StorageEngine.apply_changes = apply_changes


def _patch_concurrency(module):
    def acquire(self, txn_id, resource_id, timeout=2.0):
        if UNSAFE:
            BUS.emit({"event": "LOCK_SKIPPED", "txn_id": txn_id, "resource_id": resource_id,
                      "timestamp": time.time()})
            return True
        timeout = timeout + SLOWMO * 40   # slow-mo makes lock holders slower; give waiters proportional patience
        start = time.time()
        announced = False
        while time.time() - start < timeout:
            with self._lock:
                holder = self.locks.get(resource_id)
                if holder is None:
                    self.locks[resource_id] = txn_id
                    self.txn_locks.setdefault(txn_id, []).append(resource_id)
                    print(f"[LockManager] Txn {txn_id} acquired X-lock on {resource_id}")
                    BUS.emit({"event": "ACQUIRE_X", "txn_id": txn_id, "resource_id": resource_id,
                              "waited": round(time.time() - start, 2), "timestamp": time.time()})
                    return True
                if holder == txn_id:
                    return True
                if not announced:
                    announced = True
                    BUS.emit({"event": "LOCK_WAIT", "txn_id": txn_id, "resource_id": resource_id,
                              "holder": holder, "timestamp": time.time()})
            time.sleep(0.1)
        BUS.emit({"event": "LOCK_TIMEOUT", "txn_id": txn_id, "resource_id": resource_id,
                  "timestamp": time.time()})
        print(f"[LockManager] Txn {txn_id} FAILED to acquire lock on {resource_id} (Timeout)")
        return False

    def release(self, txn_id):
        with self._lock:
            if txn_id in self.txn_locks:
                resources = self.txn_locks.pop(txn_id)
                for res in resources:
                    if self.locks.get(res) == txn_id:
                        del self.locks[res]
                        BUS.emit({"event": "RELEASE_X", "txn_id": txn_id, "resource_id": res,
                                  "timestamp": time.time()})
                BUS.emit({"event": "RELEASE_ALL", "txn_id": txn_id, "resources": resources,
                          "timestamp": time.time()})
                print(f"[LockManager] Txn {txn_id} released all locks")

    module.Strict2PLManager.acquire_exclusive_lock = acquire
    module.Strict2PLManager.release_locks = release


def _patch_coordinator(module):
    def execute(self, txn_id, item_id, quantity):
        print(f"\n[Coordinator] Processing Global Request {txn_id} for {item_id}...")
        node_id = self.router.localize_query(item_id)
        BUS.emit({"event": "localize", "txn_id": txn_id, "item_id": item_id,
                  "target_node": node_id, "quantity": quantity, "timestamp": time.time()})
        if not node_id:
            return "TRANSACTION_ERROR_INVALID_ITEM"
        node = self.nodes.get(node_id)
        if not node:
            return "TRANSACTION_ERROR_NODE_OFFLINE"
        _pause()

        self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="BEGIN")
        BUS.emit({"event": "BEGIN", "txn_id": txn_id, "node": node_id, "timestamp": time.time()})
        _pause()

        print(f"[Coordinator] Phase 1: Sending PREPARE to {node_id}")
        BUS.emit({"event": "PREPARE_SENT", "txn_id": txn_id, "node": node_id, "timestamp": time.time()})
        try:
            vote = node.prepare_transaction(txn_id, item_id, quantity)
        except Exception as e:
            print(f"[Coordinator] Timeout/Error from node {node_id}: {e}")
            vote = "VOTE_ABORT"
        BUS.emit({"event": "VOTE_RECEIVED", "txn_id": txn_id, "node": node_id,
                  "vote": vote, "timestamp": time.time()})
        _pause(2)   # lock (if granted) is still held here, so others visibly queue up

        if vote == "VOTE_COMMIT":
            print(f"[Coordinator] Phase 2: Sending GLOBAL_COMMIT to {node_id}")
            self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="COMMIT")   # the commit point
            BUS.emit({"event": "DECISION_COMMIT", "txn_id": txn_id, "node": node_id,
                      "timestamp": time.time()})
            _pause()
            node.commit_transaction(txn_id)
            return "TRANSACTION_SUCCESS"
        print(f"[Coordinator] Phase 2: Sending GLOBAL_ABORT to {node_id}")
        self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="ABORT")
        BUS.emit({"event": "DECISION_ABORT", "txn_id": txn_id, "node": node_id,
                  "timestamp": time.time()})
        _pause()
        node.abort_transaction(txn_id)
        return "TRANSACTION_ABORTED"

    module.DistributedTransactionCoordinator.execute_flash_sale_transaction = execute


def activate():
    import src.storage as storage
    import src.concurrency as concurrency
    import src.coordinator as coordinator
    _patch_storage(storage)
    _patch_concurrency(concurrency)
    _patch_coordinator(coordinator)
    return BUS