from .storage import WriteAheadLogger
from .router import GlobalQueryRouter

class DistributedTransactionCoordinator:
    def __init__(self, nodes_dict):
        self.nodes = nodes_dict # Map of node_id -> node_instance
        self.wal_logger = WriteAheadLogger(file_path="logs/coordinator.log")
        self.router = GlobalQueryRouter()

    def execute_flash_sale_transaction(self, txn_id, item_id, quantity):
        print(f"\n[Coordinator] Processing Global Request {txn_id} for {item_id}...")
        
        # 1. Query Localization (Unit 2.1)
        target_node_id = self.router.localize_query(item_id)
        if not target_node_id:
            return "TRANSACTION_ERROR_INVALID_ITEM"
        
        target_node = self.nodes.get(target_node_id)
        if not target_node:
            return "TRANSACTION_ERROR_NODE_OFFLINE"

        self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="BEGIN")

        # Phase 1: Prepare
        print(f"[Coordinator] Phase 1: Sending PREPARE to {target_node_id}")
        try:
            vote = target_node.prepare_transaction(txn_id, item_id, quantity)
        except Exception as e:
            print(f"[Coordinator] Timeout/Error from node {target_node_id}: {e}")
            vote = "VOTE_ABORT"

        # Phase 2: Decision
        if vote == "VOTE_COMMIT":
            print(f"[Coordinator] Phase 2: Sending GLOBAL_COMMIT to {target_node_id}")
            self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="COMMIT")
            target_node.commit_transaction(txn_id)
            return "TRANSACTION_SUCCESS"
        else:
            print(f"[Coordinator] Phase 2: Sending GLOBAL_ABORT to {target_node_id}")
            self.wal_logger.write_record(txn_id, prev_lsn=0, op_type="ABORT")
            target_node.abort_transaction(txn_id)
            return "TRANSACTION_ABORTED"
