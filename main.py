from src.node import LocalEngineDataNode
from src.coordinator import DistributedTransactionCoordinator
from src.ml_allocation import perform_initial_allocation
from src.scenario import run_scenario
from telemetry import engine_events
import pandas as pd
import concurrent.futures
import argparse
import uuid

# Activate live telemetry: every WAL record, lock event and coordinator decision
# is published on BUS for the dashboard (and anything else) to read.
telemetry_bus = engine_events.activate()
BUS = engine_events.BUS


def say(text):
    print(text)
    BUS.emit({"event": "STEP", "text": text})


def run_simulation(num_users=50, workers=15, slowmo=0.0, unsafe=False):
    engine_events.SLOWMO = slowmo
    engine_events.UNSAFE = unsafe

    say("Step 1: K-Means groups items by latency and demand, one group per node")
    perform_initial_allocation()

    say("Step 2: each node loads the items assigned to it")
    inv_map = pd.read_csv("data/inventory_allocated.csv")
    nodes, topo = {}, {}
    for node_id in inv_map['node_id'].unique():
        nodes[node_id] = LocalEngineDataNode(node_id)
        topo[node_id] = {}
        for _, row in inv_map[inv_map['node_id'] == node_id].iterrows():
            stock = int(row['stock_quantity'])
            nodes[node_id].storage.data[row['item_id']] = stock
            topo[node_id][row['item_id']] = {"stock": stock,
                                             "latency": round(float(row['latency_profile']), 1),
                                             "demand": round(float(row['demand_density']), 2)}
    coordinator = DistributedTransactionCoordinator(nodes)

    HOT_ITEM = inv_map.iloc[0]['item_id']
    target_node_id = inv_map.iloc[0]['node_id']
    BUS.emit({"event": "TOPOLOGY", "nodes": topo, "hot_item": HOT_ITEM, "users": num_users,
                     "unsafe": unsafe})

    say(f"Step 3: flash sale. {num_users} users each try to buy 1 unit of {HOT_ITEM}")

    def attempt_purchase(user_id):
        txn_id = f"TXN-{uuid.uuid4().hex[:6]}"
        return f"User {user_id}: {coordinator.execute_flash_sale_transaction(txn_id, HOT_ITEM, 1)}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(attempt_purchase, i) for i in range(num_users)]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if "SUCCESS" in res:
                print(f"[SUCCESS] {res}")

    final_stock = nodes[target_node_id].storage.read_stock(HOT_ITEM)
    print(f"\nFinal Inventory for {HOT_ITEM} on {target_node_id}: {final_stock}")
    BUS.emit({"event": "SIM_END", "hot_item": HOT_ITEM, "node": target_node_id, "final": int(final_stock)})
    say("Flash sale ended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="open the live learning dashboard (scripted flash-sale scenario)")
    ap.add_argument("--classic", action="store_true", help="run the original single-item stress test in the console")
    ap.add_argument("--port", type=int, default=8000)
    a = ap.parse_args()
    if a.live:
        from telemetry import dashboard
        dashboard.serve(lambda cfg: run_scenario(float(cfg.get("tick", 0.3))), port=a.port)
    elif a.classic:
        run_simulation()
    else:
        run_scenario(0.05)