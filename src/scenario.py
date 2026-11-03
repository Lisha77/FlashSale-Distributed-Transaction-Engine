"""
src/scenario.py
A scripted, teachable flash sale: ~22 orders from customers in different regions,
multi-item carts, limited stock, and deliberately injected failures. Every step is
published on the telemetry bus so the dashboard can show what the engine is doing.

Teaching model (kept deliberately simple):
  * A node applies a change to stock when it PREPAREs (after writing undo/redo to its WAL),
    protected by an exclusive lock. COMMIT makes it permanent, ABORT undoes it.
  * Only the WAL is durable. A node crash wipes memory and rolls the data back to the last
    checkpoint (the initial stock), so recovery has to REDO committed work from the log.
"""
import glob
import os
import random
import threading
import time
import traceback

import pandas as pd

from telemetry import engine_events
from telemetry.engine_events import BUS
from .concurrency import Strict2PLManager
from .ml_allocation import perform_initial_allocation
from .node import LocalEngineDataNode
from .router import GlobalQueryRouter
from .storage import WriteAheadLogger

LABELS = ["Sneakers", "Headphones", "Smart Watch", "Backpack", "Camera", "Game Console"]
STOCK = [7, 1, 6, 3, 5, 6]
LATENCY = {"US-East": 0.04, "EU-Central": 0.09, "Asia-Pacific": 0.16}  # seconds, one way


def emit(event, **kw):
    kw["event"] = event
    kw["timestamp"] = time.time()
    BUS.emit(kw)


class NodeDown(Exception):
    pass


class Run:
    """Shared pacing so a human can follow the protocol."""
    def __init__(self, tick):
        self.tick = tick
        self.lock_timeout = max(1.2, 12 * tick)

    def pace(self, f=1.0):
        time.sleep(self.tick * f)


# --------------------------------------------------------------------------- node
class ScenarioNode(LocalEngineDataNode):
    def __init__(self, node_id, run):
        super().__init__(node_id)
        self.run = run
        self.down = False
        self.checkpoint = {}
        self.pending = {}   # txn -> [(item, undo, redo)] prepared but undecided

    def _set(self, item, new, why, txn):
        old = self.storage.data.get(item)
        self.storage.data[item] = new
        emit("STOCK", node=self.node_id, item_id=item, old=old, new=new, why=why, txn_id=txn)

    def _rollback(self, txn, done):
        for item, undo, redo in reversed(done):
            self._set(item, undo, "undo", txn)
        if done:
            self.wal.write_record(txn, 0, "ABORT")
        self.lock_table.release_locks(txn)

    def prepare(self, txn, items, patience=1.0):
        """items: [(item, qty)] living on this node. Returns (vote, reason, detail)."""
        done = []
        for item, qty in items:
            if self.down:
                raise NodeDown(self.node_id)
            if not self.lock_table.acquire_exclusive_lock(txn, item, timeout=self.run.lock_timeout * patience):
                self._rollback(txn, done)
                return "VOTE_ABORT", "LOCK_TIMEOUT", {"item": item}
            have = self.storage.read_stock(item)
            ok = have >= qty
            emit("NODE_CHECK", txn_id=txn, node=self.node_id, item_id=item, have=have, want=qty, ok=ok)
            self.run.pace(0.5)
            if not ok:
                self._rollback(txn, done)
                return "VOTE_ABORT", "INSUFFICIENT_STOCK", {"item": item, "have": have, "want": qty}
            self.wal.write_record(txn, 0, "PREPARE_UPDATE", item, int(have), int(have - qty))  # write-ahead
            self.wal.flush()
            self._set(item, have - qty, "reserve", txn)
            done.append((item, have, have - qty))
            self.run.pace(0.5)
        self.pending[txn] = done
        return "VOTE_COMMIT", None, {}

    def commit(self, txn):
        if self.down:
            raise NodeDown(self.node_id)
        self.wal.write_record(txn, 0, "COMMIT")
        recs = self.pending.pop(txn, [])
        self.lock_table.release_locks(txn)       # strict 2PL: locks only go now
        emit("NODE_COMMITTED", txn_id=txn, node=self.node_id,
             items=[{"item_id": i, "qty": int(u - r)} for i, u, r in recs])

    def abort(self, txn):
        if self.down:
            raise NodeDown(self.node_id)
        recs = self.pending.pop(txn, [])
        self._rollback(txn, recs)
        emit("NODE_ABORTED", txn_id=txn, node=self.node_id,
             items=[{"item_id": i, "qty": int(u - r)} for i, u, r in recs])

    def crash(self):
        self.down = True
        self.pending.clear()
        self.lock_table = Strict2PLManager()          # the lock table lived in memory
        self.storage.data = dict(self.checkpoint)     # unflushed pages are gone
        emit("NODE_CRASH", node=self.node_id, reverted=dict(self.checkpoint))

    def recover(self, coord_records):
        """Analysis -> REDO -> UNDO, resolving in-doubt transactions from the coordinator log."""
        run = self.run
        emit("NODE_RECOVERY_START", node=self.node_id)
        run.pace()
        recs = self.wal.get_all_records()
        state, decisions = {}, {}
        for r in coord_records:
            if r["type"] in ("COMMIT", "ABORT"):
                decisions[r["txn_id"]] = r["type"]
        for r in recs:
            t = r["txn_id"]
            if r["type"] == "PREPARE_UPDATE":
                state.setdefault(t, "PREPARED")
            elif r["type"] in ("COMMIT", "ABORT") and t in state:
                state[t] = "COMMITTED" if r["type"] == "COMMIT" else "ABORTED"
        rows, resolved = [], []
        for t, s in state.items():
            if s == "PREPARED":
                d = decisions.get(t)
                final = "COMMITTED" if d == "COMMIT" else "ABORTED"
                resolved.append((t, final))
                rows.append({"txn_id": t, "state": "PREPARED (in doubt)", "coordinator": d or "no decision",
                             "resolution": final})
            else:
                rows.append({"txn_id": t, "state": s, "coordinator": decisions.get(t, "-"), "resolution": s})
        emit("RECOVERY", node=self.node_id, phase="analysis", rows=rows)
        run.pace()
        final_state = dict(state)
        for t, f in resolved:
            final_state[t] = f
            self.wal.write_record(t, 0, "COMMIT" if f == "COMMITTED" else "ABORT")
        for r in recs:                                              # REDO, in log order
            if r["type"] == "PREPARE_UPDATE" and final_state.get(r["txn_id"]) == "COMMITTED":
                self._set(r["page_id"], r["redo"], "redo", r["txn_id"])
                emit("RECOVERY", node=self.node_id, phase="redo", txn_id=r["txn_id"], item_id=r["page_id"],
                     undo=r["undo"], redo=r["redo"], lsn=r["lsn"])
                run.pace(0.6)
        for t, f in resolved:
            items = [{"item_id": r["page_id"], "qty": int(r["undo"] - r["redo"])}
                     for r in recs if r["txn_id"] == t and r["type"] == "PREPARE_UPDATE"]
            emit("NODE_COMMITTED" if f == "COMMITTED" else "NODE_ABORTED", txn_id=t, node=self.node_id,
                 items=items, recovered=True)
        for r in reversed(recs):                                    # UNDO losers
            if r["type"] == "PREPARE_UPDATE" and final_state.get(r["txn_id"]) == "ABORTED":
                indoubt = any(t == r["txn_id"] for t, _ in resolved)
                emit("RECOVERY", node=self.node_id, phase="undo", txn_id=r["txn_id"], item_id=r["page_id"],
                     undo=r["undo"], redo=r["redo"],
                     note=("its change only lived in memory, which the crash wiped, so nothing needs restoring"
                           if indoubt else "already rolled back before the crash"))
                run.pace(0.6)
        self.down = False
        emit("NODE_UP", node=self.node_id)
        return resolved


# --------------------------------------------------------------------------- coordinator
class Coordinator:
    def __init__(self, nodes, run, labels):
        self.nodes, self.run, self.labels = nodes, run, labels
        self.router = GlobalQueryRouter()
        self.wal = WriteAheadLogger("logs/coordinator.log")
        self.orders, self.in_doubt, self.sold = {}, {}, {}

    def receipt(self, txn, status, message):
        o = self.orders[txn]
        emit("RECEIPT", txn_id=txn, customer=o["customer"], region=o["region"], status=status, message=message)

    def _fail_receipt(self, txn, reason, d):
        lab = self.labels.get(d.get("item"), "this item")
        if reason == "INSUFFICIENT_STOCK" and d["have"] == 0:
            self.receipt(txn, "soldout", f"{lab} is sold out. Nothing was charged.")
        elif reason == "INSUFFICIENT_STOCK":
            self.receipt(txn, "partial", f"Only {d['have']} left of {lab} but you asked for {d['want']}. "
                                         f"Nothing in this order was placed or charged. We never split an order.")
        elif reason == "LOCK_TIMEOUT":
            self.receipt(txn, "busy", f"Very high demand: we couldn't reserve {lab} in time. Nothing was charged. Please retry.")
        else:
            self.receipt(txn, "unavailable", f"{lab} is temporarily unavailable. Nothing was charged. Please retry.")

    def place(self, o):
        txn, run = o["txn"], self.run
        self.orders[txn] = o
        parts = {}
        for item, qty in o["cart"]:
            parts.setdefault(self.router.localize_query(item), []).append((item, qty))
        emit("ORDER", txn_id=txn, customer=o["customer"], region=o["region"], latency=LATENCY[o["region"]],
             cart=[{"item_id": i, "qty": q, "node": n} for n, its in parts.items() for i, q in its],
             fault=o.get("fault"), ordered=o.get("ordered", True))
        run.pace()
        self.wal.write_record(txn, 0, "BEGIN")
        run.pace()

        prepared, verdict = [], None
        for nid, items in parts.items():
            node = self.nodes[nid]
            emit("PREPARE_SENT", txn_id=txn, node=nid)
            run.pace()
            try:
                vote, reason, detail = node.prepare(txn, items, o.get("patience", 1.0))
            except NodeDown:
                vote, reason, detail = "VOTE_ABORT", "NODE_DOWN", {"item": items[0][0]}
            emit("VOTE_RECEIVED", txn_id=txn, node=nid, vote=vote, reason=reason, detail=detail)
            run.pace()
            if vote == "VOTE_COMMIT":
                prepared.append(nid)
            else:
                verdict = (reason, detail)
                break

        if verdict is None and o.get("fault") == "coord_crash":
            self.in_doubt[txn] = prepared
            emit("COORD_CRASH", txn_id=txn, nodes=prepared)
            self.receipt(txn, "pending", "Processing your order... (waiting for the system to decide)")
            return

        if verdict is None:
            self.wal.write_record(txn, 0, "COMMIT")             # the commit point
            for item, qty in o["cart"]:
                self.sold[item] = self.sold.get(item, 0) + qty
            emit("DECISION_COMMIT", txn_id=txn, nodes=prepared)
            run.pace()
            late = []
            for nid in prepared:
                node = self.nodes[nid]
                if o.get("fault") == "node_crash":
                    node.crash()
                try:
                    node.commit(txn)
                except NodeDown:
                    late.append(nid)
                    emit("DELIVERY_FAILED", txn_id=txn, node=nid)
            if late:
                self.receipt(txn, "finalizing", "Order confirmed. We're finalizing it while a server recovers. No action needed.")
            else:
                self.receipt(txn, "confirmed", "Order confirmed. Shipping soon.")
        else:
            reason, detail = verdict
            self.wal.write_record(txn, 0, "ABORT")
            emit("DECISION_ABORT", txn_id=txn, nodes=prepared, reason=reason, detail=detail)
            run.pace()
            for nid in prepared:
                try:
                    self.nodes[nid].abort(txn)
                except NodeDown:
                    emit("DELIVERY_FAILED", txn_id=txn, node=nid)
            self._fail_receipt(txn, reason, detail)

    def recover(self):
        run = self.run
        emit("COORD_RECOVERY_START")
        run.pace()
        decided = {r["txn_id"] for r in self.wal.get_all_records() if r["type"] in ("COMMIT", "ABORT")}
        todo = [t for t in list(self.in_doubt) if t not in decided]
        emit("COORD_RECOVERY", rows=[{"txn_id": t, "state": "BEGIN logged, no decision",
                                      "resolution": "ABORT (presumed abort)"} for t in todo])
        run.pace()
        for t in todo:
            nodes = self.in_doubt.pop(t)
            self.wal.write_record(t, 0, "ABORT")
            emit("DECISION_ABORT", txn_id=t, nodes=nodes, reason="PRESUMED_ABORT", detail={})
            run.pace()
            for nid in nodes:
                self.nodes[nid].abort(t)
            self.receipt(t, "retry", "Something went wrong on our side. You were not charged. Please retry.")
            run.pace()
        emit("COORD_RECOVERY_END")

    def recover_node(self, nid):
        resolved = self.nodes[nid].recover(self.wal.get_all_records())
        for t, final in resolved:
            if final == "COMMITTED":
                self.receipt(t, "confirmed", "Order confirmed. Shipping soon.")
            else:
                self.receipt(t, "retry", "Something went wrong on our side. You were not charged. Please retry.")


# --------------------------------------------------------------------------- script
def build_script(it, customers):
    S, n = [], [0]

    def act(t, title, text, concept):
        S.append(dict(t=t, kind="act", title=title, text=text, concept=concept))

    def order(t, items, **kw):
        n[0] += 1
        cart = [(it[k], q) for k, q in items]
        if kw.get("ordered", True):
            cart.sort()                      # lock ordering avoids deadlock
        u = customers[n[0] - 1]
        S.append(dict(t=t, kind="order", txn=f"TXN-{n[0]:02d}", cart=cart, customer=u[0], region=u[1], **kw))

    act(0, "Act 1: normal traffic",
        "Customers around the world buy different items. Each order is its own distributed transaction. They touch different items, so they run side by side without waiting. Watch one order walk through Route, Lock, Check, WAL, Vote, Decide, Apply.",
        "Isolation, 2PC")
    for t, items in [(0.5, [("I1", 1)]), (1.5, [("I3", 2)]), (2.5, [("I5", 1)]), (3.5, [("I6", 2)]), (4.5, [("I4", 1)])]:
        order(t, items)
    act(13, "Act 2: three buyers, one item",
        "Three customers want the same item at almost the same moment. Strict two-phase locking gives the exclusive lock to one transaction at a time. The others queue, then get their turn, and each one sees consistent stock.",
        "Strict 2PL")
    order(13, [("I1", 2)]); order(13.4, [("I1", 1)]); order(13.8, [("I1", 1)])
    act(27, "Act 3: ordered 2, only 1 left",
        "Headphones have a single unit. A customer asks for 2. The system never ships half an order, so the node votes ABORT and nothing changes. The next customer asks for 1 and gets it. The one after that finds it sold out.",
        "Atomicity")
    order(27, [("I2", 2)]); order(33, [("I2", 1)]); order(38, [("I2", 1)])
    act(44, "Act 4: a cart that spans two nodes",
        "Now one order touches items on two different nodes, so two participants must agree. First a cart where both have stock. Then a cart where the first node says yes and the second says no: the node that already reserved stock has to UNDO it.",
        "2PC, Atomicity")
    order(44, [("I3", 1), ("I5", 1)]); order(52, [("I4", 1), ("I6", 9)])
    act(64, "Act 5: deadlock",
        "Two carts lock the same two items in opposite order and each waits for the other forever. The lock manager only has timeouts, so the one with the shorter patience gives up, rolls back, and the other proceeds. (Normal carts avoid this by locking in item order.)",
        "Deadlock, S2PL")
    order(64, [("I3", 1), ("I5", 1)], ordered=False, patience=0.7); order(64.5, [("I5", 1), ("I3", 1)], ordered=False)
    act(98, "Act 6: a node crashes after the coordinator decided COMMIT",
        "The coordinator logs COMMIT for a Camera order, then the node holding it crashes before hearing the decision. Memory and lock table are lost. Other customers get 'unavailable'. When the node restarts, ARIES recovery replays its log and asks the coordinator what happened to the in-doubt order.",
        "Durability, ARIES")
    order(98, [("I5", 1)], fault="node_crash"); order(106, [("I6", 1)]); order(107, [("I5", 1)])
    S.append(dict(t=112, kind="recover_node", index=5))
    order(124, [("I5", 1)])
    act(130, "Act 7: the coordinator dies before deciding",
        "Every vote was COMMIT, but the coordinator crashes before logging a decision. The node is in doubt: it keeps the lock and the reserved stock, and a second buyer for the same item has to wait. This is the blocking problem of 2PC. On restart the coordinator finds BEGIN with no decision and aborts (presumed abort).",
        "2PC blocking")
    order(130, [("I4", 1)], fault="coord_crash"); order(137, [("I4", 1)]); order(139, [("I1", 1)])
    S.append(dict(t=143, kind="recover_coord"))
    return S


# --------------------------------------------------------------------------- driver
def _customers(n):
    try:
        u = pd.read_csv("data/users.csv").sample(n, random_state=7)
        return [(str(r.username), str(r.region_id)) for r in u.itertuples()]
    except Exception:
        regions = list(LATENCY)
        return [(f"customer{i}", regions[i % 3]) for i in range(n)]


def run_scenario(tick=0.3):
    engine_events.SLOWMO, engine_events.UNSAFE = 0.0, False
    for f in glob.glob("logs/*.log"):
        os.remove(f)
    emit("ACT", title="Setup", text="K-Means groups items by latency and demand and assigns each group to a node. Then each node loads its stock.", concept="Allocation")
    perform_initial_allocation()
    inv = pd.read_csv("data/inventory_allocated.csv")
    chosen = []
    for node in sorted(inv.node_id.unique()):
        chosen += list(inv[inv.node_id == node].item_id[:2])
    chosen += [i for i in inv.item_id if i not in chosen]
    it = {f"I{k + 1}": chosen[k] for k in range(6)}
    labels = {it[f"I{k + 1}"]: LABELS[k] for k in range(6)}

    run = Run(tick)
    nodes, topo = {}, {}
    for k in range(6):
        iid = it[f"I{k + 1}"]
        nid = inv.loc[inv.item_id == iid, "node_id"].iloc[0]
        if nid not in nodes:
            nodes[nid] = ScenarioNode(nid, run)
            topo[nid] = {}
        nodes[nid].storage.data[iid] = STOCK[k]
        topo[nid][iid] = {"label": LABELS[k], "stock": STOCK[k]}
    for n in nodes.values():
        n.checkpoint = dict(n.storage.data)
    coord = Coordinator(nodes, run, labels)

    script = build_script(it, _customers(30))
    emit("TOPOLOGY", nodes=topo, total_orders=sum(1 for e in script if e["kind"] == "order"))

    threads, start = [], time.time()

    def go(fn, *a):
        def wrapped():
            try:
                fn(*a)
            except Exception:
                traceback.print_exc()
        th = threading.Thread(target=wrapped, daemon=True)
        th.start()
        threads.append(th)

    def place_after_latency(o):
        time.sleep(LATENCY[o["region"]] * tick / 0.3)   # the request travels to us
        coord.place(o)

    def wait_then(cond, fn):
        end = time.time() + 120 * tick + 10
        while not cond() and time.time() < end:
            time.sleep(0.05)
        fn()

    sel = sorted(nodes)
    for ev in sorted(script, key=lambda e: e["t"]):
        d = start + ev["t"] * tick - time.time()
        if d > 0:
            time.sleep(d)
        k = ev["kind"]
        if k == "act":
            emit("ACT", title=ev["title"], text=ev["text"], concept=ev["concept"])
        elif k == "order":
            go(place_after_latency, ev)
        elif k == "recover_node":
            nid = nodes[inv.loc[inv.item_id == it["I5"], "node_id"].iloc[0]].node_id
            go(wait_then, lambda nid=nid: nodes[nid].down, lambda nid=nid: coord.recover_node(nid))
        elif k == "recover_coord":
            go(wait_then, lambda: bool(coord.in_doubt), coord.recover)
    for th in threads:
        th.join()

    # ---- audit: two independent ways to recompute the stock
    crecs = coord.wal.get_all_records()
    committed = {r["txn_id"] for r in crecs if r["type"] == "COMMIT"}
    rows = []
    for k in range(6):
        iid, init = it[f"I{k + 1}"], STOCK[k]
        node = next(n for n in nodes.values() if iid in n.checkpoint)
        sold = coord.sold.get(iid, 0)
        replay = init - sum(r["undo"] - r["redo"] for r in node.wal.get_all_records()
                            if r["type"] == "PREPARE_UPDATE" and r["page_id"] == iid and r["txn_id"] in committed)
        actual = node.storage.read_stock(iid)
        rows.append({"item_id": iid, "node": node.node_id, "initial": init, "sold": sold,
                     "expected": init - sold, "actual": actual, "replay": replay,
                     "ok": actual == init - sold == replay})
    emit("AUDIT", rows=rows, ok=all(r["ok"] for r in rows))
    emit("SIM_END")