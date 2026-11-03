# One-page plain-language walkthrough
## "How a flash sale purchase runs, inside the engine"

### The question the engine answers, 50 times in a second

> *"One more user wants to buy product X. Can we sell them one more unit, without going below zero, and can we keep that promise even if a machine dies halfway through?"*

Every request — 50 of them at once — runs through the same 7-step dance. The engine is a **shared-nothing distributed database**: it acts like several independent shops, but they coordinate so the numbers never lie.

---

### Step 1 — Know where the goods live
**Input:** a generated catalog (`data/inventory.csv`) — 100 users, 20 products, each product tagged with *how far away it is* (latency) and *how much people will want it* (demand density).
**What happens:** a K-Means algorithm groups the 20 products into 3 clusters based on those two numbers and assigns each group to a node (a server). This becomes `data/inventory_allocated.csv` — the engine's **address book**.
**Why:** instead of spreading a hot product across all shops (which means every buyer talks to every shop), products with similar behaviour are *pinned* to the same shop. Hot products get their own dedicated node.

---

### Step 2 — The event conductor starts the sale
`main.py` reads the address book, starts 3 node processes (one per node), then fires **50 purchase requests at the same time** against one hot product (the one with stock 18). Each request is a thread that says:
> "User X wants one unit of ITEM_1000."

---

### Step 3 — Find the right shop
`router.py` asks: *"Where does ITEM_1000 live?"* — answer: **Node-1**. The request is routed **only** to Node-1; Node-2 and Node-3 never see it. This is **query localization**: instead of a loud broadcast to everyone, a single point-to-point call. (Result: 50/50 correct, 0 misses, 0 broadcasts.)

---

### Step 4 — The judge (two-phase commit)
The coordinator (the judge) asks Node-1: *"Do you have the stock, and can you promise me the change?"* — this is the **PREPARE** phase.

Inside `node.py`, the node does three things in order:
1. **Takes a key** — an exclusive (X) lock on ITEM_1000. Only one thread at a time can hold it, so every buyer queues up single-file.
2. **Checks stock.** `if stock < 1 → VOTE_ABORT` (your request is rejected, no queue-busting).
3. **Writes the promise to the audit log first** — `PREPARE_UPDATE` with `undo` (what the stock was) and `redo` (what it will become), then flushes to disk. This is the **write-ahead rule**: the decision exists on non-volatile storage *before* anything changes.

**Vote:** the node replies `VOTE_COMMIT`.

---

### Step 5 — The decision
The coordinator tallies the votes. All 18 ready nodes said `VOTE_COMMIT` → **COMMIT**. It writes a `COMMIT` record to the coordinator log, flushes it, and shouts **GLOBAL_COMMIT** to every node. (If even one had said `VOTE_ABORT`, the whole group would get `GLOBAL_ABORT` and roll back — atomicity.) Each node then applies the change (`18 → 17`), writes its own COMMIT record, and **releases the lock**.

> Why is the lock held until the very end? Because if a lock were released early and the transaction later aborted, a *later* reader could see a partially rolled-back, inconsistent state. Strict 2PL (locks until termination) is what guarantees "you see either before or after, never mid-flight".

---

### Step 6 — The outcome
The 18 threads that waited in line commit successfully; the 32 threads that arrived after stock hit 0 get `VOTE_ABORT` at the stock-check step — each with its own lock, released. Net effect: **18 units sold, stock 0, zero oversold.** The accounting invariant held: `18 sold = 18 committed`.

---

### Step 7 — Crash time (why the log matters)
`test_recovery.py` simulates a crash: it lets the transaction reach `VOTE_COMMIT`, destroys the node, and reboots it with only a checkpoint snapshot. What happens now is the **ARIES recovery** routine:

1. **Analysis** — it scans the node's WAL from the checkpoint and rebuilds the transaction table: who was PREPARED, COMMITTED, or ABORTED.
2. **Decision** — it checks the coordinator log for the verdict of each PREPARED transaction.
3. **REDO** — for every committed PREPARED transaction, it re-applies the `redo` image from the log (restores stock to 9 for the crashed transaction).
4. **UNDO** — for aborted ones, it rolls back.

The test asserts: post-recovery stock must be exactly **9**. It is → **PASS**. The system never loses a committed sale and never resurrects an aborted one.

---

### The telemetry tab (the dashboard)
Streamlit renders live panels: the K-Means placement chart, per-node inventory bars, the raw write-ahead log with color-coded COMMIT/ABORT records, and a top-item contention table. `wal_stats.py` then parses the WAL back into numbers — outcome counts, average commit latency, timing — so the graph on the screen has a verifiable number behind it.

---

### The one-page summary, in one figure

```
catalog → (K-Means) → address book → 3 nodes stand up
        │
        ▼
  50 threads hurl at hot product
        ▼
   router says "Node-1" (localization)
        ▼
   judge → PREPARE: lock + check + WAL(undo/redo)
        ▼
   all vote COMMIT → GLOBAL_COMMIT
        ▼
   stock 18 → 17  (no overshoot, never seen stale)
        ▼
   crash? redo-log replay → state restored    (recovery PASS)
        ▼
   dashboard shows it live
```

**In one sentence:** the engine makes every purchase *atomic across the network*, *serializes hot-row contention with strict locks*, *durable-izes every decision with a write-ahead log*, and *proves it both by a crash-injection test and by a dashboard you can watch*.