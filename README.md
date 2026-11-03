# ⚡ FlashSale: Distributed Transaction Engine

A teaching simulation and visualizer for distributed database architectures. This project demonstrates how modern databases handle extreme concurrency (like a flash sale) across multiple nodes without losing data integrity or overselling inventory.

## 🌟 Features
- **Machine Learning Data Allocation**: Uses K-Means clustering (`scikit-learn`) to intelligently shard inventory data based on access latency and regional demand.
- **Strict Two-Phase Locking (S2PL)**: Guarantees isolation by managing exclusive (X) locks with wait queues and timeouts to prevent double-spending.
- **Two-Phase Commit (2PC)**: Ensures atomic transactions across multiple database nodes (all-or-nothing distributed carts).
- **Write-Ahead Logging (WAL)**: Logs `undo` and `redo` records before memory changes to guarantee durability.
- **ARIES-Style Crash Recovery**: Simulates node and coordinator failures, recovering lost memory states by analyzing and replaying logs.
- **Real-Time DBA Dashboard**: A `Streamlit` interface that visualizes lock queues, consensus messages, telemetry logs, and failure recovery in real-time.

## 🛠️ Tech Stack
- **Core Engine**: Python 3.10+ (Standard Libraries: `threading`, `concurrent.futures`)
- **Data Processing & ML**: `pandas`, `scikit-learn`
- **Synthetic Data**: `Faker`
- **Dashboard**: `Streamlit`

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have Python 3.10+ installed. Install the required dependencies:
```bash
pip install pandas scikit-learn faker streamlit
```

### 2. Setup & Data Generation
First, generate the synthetic user and inventory datasets, and run the ML allocation script:
```bash
python data_gen.py
```
*(This creates `data/inventory.csv` and `data/users.csv`, and maps them to nodes).*

### 3. Run the Simulation
Launch the real-time telemetry dashboard to watch the database engine at work:
```bash
streamlit run main.py --live
```
*(Or run `python main.py --classic` for a terminal-only stress test).*

## 📂 Project Structure
```text
flash_sale_engine/
├── data/                   # Generated CSV datasets
├── logs/                   # WAL logs for Coordinator and Nodes
├── src/                    # Core Database Engine
│   ├── concurrency.py      # Strict 2PL Lock Manager
│   ├── coordinator.py      # 2PC Coordinator logic
│   ├── ml_allocation.py    # K-Means fragmentation logic
│   ├── node.py             # Data node execution & state
│   ├── router.py           # Query localization
│   ├── scenario.py         # Flash sale simulation scripts
│   └── storage.py          # Storage engine and WAL logger
├── telemetry/              # Event bus and Dashboard
│   ├── bus.py              # Pub/Sub event bus
│   ├── dashboard.py        # Streamlit dashboard logic
│   └── engine_events.py    # Telemetry wrappers
├── data_gen.py             # Synthetic data generator
├── main.py                 # Application entry point
├── requirements.txt        # Python dependencies
└── README.md
```

## 🧪 Simulation Scenarios (What to look for)
When running the dashboard, watch for these specific events:
1. **Lock Contention**: Multiple buyers waiting in the queue for the same item tile.
2. **Distributed Commit**: A cart spanning Node 1 and Node 2 requiring consensus messages (PREPARE / VOTE_COMMIT).
3. **Deadlocks**: Two transactions locking resources in reverse order, forcing a timeout and rollback for the victim.
4. **Crash Recovery**: Use the dashboard to simulate a node crash; watch the engine parse the WAL to UNDO unfinished transactions and REDO committed ones.

## 📝 License
Built for Academic & Demonstration Purposes.
