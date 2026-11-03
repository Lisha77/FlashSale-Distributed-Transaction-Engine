import pandas as pd
from sklearn.cluster import KMeans
import os

def allocate_fragments_kmeans(data_df, num_clusters, feature_cols):
    """
    Uses K-Means to cluster data based on features (e.g., Latency, Demand Density).
    Returns the dataframe with a 'node_id' column.
    """
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    data_df['cluster_label'] = kmeans.fit_predict(data_df[feature_cols])
    
    # Map clusters to node IDs
    node_mapping = {i: f"Node-{i+1}" for i in range(num_clusters)}
    data_df['node_id'] = data_df['cluster_label'].map(node_mapping)
    
    return data_df, kmeans

def perform_initial_allocation():
    if not os.path.exists("data/inventory.csv"):
        print("Inventory data not found. Please run data_gen.py first.")
        return
    
    inv_df = pd.read_csv("data/inventory.csv")
    # Cluster based on latency and demand
    allocated_df, _ = allocate_fragments_kmeans(inv_df, 3, ['latency_profile', 'demand_density'])
    
    # Save the allocation map (Data Dictionary)
    allocated_df.to_csv("data/inventory_allocated.csv", index=False)
    print("ML Allocation Complete: data/inventory_allocated.csv created.")

if __name__ == "__main__":
    perform_initial_allocation()
