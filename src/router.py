import pandas as pd

class GlobalQueryRouter:
    def __init__(self, allocation_file="data/inventory_allocated.csv"):
        self.allocation_df = pd.read_csv(allocation_file)
        # Create a quick lookup map: item_id -> node_id
        self.localization_map = dict(zip(self.allocation_df['item_id'], self.allocation_df['node_id']))

    def localize_query(self, item_id):
        """
        Unit 2.1: Query Localization
        Maps the global item_id to the physical node fragment.
        """
        target_node = self.localization_map.get(item_id)
        if target_node:
            print(f"[Router] Localizing item {item_id} to physical fragment on {target_node}")
            return target_node
        else:
            print(f"[Router] Error: Item {item_id} not found in global data dictionary.")
            return None

    def get_item_details(self, item_id):
        item_row = self.allocation_df[self.allocation_df['item_id'] == item_id]
        if not item_row.empty:
            return item_row.iloc[0].to_dict()
        return None
