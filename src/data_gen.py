from faker import Faker
import pandas as pd
import random
import os

def generate_synthetic_data(num_users=100, num_items=20):
    fake = Faker()
    os.makedirs("data", exist_ok=True)
    
    # Generate Inventory
    items = []
    for i in range(num_items):
        items.append({
            "item_id": f"ITEM_{1000+i}",
            "item_name": fake.ecommerce_name() if hasattr(fake, 'ecommerce_name') else f"Product {i}",
            "stock_quantity": random.randint(10, 100),
            "region_id": random.choice(["US-East", "EU-Central", "Asia-Pacific"]),
            "latency_profile": random.uniform(5, 150), # ms
            "demand_density": random.uniform(0, 1)
        })
    
    inventory_df = pd.DataFrame(items)
    inventory_df.to_csv("data/inventory.csv", index=False)
    
    # Generate Users
    users = []
    for i in range(num_users):
        users.append({
            "user_id": 884000 + i,
            "username": fake.user_name(),
            "region_id": random.choice(["US-East", "EU-Central", "Asia-Pacific"]),
            "credit_balance": random.randint(100, 5000)
        })
    
    users_df = pd.DataFrame(users)
    users_df.to_csv("data/users.csv", index=False)
    
    print(f"Generated {num_users} users and {num_items} items.")

if __name__ == "__main__":
    generate_synthetic_data()
