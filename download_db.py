import requests
import os
import time

url = "https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/main/dist/northwind.db"
output_path = "data/northwind.sqlite"

if os.path.exists(output_path):
    os.remove(output_path)

print(f"Downloading {url} to {output_path}...")
try:
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    print("Download complete.")
    print(f"File size: {os.path.getsize(output_path)} bytes")
    
    # Create views for easier querying
    import sqlite3
    conn = sqlite3.connect(output_path)
    cursor = conn.cursor()
    cursor.execute('CREATE VIEW IF NOT EXISTS OrderDetails AS SELECT * FROM "Order Details"')
    cursor.execute('CREATE VIEW IF NOT EXISTS DiscontinuedProducts AS SELECT * FROM Products WHERE Discontinued = 1')
    cursor.execute('CREATE VIEW IF NOT EXISTS OrderItems AS SELECT * FROM "Order Details"')
    cursor.execute('CREATE VIEW IF NOT EXISTS Order_Items AS SELECT * FROM "Order Details"')
    cursor.execute('CREATE VIEW IF NOT EXISTS ProductCategories AS SELECT * FROM Categories')
    conn.commit()
    conn.close()
    print("Created views: OrderDetails, DiscontinuedProducts, OrderItems, ProductCategories.")
    
except Exception as e:
    print(f"Error: {e}")
