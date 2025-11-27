import sqlite3

db_path = "data/northwind.sqlite"
sql_path = "data/create_views.sql"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

with open(sql_path, "r") as f:
    sql_script = f.read()

cursor.executescript(sql_script)
conn.commit()
conn.close()
print("Views created successfully.")
