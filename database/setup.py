import duckdb
import os

DB_PATH = os.path.join("data", "warehouse", "analytics.db")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) #create/check database path

con = duckdb.connect(DB_PATH)

print(f"Database created at: {DB_PATH}")

# test the database file
test = con.execute("SELECT 'duckdb is working...' AS message").fetchone()
print(test[0])

con.close()
print("Setup finished.")