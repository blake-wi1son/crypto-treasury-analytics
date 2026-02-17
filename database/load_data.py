import duckdb
import glob

DB_PATH = "data/warehouse/analytics.db"
RAW_DIR = "data/raw"

con = duckdb.connect(DB_PATH)

xrp_files = glob.glob(f"{RAW_DIR}/xrp_prices_*.csv")

#TODO - refactor to function

if xrp_files:
    print("xrp_files exist, loading to database")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_xrp_prices AS
                SELECT * FROM read_csv_auto('{RAW_DIR}/xrp_prices_*.csv')
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_xrp_prices").fetchone()[0]
    print(f"Loaded {count} xrp records from {len(xrp_files)} xrp files")
else:
    print("No xrp files found")

fed_files = glob.glob(f"{RAW_DIR}/fed_data_*.csv")

if fed_files:

    print("fed_files exist, laoding to database")

    con.execute(f"""
            CREATE OR REPLACE TABLE raw_fed_rates AS
                SELECT * FROM read_csv_auto('{RAW_DIR}/fed_data_*.csv')
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_fed_rates").fetchone()[0]
    print(f"Loaded {count} fed records from {len(fed_files)} files")
else:
    print("No fed rate files found")


tables = con.execute("Show tables").fetchall()
for table in tables:
    print(f" - {table[0]}")

con.close()