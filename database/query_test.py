import duckdb

con = duckdb.connect("data/warehouse/analytics.db")

tables = con.execute("SHOW TABLES").fetchall()
print("Tables:", tables)

xrp_result = con.execute("""
    SELECT
        timestamp
        , price
        , crypto_id
    FROM raw_xrp_prices
    ORDER BY timestamp DESC
    LIMIT 5
""").fetchdf()

print("xrp Results below:")
print(xrp_result)

fed_data = con.execute("""
    SELECT
        date
        , DFF_value
        , DGS2_value
        , DGS10_value
    FROM raw_fed_rates
    ORDER BY date DESC
    LIMIT 5                       
""").fetchdf()

print("fed rate data below:")
print(fed_data)

summary_data = con.execute("""
    SELECT
        'xrp' AS dataset
        , COUNT(*) AS row_cnt
    FROM raw_xrp_prices
                           
    UNION ALL
                           
    SELECT
        'fred' AS dataset
        , COUNT(*) AS row_cnt
    FROM raw_fed_rates    
""").fetchdf()

print("dataset summary below:")
print(summary_data)

con.close()
