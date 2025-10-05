import duckdb
import sqlite3
import pandas as pd
import time

# 共通データ読み込み（gzipもOK）
start = time.perf_counter()
df = pd.read_csv('access_log.20250831.gz', sep=' ', header=None, compression='infer')
end = time.perf_counter()
print(f"Read data: {end - start:.4f} 秒")

# --- DuckDB ---
start = time.perf_counter()

con = duckdb.connect(database=':memory:')
con.register('accesslog', df)
res_duck = con.execute("""
SELECT
    split_part("5", '?', 1) AS path,
    COUNT(*) AS cnt
FROM accesslog
GROUP BY path
ORDER BY cnt DESC
""").df()

end = time.perf_counter()
print(res_duck)
print(f"DuckDB: {end - start:.4f} 秒")

# --- SQLite ---
start = time.perf_counter()

con = sqlite3.connect(':memory:')
df.to_sql('accesslog', con, index=False, if_exists='replace')
res_sqlite = pd.read_sql_query("""
SELECT
    CASE
        WHEN instr("5", '?') = 0 THEN "5"
        ELSE substr("5", 1, instr("5", '?') - 1)
    END AS path,
    COUNT(*) AS cnt
FROM accesslog
GROUP BY path
ORDER BY cnt DESC
""", con)

end = time.perf_counter()
print(res_sqlite)
print(f"SQLite: {end - start:.4f} 秒")
