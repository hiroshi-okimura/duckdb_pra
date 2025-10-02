import duckdb, pandas as pd

# ① メモリ上のDBで即席テーブルを作成
con = duckdb.connect(database=':memory:')

# ② Pandasから取り込み → SQL 集計
df = pd.DataFrame({
    'brand': ['A','A','B','B','B'],
    'price': [100, 120, 80, 90, 110],
})
con.register('brands', df)  # DataFrameを一時テーブルとして登録

res = con.execute("""
SELECT brand, COUNT(*) AS n, AVG(price) AS avg_price
FROM brands
GROUP BY 1
ORDER BY 1
""").df()

print(res)

# ③ DuckDBファイルに保存（永続化）
# ファイルDBに接続してからテーブルを作成
con2 = duckdb.connect('duckdb-play.db')
con2.register('brands', df)  # ファイルDBにもDataFrameを登録
con2.execute("CREATE TABLE t AS SELECT * FROM brands")
con2.close()

# ファイルDBを使う例（プロジェクト直下に duckdb-play.db を作成）
con3 = duckdb.connect('duckdb-play.db')
print(con3.execute("SELECT COUNT(*) FROM t").fetchall())
con3.close()
