import duckdb
import pandas as pd


con = duckdb.connect(database=':memory:')
df = pd.read_csv('accesslog', sep=' ', header=None)
con.register('accesslog', df)

res = con.execute("""
SELECT * FROM accesslog
""").df()

print(res)
