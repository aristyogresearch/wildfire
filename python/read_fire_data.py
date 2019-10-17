import sqlite3
import pandas as pd
con = sqlite3.connect('FPA_FOD_20170508.sqlite')
df = pd.read_sql_query("SELECT * from fires", con)
df.to_csv('FPA_FOD_20170508.csv', sep=',')
