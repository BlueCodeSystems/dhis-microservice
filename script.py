import config
import psycopg2

#db connection
conn = psycopg2.connect(host=config.POSTGRES_HOST,database=config.POSTGRES_DB, user=config.POSTGRES_USER, password=config.POSTGRES_PASS)
cur = conn.cursor()
sql="SELECT * FROM public.couchdb where doc @> '{\"type\":\"Report\"}';"

cur.execute(sql)

print(cur.fetchall())