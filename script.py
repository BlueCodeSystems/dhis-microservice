import config
import psycopg2

#db connection
conn = psycopg2.connect(host=config.POSTGRES_HOST,database=config.POSTGRES_DB, user=config.POSTGRES_USER, password=config.POSTGRES_PASS)