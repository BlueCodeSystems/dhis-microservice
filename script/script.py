import config
import psycopg2

#db connection
conn = psycopg2.connect(host=config.POSTGRES_HOST,database=config.POSTGRES_DB, user=config.POSTGRES_USER, password=config.POSTGRES_PASS)
cur = conn.cursor()
sql="SELECT * FROM public.couchdb where doc @> '{\"type\":\"Report\"}';"

cur.execute(sql)
result = cur.fetchall()

def clean_indicator(indicator):
    if(indicator["dhisId"] !=''):
    	return { "dhisId":indicator["dhisId"],
             	"value":indicator["value"]}

def clean_report(report):
    cleaned_indicators = map(clean_indicator, report["hia2Indicators"])
    return { "locationId":report["locationId"],
             "hia2Indicators":cleaned_indicators}

cleaned_reports = map(clean_report,[row[0] for row in result])