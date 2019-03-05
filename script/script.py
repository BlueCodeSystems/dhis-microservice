import config
import psycopg2
import sys
import json
import requests

try:
    report_month = sys.argv[1]
except:
    report_month = None

rest_api_endpoint = '/api/26/dataValueSets'

#db connection
conn = psycopg2.connect(host=config.POSTGRES_HOST,database=config.POSTGRES_DB, user=config.POSTGRES_USER, password=config.POSTGRES_PASS)
cur = conn.cursor()

#fetch reports
sql="SELECT * FROM public.couchdb where doc @> '{\"type\":\"Report\"}';"
cur.execute(sql)
result = cur.fetchall()

def clean_indicator(indicator):
    if(indicator["dhisId"] !=''):
    	return { "dhisId":indicator["dhisId"],
             	"value":indicator["value"]}

def clean_report(report):
    cleaned_indicators = map(clean_indicator, report["hia2Indicators"])
    sql = "SELECT dhis_orgunit_id FROM openmrs_dhis_location_map WHERE location_uuid ='"+report["locationId"]+"';"
    cur.execute(sql)
    dhis_org_id = cur.fetchall()
    return { "orgUnitId":dhis_org_id[0][0],
              "reportDate":report["reportDate"],
             "hia2Indicators":cleaned_indicators}

def filter_report_by_month(month):
    def f(report):
        if(report["reportDate"][:7] == month):
            return report
    return f 
    

def parse_to_dhis_dataelement_json_payload(reports):
    dataelements = []
    for report in reports:
        if(report != None):
            org_unit_id = report["orgUnitId"]
            period = report["reportDate"][:7].replace("-","")
            for indicator in report["hia2Indicators"]:
               if(indicator != None):
                   dataelements.append({"dataElement":indicator["dhisId"],"period":period,"orgUnit":org_unit_id,"value":indicator["value"]})
    return json.dumps({"dataValues":dataelements})

cleaned_reports = map(clean_report,[row[0] for row in result])

if(report_month !=  None):
    final_reports = map(filter_report_by_month(report_month),cleaned_reports)
else:
    final_reports = cleaned_reports

json_payload = parse_to_dhis_dataelement_json_payload(final_reports)

#post reports to DHIS
url = config.DHIS_HOST+rest_api_endpoint

r = requests.post(url, auth=(config.DHIS_USER,config.DHIS_PASS), json=json_payload)
