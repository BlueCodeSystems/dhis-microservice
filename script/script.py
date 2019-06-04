import config
import psycopg2
import sys
import json
import requests
import datetime

#log time 
print('\n\n','['+datetime.datetime.now().strftime('%c')+']')

try:
    report_month = sys.argv[1]
except:
    report_month = None

dhis_credentials = (config.DHIS_USER,config.DHIS_PASS)
data_elements_id = []

#db connection
conn = psycopg2.connect(host=config.POSTGRES_HOST,database=config.POSTGRES_DB, user=config.POSTGRES_USER, password=config.POSTGRES_PASS)
cur = conn.cursor()

#fetch reports
sql="SELECT * FROM public.couchdb where doc @> '{\"type\":\"Report\"}';"
cur.execute(sql)
result = cur.fetchall()

#Retrieve data element ids
url = config.DHIS_HOST+config.DHIS_DATASET_REST_API_ENDPOINT+'/'+config.DHIS_DATASET
r = requests.get(url, auth=dhis_credentials)
response = r.json()

data_element_ids = [d['dataElement']['id'] for d in response['dataSetElements']]

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
    

def parse_to_dhis_dataelement_json_payload(report,dataelement_set):
    if(report != None):
        report_date = report['reportDate'][:10]
        data_elements = []
        
        #Populate required data elements with 0 as defualt value
        for dataelement_id in dataelement_set:
            data_elements.append({"dataElement":dataelement_id,"value":"0"})

        #Update data element value from indicator values
        for indicator in report['hia2Indicators']:
                if(indicator != None):
                     for data_element in data_elements:
                         if(indicator['dhisId'] == data_element['dataElement']):
                             data_element['value'] = indicator['value']

        return {"dataSet":config.DHIS_DATASET,
                "completeDate":report_date,
                "period":report_date[:7].replace("-",""),
                "orgUnit":report['orgUnitId'],
                "dataValues":data_elements}

cleaned_reports = map(clean_report,[row[0] for row in result])

if(report_month !=  None):
    final_reports = map(filter_report_by_month(report_month),cleaned_reports)
else:
    final_reports = cleaned_reports

#post reports to DHIS
url = config.DHIS_HOST+config.DHIS_DATA_VALUE_SET_REST_API_ENDPOINT
for report in final_reports:
   if(report != None):
       json_payload = parse_to_dhis_dataelement_json_payload(report,data_element_ids)
       r = requests.post(url, auth=dhis_credentials, json=json_payload, headers={"Content-Type":"application/json"})
       print(r.json())
