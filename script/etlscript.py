import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import json
import requests
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Function definitions

#Get visit ids that satisfy a given indicator
def visitList(indicator, visitTypeId, locationId, visitMonth, conn):
    cursor = conn.cursor(dictionary = True)
    strings = []
    lesionsConceptId = 165184
    for key in indicator:
        if (key != 'question'):
            if (lesionsConceptId in indicator.values()):
                strings.append('obs_value_concept_id != {}'.format(indicator[key] - 1))
            else:
                strings.append('obs_value_concept_id = {}'.format(indicator[key]))
    answers = ' OR '.join(strings)
    query = "SELECT visit_id FROM visit_data_matvw WHERE obs_concept_id = {} AND ({}) AND visit_type_id = {} AND visit_location_id = {} AND DATE_FORMAT(date_started, '%m-%Y') = '{}' AND visit_location_retired = 0".format(indicator['question'], answers, visitTypeId, locationId, visitMonth)
    cursor.execute(query)
    visitIdsList = cursor.fetchall()
    result = []
    for visit in visitIdsList:
        result.append(visit['visit_id'])
    return result

#Get patients that satisfy all indicators
def listIntersection(args):
    return list(set(args[0]).intersection(*args))

#Get patient ids from list of visit ids
def patientList(visitIds, conn):
    cursor = conn.cursor(dictionary = True)
    if (len(visitIds) == 1):
        query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id = {}'.format(visitIds[0])
    elif (len(visitIds) == 0):
        return []
    else:
        query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id IN {}'.format(tuple(visitIds))
    cursor.execute(query)
    patientIds = cursor.fetchall()
    result = []
    for visit in patientIds:
        result.append(visit['patient_id'])
    return result

# Filter patients by age
def patientAges(patientIds, conn, lowerLimit, upperLimit):
    if (len(patientIds) == 1):
        query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id = {} AND identifier_type_id = 4 AND age BETWEEN {} AND {}'.format(patientIds[0], lowerLimit, upperLimit)
    elif (len(patientIds) == 0):
        return 0
    else:
        query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id IN {} AND identifier_type_id = 4 AND age BETWEEN {} AND {}'.format(tuple(patientIds), lowerLimit, upperLimit)
    cursor = conn.cursor(dictionary = True)
    cursor.execute(query)
    patientsInRange = cursor.fetchall()
    result = []
    for visit in patientsInRange:
        result.append(visit['patient_id'])
    return result[0]

# Aggregated function call
def patientCount(questionAnswer, visitTypeId, locationId, visitMonth, conn, lowerAgeLimit = -1, upperAgeLimit = -1):
    listOfVisits = []
    #cursor = conn.cursor(dictionary = True)
    for questAns in questionAnswer:
        listOfVisits.append(visitList(questAns, visitTypeId, locationId, visitMonth, conn))
    indicators = listIntersection(listOfVisits)
    patients = patientList(indicators, conn)
    if (lowerAgeLimit == -1 or upperAgeLimit == -1):
        return len(patients)
    else: 
        result = patientAges(patients, conn, lowerAgeLimit, upperAgeLimit)
        #cursor.close()
        return result

#Get patient counts for a particular visit type
def visitTypeFunc(listOfIndicators, visitTypeId, visitLocationId, visitMonth, conn):
    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165132}])
    hivUnknown = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165131}])
    hivNegative = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}])
    hivPositive = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn)       

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165127}])
    notOnArt = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onARTUnder24 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 0, 24)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onART_25_29 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 25, 29)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onART_30_34 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 30, 34)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onART_35_39 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 35, 39)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onART_40_49 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 40, 49)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onART_50_59 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 50, 59)

    concepts1 = listOfIndicators.copy()
    concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    onARTOver60 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, conn, 60, 110)

    totalOnART = onARTUnder24 + onART_25_29 + onART_30_34 + onART_35_39 + onART_40_49 + onART_50_59 + onARTOver60

    total = hivUnknown + hivNegative + notOnArt + totalOnART

    countList = [hivUnknown, hivNegative, hivPositive, notOnArt, onARTUnder24, onART_25_29, onART_30_34, onART_35_39, onART_40_49, onART_50_59, onARTOver60, totalOnART, total]
    
    return countList

# Get patient counts for each of the three visit types
def indicatorRows(args):
    print('Thread Started')
    indicators = args[0]
    visitTypeIds = args[1]
    visitLocationId = args[2]
    visitMonth = args[3]
    connection = args[4]

    # Initial Visit - visit_type_id = 2
    initialVisit = visitTypeFunc(indicators, visitTypeIds[0], visitLocationId, visitMonth, connection)

    # One Year Follow-up - visit_type_id = 5
    oneYearFollowUp = visitTypeFunc(indicators, visitTypeIds[1], visitLocationId, visitMonth, connection)

    # Routine Visit - visit_type_id = 6
    routineVisit = visitTypeFunc(indicators, visitTypeIds[2], visitLocationId, visitMonth, connection)

    # return result as a dictionary
    result = {'initialVisit': initialVisit, 'oneYearFollowUp': oneYearFollowUp, 'routineVisit': routineVisit}
    connection.close()
    return result

# Get counts for each of the indicators
def indicatorList(location, month, connection_pool):
    result = {}
    indicators = [
        ([{'question':165182, 'answer':165183}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([{'question':165155, 'answer':1}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([{'question':165160, 'answer':165162}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([{'question':165219, 'answer':165174, 'answer1':165175}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([], [3, 3, 3], location, month, connection_pool.get_connection()),
        ([{'question':165219, 'answer':165176, 'answer1':165177}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([{'question':165143, 'answer':165144, 'answer1':165145, 'answer2':165146}], [2, 5, 6], location, month, connection_pool.get_connection()),
        ([{'question':165182, 'answer':165184}], [2, 5, 6],  location, month, connection_pool.get_connection())
    ]

    with ThreadPoolExecutor(max_workers = 10) as executor:            
        indicatorNames = ('suspectCancer', 'viaScreening', 'positiveVIA', 'cryoThermal', 'prevDelayedCryoThermal', 'cryoThermalDelayed', 'ptComplication', 'lesions')
        result = dict(zip(indicatorNames, executor.map(indicatorRows, indicators)))
    
    suspectCancer = result['suspectCancer']
    viaScreening = result['viaScreening']
    suspectCancerViaScreening = aggregate(suspectCancer, viaScreening)
    result['suspectCancerViaScreening'] = suspectCancerViaScreening
    
    del result['prevDelayedCryoThermal']['oneYearFollowUp']
    del result['prevDelayedCryoThermal']['routineVisit']
    
    #Total number of clients treated with cryotherapy/thermal coagulation (5+6)
    listOfRows = [result['cryoThermal']['initialVisit'], result['cryoThermal']['oneYearFollowUp'], result['cryoThermal']['routineVisit'], result['prevDelayedCryoThermal']['initialVisit']]
    totalCryoThermal = {}
    totalCryoThermal['initialVisit'] = sumRows(listOfRows)
    result['totalCryoThermal'] = totalCryoThermal
        
    valuesList = []
    for item in result:
        if (isinstance(item, dict)):
            for key in item.keys():
                valuesList.append(item[key])
        else:
            valuesList.append(item)

    return result

# Aggregate two dictionaries
def aggregate(dict1, dict2):
    result = {}
    for key in dict1:
        lists = [dict1[key], dict2[key]]
        sumOfLists = [sum(x) for x in zip(*lists)]
        result[key] = sumOfLists
    return result

# Sum up the corresponding values in several lists
def sumRows(lists):
    result = [sum(x) for x in zip(*lists)]
    return result

# Create list of dictionary values
def getDataElements(location, month, connection_pool):
    dataElements = []
    categoryOptionCombos = {
        'initialVisit': ['ZxsS9HGdhV1', 'AHS2fnqf971', 'VMMUi2HZOpS', 'PqG5oFcHpLf', 'I5LyQqIyqX9', 'eb9lrs8dq64', 'NmBwhTMgYDK', 'CNKn8YRApww', 'kImwcR75U8J', 'wJlF4hrj7IA', 'ZDexPKuoua5', 'nc4NgEe5n91', 'mpAMSeHtHhc'],
        'oneYearFollowUp': ['q7v6tWrRqF9', 'haZd47S1HFE', 'N1dUQRNIVLr', 'qhI88d1Z0F9', 'ntHyHb6e0wx', 'M9CKfD4v6lw', 'lD5EVDDcNqQ', 'ICJkUDl3DdT', 'Uxb6dihTO4D', 'ApDZ1vSzys3', 'HCWBLRB75Q7', 'vMJjcIX61tu', 'kyymytyxhpr'],
        'routineVisit': ['s6jpj7PK07u', 'fnBBGZhN67e', 'cnpi9Pp8CBl', 'Nq3oHg0fGVl', 'AGtLpLlS4h5', 'Czh8BvXx7w8', 'IPAqVQzuV3q', 'hOpp3nH7TCm', 'N8VF37JEOQz', 'ScCwG7tlcOM', 'mhq0Hm025Kn', 'fF4dYOFqQCs', 'BOd06N4seRE']
    }
    dataElementIds = {
        'suspectCancer': 'NGNkmwHNCwE',
        'viaScreening': 'BGsWghec94N',
        'suspectCancerViaScreening': 'q1lRGMdyYGD',
        'positiveVIA': 'DOkKxneLJ9q',
        'cryoThermal': 'sU5nvr2O0p6',
        'prevDelayedCryoThermal': 'njmiQgcpM71',
        'totalCryoThermal': 'ujgl9EvEZip',
        'cryoThermalDelayed': 'rFwBlzJqYM4',
        'ptComplication': 'yrGIgSDtA3s',
        'lesions': 'VXXbhsUFBEY'
    }
    
    listOfValues = indicatorList(location, month, connection_pool)
    #print(listOfValues)

    for indicator in listOfValues:
        dataElement = dataElementIds[indicator]
        for visitName in listOfValues[indicator]:
            valueList = listOfValues[indicator][visitName]
            for i in range(0, len(valueList)):
                categoryOptionCombo = categoryOptionCombos[visitName][i]
                dataElements.append({
                    "dataElement": dataElement,
                    "categoryOptionCombo": categoryOptionCombo,
                    "value": valueList[i]
                })

    return dataElements

# Get facility's dhis id
def getOrgUnitId(location, connection_pool):
    connection = connection_pool.get_connection()
    if connection.is_connected():
        cur = connection.cursor(dictionary = True)
        query = 'SELECT facility_dhis_ou_id FROM location_data_matvw WHERE facility_id = {}'.format(location)
        cur.execute(query)
        facilityDhisId = cur.fetchall()
        cur.close()
        connection.close()
    return facilityDhisId[0]['facility_dhis_ou_id']

def getFacilityIds(cursor):
    query = 'SELECT facility_name, facility_id, facility_dhis_ou_id FROM location_data_matvw WHERE facility_retired = 0 LIMIT 100'
    cursor.execute(query)
    facilityIds = cursor.fetchall()
    return facilityIds


#Get a formatted complte date for the report
def getCompleteDate(month):
    dateList = month.split('-')
    reportMonth = int(dateList[0])
    reportYear = int(dateList[1])
    if reportMonth == 12:
        completeMonth = 1
        completeYear = reportYear + 1
    else:
        completeMonth = reportMonth + 1
        completeYear = reportYear
    date = datetime.datetime(completeYear, completeMonth, 1)

    return '{}-{}-{}'.format(date.strftime('%Y'), date.strftime('%m'), date.strftime('%d'))

# Generate json payload for POST request
def generateJsonPayload(args):
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name = 'connection_pool',
            pool_size = 11,
            host = '34.240.241.171', 
            database = 'openmrs', 
            user = 'smartcerv', 
            password = 'smartcerv'
    )

    location = args[0]
    month = args[1]

    print('Process started')
    dataSetId = 'oIZPVojzsdH'
    # Get list of dictionary values
    dataElements = getDataElements(location, month, connection_pool)
    # Get facility's dhis id
    orgUnitId = getOrgUnitId(location, connection_pool)
    #Calculate the complete date of the report
    completeDate = getCompleteDate(month)
    #Format the period for the report
    formattedMonth = month.split('-')
    formattedPeriod = '{}-{}'.format(formattedMonth[1], formattedMonth[0])
    period = formattedPeriod[:7].replace("-","")
    return {
        "dataSet": dataSetId,
        "completeDate": completeDate,
        "period": period,
        "orgUnit": orgUnitId,
        "dataValues": dataElements
    }


# Main thread
if __name__ == '__main__':
    try:
        # Refresh the materialized views
        connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
        if connection.is_connected():
            cursor = connection.cursor(dictionary = True)
            cursor.execute('CALL RefreshMaterializedViews()')
            connection.commit()

        facilityIds = getFacilityIds(cursor)

        url = 'https://dhis.bluecodeltd.com/api/dataValueSets/'
        dhisCredentials = ('admin', 'district')
        start_time = round(time.time(), 4)
        month = '08-2019'
        facilityInfo = []
        facilities = []
        for facility in facilityIds:
            facilityInfo.append((facility['facility_id'], facility['facility_dhis_ou_id'], month))
            facilities.append(facility['facility_name'])

        

        with ProcessPoolExecutor(max_workers = 16) as executor:
            jsonPayload = dict(zip(facilities, executor.map(generateJsonPayload, facilityInfo)))

        #jsonPayload = generateJsonPayload(5314,'08-2019')
        print('jsonPayload: ', jsonPayload)
        duration = round(round(time.time(), 4) - start_time)
        print('Duration: ', duration, 's')
        #response = requests.post(url, auth = dhisCredentials, json = jsonPayload, headers = {"Content-Type":"application/json"})
        #print(response.json())
  
    except Error as e:
        print('An error occurred: ', e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        print('Database connection closed.')