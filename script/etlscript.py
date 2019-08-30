import mysql.connector
from mysql.connector import Error
import variables

try:
    connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print('Connected to database ', db_Info)
        cursor = connection.cursor(dictionary=True)
        cursor.execute('CALL RefreshMaterializedViews()')

    # Function definitions

    #Get visit ids that satisfy a given indicator
    def visitList(indicator, visitTypeId, locationId, visitMonth): 
        strings = []
        for key in indicator:
            if (key != 'question'):
                if (165184 in indicator.values()):
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
        return sorted(result)

    #Get patients that satisfy all indicators
    def listIntersection(args):
        return sorted(list(set(args[0]).intersection(*args)))

    #Get patient ids from list of visit ids
    def patientList(visitIds):
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
        return sorted(result)

    # Filter patients by age
    def patientAges(patientIds, lowerLimit, upperLimit):
        if (len(patientIds) == 1):
            query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id = {} AND identifier_type_id = 4 AND age BETWEEN {} AND {}'.format(patientIds[0], lowerLimit, upperLimit)
        elif (len(patientIds) == 0):
            return 0
        else:
            query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id IN {} AND identifier_type_id = 4 AND age BETWEEN {} AND {}'.format(tuple(patientIds), lowerLimit, upperLimit)
        cursor.execute(query)
        patientsInRange = cursor.fetchall()
        result = []
        for visit in patientsInRange:
            result.append(visit['patient_id'])
        return result[0]

    # Aggregated function call
    def patientCount(questionAnswer, visitTypeId, locationId, visitMonth, lowerAgeLimit = -1, upperAgeLimit = -1):
        listOfVisits = []
        for questAns in questionAnswer:
            listOfVisits.append(visitList(questAns, visitTypeId, locationId, visitMonth))
        indicators = listIntersection(listOfVisits)
        patients = patientList(indicators)
        if (lowerAgeLimit == -1 or upperAgeLimit == -1):
            return len(patients)
        else: 
            return patientAges(patients, lowerAgeLimit, upperAgeLimit)
    
    #Get patient counts for a particular visit type
    def visitTypeFunc(listOfIndicators, visitTypeId, visitLocationId, visitMonth):
        
        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165132}])
        hivUnknown = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth), 'categoryOption': 'hivUnknown'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165131}])
        hivNegative = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth), 'categoryOption': 'hivNegative'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}])
        hivPositive = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth), 'categoryOption': 'hivPositive'}       

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165127}])
        notOnArt = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth), 'categoryOption': 'notOnART'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onARTUnder24 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 0, 24), 'categoryOption': 'onARTUnder24'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_25_29 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 25, 29), 'categoryOption': 'onART_25_29'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_30_34 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 30, 34), 'categoryOption': 'onART_30_34'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_35_39 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 35, 39), 'categoryOption': 'onART_35_39'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_40_49 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 40, 49), 'categoryOption': 'onART_40_49'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_50_59 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 50, 59), 'categoryOption': 'onART_50_59'}

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onARTOver60 = {'value': patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 60, 110), 'categoryOption': 'onARTOver60'}

        totalOnART = {'value': (onARTUnder24['value'] + onART_25_29['value'] + onART_30_34['value'] + onART_35_39['value'] + onART_40_49['value'] + onART_50_59['value'] + onARTOver60['value']), 'categoryOption': 'totalOnART'}

        total = {'value': (hivUnknown['value'] + hivNegative['value'] + notOnArt['value'] + totalOnART['value']), 'categoryOption': 'total'}

        countList = [hivUnknown, hivNegative, hivPositive, notOnArt, onARTUnder24, onART_25_29, onART_30_34, onART_35_39, onART_40_49, onART_50_59, onARTOver60, totalOnART, total]
        
        return countList

    # Get patient counts for each of the three visit types
    def indicatorRows(indicators, visitTypeIds, visitLocationId, visitMonth, dataElement):
        categoryOptionCombos = {
            'initial': {'hivUnknown': 'ZxsS9HGdhV1', 'hivNegative': 'AHS2fnqf971', 'hivPositive': 'VMMUi2HZOpS', 'notOnART': 'PqG5oFcHpLf', 'onARTUnder24': 'I5LyQqIyqX9', 'onART_25_29': 'eb9lrs8dq64', 'onART_30_34': 'NmBwhTMgYDK', 'onART_35_39': 'CNKn8YRApww', 'onART_40_49': 'kImwcR75U8J', 'onART_50_59': 'wJlF4hrj7IA', 'onARTOver60': 'ZDexPKuoua5', 'totalOnART': 'nc4NgEe5n91', 'total': 'mpAMSeHtHhc'},
            'oneYearFollowUp': {'hivUnknown': 'q7v6tWrRqF9', 'hivNegative': 'haZd47S1HFE', 'hivPositive': 'N1dUQRNIVLr', 'notOnART': 'qhI88d1Z0F9', 'onARTUnder24': 'ntHyHb6e0wx', 'onART_25_29': 'M9CKfD4v6lw', 'onART_30_34': 'lD5EVDDcNqQ', 'onART_35_39': 'ICJkUDl3DdT', 'onART_40_49': 'Uxb6dihTO4D', 'onART_50_59': 'ApDZ1vSzys3', 'onARTOver60': 'HCWBLRB75Q7', 'totalOnART': 'vMJjcIX61tu', 'total': 'kyymytyxhpr'},
            'routine': {'hivUnknown': 's6jpj7PK07u', 'hivNegative': 'fnBBGZhN67e', 'hivPositive': 'cnpi9Pp8CBl', 'notOnART': 'Nq3oHg0fGVl', 'onARTUnder24': 'AGtLpLlS4h5', 'onART_25_29': 'Czh8BvXx7w8', 'onART_30_34': 'IPAqVQzuV3q', 'onART_35_39': 'hOpp3nH7TCm', 'onART_40_49': 'N8VF37JEOQz', 'onART_50_59': 'ScCwG7tlcOM', 'onARTOver60': 'mhq0Hm025Kn', 'totalOnART': 'fF4dYOFqQCs', 'total': 'BOd06N4seRE'}
            }
        # Initial Visit - visit_type_id = 2
        initialVisit = visitTypeFunc(indicators, visitTypeIds[0], visitLocationId, visitMonth)
        #Assign categoryOptionCombo to each category option in initial visit
        for item in initialVisit:
            item['categoryOptionCombo'] = categoryOptionCombos['initial'][item['categoryOption']]
            item['dataElement'] = dataElement

        # One Year Follow-up - visit_type_id = 5
        oneYearFollowUp = visitTypeFunc(indicators, visitTypeIds[1], visitLocationId, visitMonth)
        #Assign categoryOptionCombo to each category option in initial visit
        for item in oneYearFollowUp:
            item['categoryOptionCombo'] = categoryOptionCombos['oneYearFollowUp'][item['categoryOption']]
            item['dataElement'] = dataElement
            
        # Routine Visit - visit_type_id = 6
        routineVisit = visitTypeFunc(indicators, visitTypeIds[2], visitLocationId, visitMonth)
        #Assign categoryOptionCombo to each category option in initial visit
        for item in routineVisit:
            item['categoryOptionCombo'] = categoryOptionCombos['routine'][item['categoryOption']]
            item['dataElement'] = dataElement

        # return result as a dictionary
        result = {'initialVisit': initialVisit, 'oneYearFollowUp': oneYearFollowUp, 'routineVisit': routineVisit}
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
     


    # Get counts for each of the indicators
    def indicatorList(location, month):
        result = []
        dataElementIds = {
            'suspectCancer': 'NGNkmwHNCwE',
            'viaScreening': 'BGsWghec94N',
            'suspectCancerViaScreening': '',
            'positiveVIA': 'DOkKxneLJ9q',
            'cryoThermal': 'sU5nvr2O0p6',
            'prevDelayedCryoThermal': 'njmiQgcpM71',
            'totalCryoThermal': 'ujgl9EvEZip',
            'cryoThermalDelayed': 'rFwBlzJqYM4',
            'ptComplication': 'yrGIgSDtA3s',
            'lesions': 'VXXbhsUFBEY'
            }
        #Number of clients referred for suspect cancer
        suspectCancer = indicatorRows([{'question':165182, 'answer':165183}], [2, 5, 6], location, month, dataElementIds['suspectCancer'])
        result.append(suspectCancer)

        #Number of clients who received a VIA screening
        viaScreening = indicatorRows([{'question':165155, 'answer':1}], [2, 5, 6], location, month, dataElementIds['viaScreening'])
        result.append(viaScreening)

        #Total number of clients seen this month (1+2)
        '''suspectCancerViaScreening = aggregate(suspectCancer, viaScreening)
        result.append(suspectCancerViaScreening)'''
        
        #Number of clients with positive VIA result
        positiveVIA = indicatorRows([{'question':165160, 'answer':165162}], [2, 5, 6], location, month, dataElementIds['positiveVIA'])
        result.append(positiveVIA)
        
        #Number of VIA+ve clients with cryotherapy/thermal coagulation performed on the same day (single visit approach)
        cryoThermal = indicatorRows([{'question':165219, 'answer':165174, 'answer1':165175}], [2, 5, 6], location, month, dataElementIds['cryoThermal'])
        result.append(cryoThermal)
        
        #Number of clients with previously delayed cryotherapy/thermal coagulation performed this month
        prevDelayedCryoThermal = indicatorRows([], [3, 3, 3], location, month, dataElementIds['prevDelayedCryoThermal'])
        result.append(prevDelayedCryoThermal['initialVisit'])
        
        #Total number of clients treated with cryotherapy/thermal coagulation (5+6)
        '''listOfRows = [cryoThermal['initialVisit'], cryoThermal['oneYearFollowUp'], cryoThermal['routineVisit'], prevDelayedCryoThermal['initialVisit']]
        totalCryoThermal = sumRows(listOfRows)
        result.append(totalCryoThermal)'''
        
        #Number of VIA+ve clients with cryotherapy/thermal coagulation delayed
        cryoThermalDelayed = indicatorRows([{'question':165219, 'answer':165176, 'answer1':165177}], [2, 5, 6], location, month, dataElementIds['cryoThermalDelayed'])
        result.append(cryoThermalDelayed)
    
        #Number of clients with a post-treatment complication
        ptComplication = indicatorRows([{'question':165143, 'answer':165144, 'answer1':165145, 'answer2':165146}], [2, 5, 6], location, month, dataElementIds['ptComplication'])
        result.append(ptComplication)
        
        #Number of VIA+ve clients referred for lesions ineligible for cryotherapy or thermal coagulation (excluding suspect cancer)')
        lesions = indicatorRows([{'question':165182, 'answer':165184}], [2, 5, 6],  location, month, dataElementIds['lesions'])
        result.append(lesions)

        valuesList = []
        for item in result:
            if (isinstance(item, dict)):
                for key in item.keys():
                    valuesList.append(item[key])
            else:
                valuesList.append(item)

        return valuesList

    # Create list of dictionary values
    def getDataElements(location, month):
        dataElements = []
        listOfValues = indicatorList(location, month)
        dataElements.append({
                "dataElement": "DOkKxneLJ9q",
                "categoryOptionCombo": "AHS2fnqf971",
                "value": 100
        })

        return dataElements

    # Get facility's dhis id
    def getOrgUnitId(location):
        query = 'SELECT facility_dhis_ou_id FROM location_data_matvw WHERE facility_id = {}'.format(location)
        cursor.execute(query)
        facilityDhisId = cursor.fetchall()
        return facilityDhisId[0]['facility_dhis_ou_id']

    # Generate json payload for POST request
    def generateJsonPayload(location, month):
        dataSetId = 'oIZPVojzsdH'

        # Get list of dictionary values
        dataElements = getDataElements(location, month)
        # Get facility's dhis id
        orgUnitId = getOrgUnitId(location)

        return {"dataSet": dataSetId,
                "completeDate": month,
                "period": month[:7].replace("-",""),
                "orgUnit": orgUnitId,
                "dataValues": dataElements}

    
    query = "SELECT facility_dhis_ou_id FROM location_data_matvw WHERE facility_id = {}".format(50)
    cursor.execute(query)
    facilityDhisId = cursor.fetchall()
    orgUnitId = facilityDhisId[0]['facility_dhis_ou_id']
    print(orgUnitId)
    print(indicatorList(5314,'08-2019'))

    #JSON payload
    '''{
        "dataSet": "oIZPVojzsdH",
        "completeDate": "2019-07-01",
        "period": "201907",
        "orgUnit": "k2SgIKwkSh1",
        "dataValues":[
            {
                "dataElement": "DOkKxneLJ9q",
                "categoryOptionCombo": "AHS2fnqf971",
                "value": 100
            }
        ]
    }'''
 
    
except Error as e:
    print('Error while connecting to database: ', e)
finally:
    if connection.is_connected():
        connection.close()
        print('Database connection closed.')