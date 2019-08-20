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
        hivUnknown = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165131}])
        hivNegative = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}])
        hivPositive = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)        

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165127}])
        notOnArt = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onARTUnder24 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 0, 24)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_25_29 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 25, 29)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_30_34 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 30, 34)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_35_39 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 35, 39)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_40_49 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 40, 49)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onART_50_59 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 50, 59)

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        onARTOver60 = patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 60, 110)

        totalOnART = onARTUnder24 + onART_25_29 + onART_30_34 + onART_35_39 + onART_40_49 + onART_50_59 + onARTOver60

        total = hivUnknown + hivNegative + notOnArt + totalOnART

        countList = [hivUnknown, hivNegative, hivPositive, notOnArt, onARTUnder24, onART_25_29, onART_30_34, onART_35_39, onART_40_49, onART_50_59, onARTOver60, totalOnART, total]
        
        return countList

    # Get patient counts for each of the three visit types
    def indicatorRows(indicators, visitTypeIds, visitLocationId, visitMonth):
        # Initial Visit - visit_type_id = 2
        initialVisit = visitTypeFunc(indicators, visitTypeIds[0], visitLocationId, visitMonth)

        # One Year Follow-up - visit_type_id = 5
        oneYearFollowUp = visitTypeFunc(indicators, visitTypeIds[1], visitLocationId, visitMonth)

        # Routine Visit - visit_type_id = 6
        routineVisit = visitTypeFunc(indicators, visitTypeIds[2], visitLocationId, visitMonth)
        # return result as a dictionary
        result = {'initialVisit':initialVisit, 'oneYearFollowUp':oneYearFollowUp, 'routineVisit':routineVisit}
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
     


    #Function Calls to get counts for each of the indicators
    def indicatorList(location, month, index):
        result = []

        print('\nNumber of clients referred for suspect cancer')
        suspectCancer = indicatorRows([{'question':165182, 'answer':165183}], [2, 5, 6], location, month)
#        print(suspectCancer)
        result.append(suspectCancer)

        print('\nNumber of clients who received a VIA screening')
        viaScreening = indicatorRows([{'question':165155, 'answer':1}], [2, 5, 6], location, month)
#        print(viaScreening)
        result.append(viaScreening)

        print('\nTotal number of clients seen this month (1+2)')
        suspectCancerViaScreening = aggregate(suspectCancer, viaScreening)
#        print(suspectCancerViaScreening)
        result.append(suspectCancerViaScreening)
        
        print('\nNumber of clients with positive VIA result')
        positiveVIA = indicatorRows([{'question':165160, 'answer':165162}], [2, 5, 6], location, month)
#        print(positiveVIA)
        result.append(positiveVIA)
        
        print('\nNumber of VIA+ve clients with cryotherapy/thermal coagulation performed on the same day (single visit approach)')
        cryoThermal = indicatorRows([{'question':165219, 'answer':165174, 'answer1':165175}], [2, 5, 6], location, month)
#        print(cryoThermal)
        result.append(cryoThermal)
        
        print('\nNumber of clients with previously delayed cryotherapy/thermal coagulation performed this month')
        prevDelayedCryoThermal = indicatorRows([], [3, 3, 3], location, month)
#        print(prevDelayedCryoThermal['initialVisit'])
        result.append(prevDelayedCryoThermal['initialVisit'])
        
        print('\nTotal number of clients treated with cryotherapy/thermal coagulation (5+6)')
        listOfRows = [cryoThermal['initialVisit'], cryoThermal['oneYearFollowUp'], cryoThermal['routineVisit'], prevDelayedCryoThermal['initialVisit']]
        totalCryoThermal = sumRows(listOfRows)
#        print(totalCryoThermal)
        result.append(totalCryoThermal)
        
        print('\nNumber of VIA+ve clients with cryotherapy/thermal coagulation delayed')
        cryoThermalDelayed = indicatorRows([{'question':165219, 'answer':165176, 'answer1':165177}], [2, 5, 6], location, month)
#        print(cryoThermalDelayed)
        result.append(cryoThermalDelayed)
    
        print('\nNumber of clients with a post-treatment complication')
        ptComplication = indicatorRows([{'question':165143, 'answer':165144, 'answer1':165145, 'answer2':165146}], [2, 5, 6], location, month)
#        print(ptComplication)
        result.append(ptComplication)
        
        print('\nNumber of VIA+ve clients referred for lesions ineligible for cryotherapy or thermal coagulation (excluding suspect cancer)')
        lesions = indicatorRows([{'question':165182, 'answer':165184}], [2, 5, 6],  location, month)
#        print(lesions)
        result.append(lesions)

        valuesList = []
        for item in result:
            if (isinstance(item, dict)):
                for key in item.keys():
                    valuesList.append(item[key])
            else:
                valuesList.append(item)

        return valuesList[index[0]][index[1]]

 
    print('Suspect cancer initial visit hiv unknown - {}'.format(indicatorList(5314, '08-2019', (0, 0))))
    print('Via screening initial visit total - {}'.format(indicatorList(5314, '08-2019', (3, 12))))

    
except Error as e:
    print('Error while connecting to database: ', e)
finally:
    if connection.is_connected():
        connection.close()
        print('Database connection closed.')