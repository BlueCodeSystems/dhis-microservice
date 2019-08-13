import mysql.connector
from mysql.connector import Error

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
        query = "SELECT visit_id FROM visit_data_matvw WHERE obs_concept_id = {} AND obs_value_concept_id = {} AND visit_type_id = {} AND visit_location_id = {} AND DATE_FORMAT(date_started, '%m-%Y') = '{}' AND visit_location_retired = 0".format(indicator['question'], indicator['answer'], visitTypeId, locationId, visitMonth)
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
        if (len(visitIds) > 1):
            query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id IN {}'.format(tuple(visitIds))
        else:
            query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id = {}'.format(visitIds[0])
        cursor.execute(query)
        patientIds = cursor.fetchall()
        result = []
        for visit in patientIds:
            result.append(visit['patient_id'])
        return sorted(result)

    # Filter patients by age
    def patientAges(patientIds, lowerLimit, upperLimit):
        if (len(patientIds) > 1):
            query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id IN {} AND TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) BETWEEN {} AND {}'.format(tuple(patientIds), lowerLimit, upperLimit)
        else:
            query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id = {} AND TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) BETWEEN {} AND {}'.format(patientIds[0], lowerLimit, upperLimit)
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





    #Suspect cancer - initial visit
    
    def indicatorRow(listOfIndicators, visitTypeId, visitLocationId, visitMonth):
        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165132}])
        print('Hiv unknown - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165131}])
        print('Hiv negative - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}])
        print('Hiv positive - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165127}])
        print('Not on ART - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - <= 24 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 0, 24)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - 25-29 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 25, 29)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - 30-34 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 30, 34)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - 35-39 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 35, 39)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - 40-49 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 40, 49)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - 50-59 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 50, 59)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Positive, on ART - >= 60 - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth, 60, 110)))

        concepts1 = listOfIndicators.copy()
        concepts1.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
        print('Total on ART - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

        concepts1 = listOfIndicators.copy()
        print('Total suspect cancer - {}'.format(patientCount(concepts1, visitTypeId, visitLocationId, visitMonth)))

    print('###############################')

    indicatorRow([{'question':165182, 'answer':165183}], 5, 5314, '08-2019')


except Error as e:
    print('Error while connecting to database: ', e)
finally:
    if connection.is_connected():
        connection.close()
        print('Database connection closed.')