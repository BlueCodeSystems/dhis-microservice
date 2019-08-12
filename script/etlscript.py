import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to database ", db_Info)
        cursor = connection.cursor(dictionary=True)

    #Get visit ids that satisfy a given indicator
    def visitList(indicator, visitTypeId, locationId, visitMonth):
        query = "SELECT visit_id FROM visit_data_matvw WHERE obs_concept_id = {} AND obs_value_concept_id = {} AND visit_type_id = {} AND visit_location_id = {} AND DATE_FORMAT(date_started, '%m-%Y') = '{}' AND visit_location_retired = 0".format(indicator["question"], indicator["answer"], visitTypeId, locationId, visitMonth)
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
        query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id IN {}'.format(tuple(visitIds))
        cursor.execute(query)
        patientIds = cursor.fetchall()
        result = []
        for visit in patientIds:
            result.append(visit['patient_id'])
        return sorted(result)

    # Filter patients by age
    def patientAges(patientIds, lowerLimit, upperLimit):
        query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id IN {} AND TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) BETWEEN {} AND {}'.format(tuple(patientIds), lowerLimit, upperLimit)
        cursor.execute(query)
        patientsInRange = cursor.fetchall()
        result = []
        for visit in patientsInRange:
            result.append(visit['patient_id'])
        return sorted(result)

    # Aggregated function call
    def patientCount(concepts, visitTypeId, locationId, visitMonth, lowerAgeLimit = -1, upperAgeLimit = -1):
        listOfVisits = []
        for questAns in concepts:
            listOfVisits.append(visitList(questAns, visitTypeId, locationId, visitMonth))
        indicators = listIntersection(listOfVisits)
        patients = patientList(indicators)
        if (lowerAgeLimit == -1 or upperAgeLimit == -1):
            return len(patients)
        else: 
            return patientAges(patientList(indicators), lowerAgeLimit, upperAgeLimit)



    visits1 = visitList({"question":165182, "answer":165183}, 5, 5314, '08-2019')
#    print(visits1)
#    print("-------------------------------------------------------------------")

    visits2 = visitList({"question":165203, "answer":165125}, 5, 5314, '08-2019')
#    print(visits2)
#    print("-------------------------------------------------------------------")

    visits3 = visitList({"question":165223, "answer":165126}, 5, 5314, '08-2019')
#    print(visits3)
    print("---------------------Indicators---------------------------")

    indicators = listIntersection([visits1, visits2, visits3])
#    print(indicators)

    print("------------------------Total patients on ART----------------------------")
    patients = patientList(indicators)
    print(patients)

    print("######################################################################")

    if isinstance(patients, list):
        print("----------------HIV +ve clients on art in age range --------------")
        ages = patientAges(patientList(indicators), 0, 24)
        print(ages)
    
    print("Patients referred for suspcect cancer on initial visit and HIV unknown")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019'))


except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")