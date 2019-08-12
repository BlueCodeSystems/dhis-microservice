import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to database ", db_Info)
        cursor = connection.cursor(dictionary=True)

    # Function definitions
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



    # Function calls

    #No. of patients referred for suspect cancer on one year follow-up visit with Unknown HIV status
    print("No. of patients referred for suspect cancer on one year follow-up visit with Unknown HIV status")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165132}]
    print(patientCount(concepts, 5, 5314, '08-2019'))

    #No. of patients referred for suspect cancer on one year follow-up visit and are HIV negative
    print("No. of patients referred for suspect cancer on one year follow-up visit and are HIV negative")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165131}]
    print(patientCount(concepts, 5, 5314, '08-2019'))    

    #No. of patients referred for suspect cancer on one year follow-up visit and are HIV positive
    print("No. of patients referred for suspect cancer on one year follow-up visit and are HIV positive")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}]
    print(patientCount(concepts, 5, 5314, '08-2019'))

    #No. of patients referred for suspect cancer on one year follow-up visit and are not on ART
    print("No. of patients referred for suspect cancer on one year follow-up visit and are not on ART")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165127}]
    print(patientCount(concepts, 5, 5314, '08-2019'))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age <= 24
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age <= 24")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 0, 24))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 25-29
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 25-29")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 25, 29))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 30-34
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 30-34")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 30, 34))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 40-49
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 40-49")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 40, 49))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 50-59
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age between 50-59")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 50, 59))

    #No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age >= 60
    print("No. of patients referred for suspect cancer on one year follow-up visit who are HIV +ve, on ART and age >= 60")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019', 60, 110))

    #Total No. of patients referred for suspect cancer on one year follow-up visit who are on ART
    print("Total No. of patients referred for suspect cancer on one year follow-up visit who are on ART")
    concepts = [{"question":165182, "answer":165183}, {"question":165203, "answer":165125}, {"question":165223, "answer":165126}]
    print(patientCount(concepts, 5, 5314, '08-2019'))

    #Total No. of patients referred for suspect cancer on one year follow-up visit
    print("Total No. of patients referred for suspect cancer on one year follow-up visit")
    concepts = [{"question":165182, "answer":165183}]
    print(patientCount(concepts, 5, 5314, '08-2019'))


except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")