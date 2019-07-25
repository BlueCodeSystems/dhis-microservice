import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to database ", db_Info)

        cursor = connection.cursor(dictionary=True)

        #Patients who are HIV positive - Initial visit
        cursor.execute("SELECT COUNT(DISTINCT patient.patient_id) AS HIV_Positive FROM patient JOIN visit ON patient.patient_id = visit.patient_id JOIN encounter ON encounter.visit_id = visit.visit_id JOIN obs ON encounter.encounter_id = obs.encounter_id WHERE patient.voided = 0 AND visit_type_id = 2 AND encounter_type = 10 AND obs.concept_id = 165203 AND obs.value_coded = 165125")
        hiv_positive = cursor.fetchall()
        print(hiv_positive)

        print("------------------------------------------------")

        #Patients who are HIV negative - Initial visit
        cursor.execute("SELECT COUNT(DISTINCT patient.patient_id) AS HIV_Negative FROM patient JOIN visit ON patient.patient_id = visit.patient_id JOIN encounter ON encounter.visit_id = visit.visit_id JOIN obs ON encounter.encounter_id = obs.encounter_id WHERE patient.voided = 0 AND visit_type_id = 2 AND encounter_type = 10 AND obs.concept_id = 165203 AND obs.value_coded = 165131")
        hiv_negative = cursor.fetchall()
        print(hiv_negative)

        print("------------------------------------------------")

        #Patients with Unknown HIV status - Initial visit
        cursor.execute("SELECT COUNT(DISTINCT patient.patient_id) AS HIV_Unknown FROM patient JOIN visit ON patient.patient_id = visit.patient_id JOIN encounter ON encounter.visit_id = visit.visit_id JOIN obs ON encounter.encounter_id = obs.encounter_id WHERE patient.voided = 0 AND visit_type_id = 2 AND encounter_type = 10 AND obs.concept_id = 165203 AND obs.value_coded = 165132")
        hiv_unknown = cursor.fetchall()
        print(hiv_unknown)

        print("------------------------------------------------")

        #Patients who are not on ART - Initial visit
        cursor.execute("SELECT COUNT(DISTINCT patient.patient_id) AS Not_on_ART FROM patient JOIN visit ON patient.patient_id = visit.patient_id JOIN encounter ON encounter.visit_id = visit.visit_id JOIN obs ON encounter.encounter_id = obs.encounter_id WHERE patient.voided = 0 AND visit_type_id = 2 AND encounter_type = 10 AND obs.concept_id = 165223 AND obs.value_coded = 165127;")
        not_on_art = cursor.fetchall()
        print(not_on_art)

        print("------------------------------------------------")

        #Patients who are not on ART and were referred for suspect cancer on initial visit
        cursor.execute("SELECT a.patient_id, a.visit_id FROM (SELECT * from patient_visit WHERE obs_value = 'Suspect Cancer') a JOIN (SELECT * from patient_visit WHERE obs_value = 'Not on ART') b on a.visit_id = b.visit_id where a.visit_type_id = 2;")
        not_on_art_referred = cursor.fetchall()
        print(not_on_art_referred)

except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")