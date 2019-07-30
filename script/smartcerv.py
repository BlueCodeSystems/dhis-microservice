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

        #Patients who are not on ART and were referred for suspect cancer on initial visit by month and facility
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS Patients, DATE_FORMAT(a.date_started, '%m-%Y') AS date,a.visit_location_id, a.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165127) b ON a.visit_id = b.visit_id WHERE a.visit_type_id = 2 GROUP BY date, visit_location_id, visit_location_name")
        not_on_art_referred = cursor.fetchall()
        print(not_on_art_referred)

        #HIV+ - on ART <= 24
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS _24 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age <= 24) c)")
        on_art_24 = cursor.fetchall()
        print(on_art_24)

        #HIV+ - on ART 25-29
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 25_29 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age between 25 and 29) c)")
        on_art_25_29 = cursor.fetchall()
        print(on_art_25_29)

        #HIV+ - on ART 30-34
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 30_34 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age between 30 and 34) c)")
        on_art_30_34 = cursor.fetchall()
        print(on_art_30_34)

        #HIV+ - on ART 35-39
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 35_39 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age between 35 and 39) c)")
        on_art_35_39 = cursor.fetchall()
        print(on_art_35_39)

        #HIV+ - on ART 40-49
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 40_49 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age between 40 and 49) c)")
        on_art_40_49 = cursor.fetchall()
        print(on_art_40_49)

        #HIV+ - on ART 50-59
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 50_59 FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age between 50 and 59) c)")
        on_art_50_59 = cursor.fetchall()
        print(on_art_50_59)

        #HIV+ - on ART 60+
        cursor.execute("SELECT COUNT(DISTINCT a.patient_id) AS 60_ FROM (SELECT patient_id, visit_id FROM visit_data_vw WHERE obs_value_concept_id = 165183) a JOIN (SELECT patient_id, visit_id,visit_type_id,visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) b ON a.visit_id = b.visit_id WHERE b.visit_type_id=2 AND b.visit_location_retired=0 AND a.patient_id IN (SELECT patient_id FROM (SELECT patient_data_vw.patient_id, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw HAVING age >= 60) c)")
        on_art_60 = cursor.fetchall()
        print(on_art_60)

except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")