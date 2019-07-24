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
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS referred_not_on_art FROM (SELECT patient.patient_id FROM patient JOIN visit ON patient.patient_id = visit.patient_id JOIN encounter ON encounter.visit_id = visit.visit_id JOIN obs ON encounter.encounter_id = obs.encounter_id WHERE patient.voided = 0 AND visit_type_id = 2 AND encounter_type = 10 AND obs.concept_id = 165223 AND obs.value_coded = 165127) AS not_on_art JOIN obs ON patient_id = obs.person_id WHERE concept_id = 165182 AND value_coded = 165183;")
        not_on_art_referred = cursor.fetchall()
        print(not_on_art_referred)

        #Proof
        cursor.execute("SELECT cancer.visit_id from (select visit_intial_hiv_referred.visit_id, encounter.encounter_id, obs_id, value_coded from encounter JOIN (select a.visit_id from (select visit.* from visit join encounter on visit.visit_id = encounter.visit_id where encounter_type = 10) a join (select  visit.visit_id from visit join encounter on visit.visit_id = encounter.visit_id where encounter_type = 14) b on a.visit_id = b.visit_id where a.visit_type_id = 2) visit_intial_hiv_referred on encounter.visit_id = visit_intial_hiv_referred.visit_id JOIN obs on obs.encounter_id = encounter.encounter_id WHERE value_coded = 165127) not_art join (select visit_intial_hiv_referred.visit_id, encounter.encounter_id, obs_id, value_coded from encounter JOIN select a.visit_id from (select visit.* from visit join encounter on visit.visit_id = encounter.visit_id where encounter_type = 10) a join (select  visit.visit_id from visit join encounter on visit.visit_id = encounter.visit_id where encounter_type = 14) b on a.visit_id = b.visit_id where a.visit_type_id = 2) visit_intial_hiv_referred on encounter.visit_id = visit_intial_hiv_referred.visit_id JOIN obs on obs.encounter_id = encounter.encounter_id WHERE value_coded = 165183) cancer on not_art.visit_id = cancer.visit_id")
        proof = cursor.fetchall()
        print(proof)

except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")