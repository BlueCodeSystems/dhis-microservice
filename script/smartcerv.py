import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host = '34.240.241.171', database = 'openmrs', user = 'smartcerv', password = 'smartcerv')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to database ", db_Info)

        cursor = connection.cursor(dictionary=True)

        #No. of patients referred for suspect cancer on initial visit and are HIV positive
        cursor.execute("SELECT COUNT(DISTINCT referral_suspect_cancer.patient_id) AS no_of_patients, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month, referral_suspect_cancer.visit_location_id, referral_suspect_cancer.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id WHERE referral_suspect_cancer.visit_type_id = 2 AND referral_suspect_cancer.visit_location_retired = 0 GROUP BY visit_month, visit_location_id, visit_location_name;")
        hiv_positive = cursor.fetchall()
        print(hiv_positive)

        print("------------------------------------------------")

        #No. of patients referred for suspect cancer on initial visit and are HIV negative
        cursor.execute("SELECT COUNT(DISTINCT referral_suspect_cancer.patient_id) AS no_of_patients, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month, referral_suspect_cancer.visit_location_id, referral_suspect_cancer.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165131) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id WHERE referral_suspect_cancer.visit_type_id = 2 AND referral_suspect_cancer.visit_location_retired = 0 GROUP BY visit_month, visit_location_id, visit_location_name;")
        hiv_negative = cursor.fetchall()
        print(hiv_negative)

        print("------------------------------------------------")

        #No. of patients referred for suspect cancer on initial visit with Unknown HIV status
        cursor.execute("SELECT COUNT(DISTINCT referral_suspect_cancer.patient_id) AS no_of_patients, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month, referral_suspect_cancer.visit_location_id, referral_suspect_cancer.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165132) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id WHERE referral_suspect_cancer.visit_type_id = 2 AND referral_suspect_cancer.visit_location_retired = 0 GROUP BY visit_month, visit_location_id, visit_location_name;")
        hiv_unknown = cursor.fetchall()
        print(hiv_unknown)

        print("------------------------------------------------")

        #No. of patients referred for suspect cancer on initial visit and are not on ART
        cursor.execute("SELECT COUNT(DISTINCT referral_suspect_cancer.patient_id) AS no_of_patients, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month, referral_suspect_cancer.visit_location_id, referral_suspect_cancer.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165127) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE referral_suspect_cancer.visit_type_id = 2 AND referral_suspect_cancer.visit_location_retired = 0 GROUP BY visit_month, visit_location_id, visit_location_name;")
        not_on_art = cursor.fetchall()
        print(not_on_art)

        print("------------------------------------------------")

        #No. of patients referred for suspect cancer on initial visit who are HIV +ve, on ART and age <= 24
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (SELECT referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age <= 24) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_24 = cursor.fetchall()
        print(on_art_24)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and age between 25-29
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age BETWEEN 25 AND 29) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_25_29 = cursor.fetchall()
        print(on_art_25_29)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and  age between 30-34
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age BETWEEN 30 AND 34) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_30_34 = cursor.fetchall()
        print(on_art_30_34)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and  age between 35-39
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age BETWEEN 35 AND 39) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_35_39 = cursor.fetchall()
        print(on_art_35_39)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and  age between 40-49
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age BETWEEN 40 AND 49) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_40_49 = cursor.fetchall()
        print(on_art_40_49)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and  age between 50-59
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age BETWEEN 50 AND 59) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_50_59 = cursor.fetchall()
        print(on_art_50_59)

        print("------------------------------------------------")

        #No. of patients who are HIV +ve, on ART and  age >= 60
        cursor.execute("SELECT COUNT(DISTINCT patient_id) AS no_of_patients, visit_location_id, visit_location_name, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month FROM (SELECT patient_info.patient_id, visit_info.visit_location_id, visit_info.visit_location_name, visit_info.date_started, TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age FROM patient_data_vw patient_info JOIN (select referral_suspect_cancer.patient_id, referral_suspect_cancer.date_started, visit_location_name, visit_location_id FROM (SELECT patient_id, visit_id, visit_data_vw.date_started, visit_location_id, visit_location_name FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT patient_id, visit_id, visit_type_id, visit_location_retired FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE not_on_art.visit_type_id = 2 AND not_on_art.visit_location_retired = 0) visit_info ON patient_info.patient_id = visit_info.patient_id HAVING age >= 60) patient_visit_info GROUP BY visit_month, visit_location_id")
        on_art_60 = cursor.fetchall()
        print(on_art_60)

        print("------------------------------------------------")

        #Total no. of patients who are on ART
        cursor.execute("SELECT COUNT(DISTINCT referral_suspect_cancer.patient_id) AS no_of_patients, DATE_FORMAT(referral_suspect_cancer.date_started, '%m-%Y') AS visit_month, referral_suspect_cancer.visit_location_id, referral_suspect_cancer.visit_location_name FROM (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165183) referral_suspect_cancer JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165126) not_on_art ON referral_suspect_cancer.visit_id = not_on_art.visit_id JOIN (SELECT * FROM visit_data_vw WHERE obs_value_concept_id = 165125) hiv_positive ON referral_suspect_cancer.visit_id = hiv_positive.visit_id WHERE referral_suspect_cancer.visit_type_id = 2 AND referral_suspect_cancer.visit_location_retired = 0 GROUP BY visit_month, visit_location_id, visit_location_name")
        total_on_art = cursor.fetchall()
        print(total_on_art)
             
except Error as e:
    print("Error while connecting to database: ", e)
finally:
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")