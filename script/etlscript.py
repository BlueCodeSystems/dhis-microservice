import smartcerv_config
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import json
import sys
import requests
import multiprocessing
from multiprocessing import Manager
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Function definitions

#Get visit ids that satisfy a given indicator
def visit_list(indicator, visit_type_id, location_id, visit_month, connection):
    cursor = connection.cursor(dictionary = True)
    strings = []
    parameters = [indicator['question']]
    lesions_concept_id = 165184
    suspect_cancer = 165183
    for key in indicator:
        if (key != 'question'):
            if (lesions_concept_id in indicator.values()):
                strings.append('obs_value_concept_id != %s')
                parameters.append(suspect_cancer)
            else:
                strings.append('obs_value_concept_id = %s')
                parameters.append(indicator[key])
    answers = ' OR '.join(strings)
    query = "SELECT visit_id FROM visit_data_matvw WHERE obs_concept_id = %s AND ({}) AND visit_type_id = %s AND visit_location_id = %s AND DATE_FORMAT(date_started, '%m-%Y') = %s AND visit_location_retired = 0".format(answers)
    parameters.extend([visit_type_id, location_id, visit_month])
    cursor.execute(query, parameters)
    visit_idsList = cursor.fetchall()
    cursor.close()
    result = []
    for visit in visit_idsList:
        result.append(visit['visit_id'])
    return result

#Get patients that satisfy all indicators
def list_intersection(args):
    return list(set(args[0]).intersection(*args))

#Get patient ids from list of visit ids
def patient_list(visit_ids, connection):
    parameters = []
    visit_ids_placeholders = '%s'
    if (len(visit_ids) == 0):
        return []
    else:
        for _ in range(1, len(visit_ids)):
            visit_ids_placeholders = '{}, {}'.format(visit_ids_placeholders, '%s')
        query = 'SELECT DISTINCT patient_id FROM visit_data_matvw WHERE visit_id IN ({})'.format(visit_ids_placeholders)
        parameters.extend(visit_ids)
    cursor = connection.cursor(dictionary = True)
    cursor.execute(query, parameters)
    patient_ids = cursor.fetchall()
    cursor.close()
    result = []
    for visit in patient_ids:
        result.append(visit['patient_id'])
    return result

# Filter patients by age
def patients_in_age_range(patient_ids, connection, lower_age_limit, upper_age_limit):
    parameters = []
    patient_ids_placeholders = '%s'
    if (len(patient_ids) == 0):
        return 0
    else:
        for _ in range(1, len(patient_ids)):
            patient_ids_placeholders = '{}, {}'.format(patient_ids_placeholders, '%s')
        query = 'SELECT COUNT(DISTINCT patient_id) AS patient_id FROM patient_data_matvw WHERE patient_id IN ({}) AND identifier_type_id = 4 AND age BETWEEN %s AND %s'.format(patient_ids_placeholders)
        parameters.extend(patient_ids)
    cursor = connection.cursor(dictionary = True)
    parameters.extend([lower_age_limit, upper_age_limit])
    cursor.execute(query, parameters)
    patients_in_range = cursor.fetchall()
    cursor.close()
    result = []
    for visit in patients_in_range:
        result.append(visit['patient_id'])
    return result[0]

# Aggregated function call
def patient_count(indicator_concept_ids, visit_type_id, location_id, visit_month, connection, lower_age_limit = -1, upper_age_limit = -1):
    list_of_visits = []
    for indicator_concept_id in indicator_concept_ids:
        list_of_visits.append(visit_list(indicator_concept_id, visit_type_id, location_id, visit_month, connection))
    indicators = list_intersection(list_of_visits)
    patients = patient_list(indicators, connection)
    if (lower_age_limit == -1 or upper_age_limit == -1):
        return len(patients)
    else: 
        result = patients_in_age_range(patients, connection, lower_age_limit, upper_age_limit)
        return result

#Get patient counts for a particular visit type
def visit_type_func(indicator_concept_ids, visit_type_id, visit_location_id, visit_month, connection):
    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165132}])
    hiv_unknown = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165131}])
    hiv_negative = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}])
    hiv_positive = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection)       

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165127}])
    not_on_art = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_under_24 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 0, 24)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_25_29 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 25, 29)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_30_34 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 30, 34)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_35_39 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 35, 39)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_40_49 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 40, 49)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_50_59 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 50, 59)

    concepts = indicator_concept_ids.copy()
    concepts.extend([{'question':165203, 'answer':165125}, {'question':165223, 'answer':165126}])
    on_art_over_60 = patient_count(concepts, visit_type_id, visit_location_id, visit_month, connection, 60, 110)

    total_on_art = on_art_under_24 + on_art_25_29 + on_art_30_34 + on_art_35_39 + on_art_40_49 + on_art_50_59 + on_art_over_60

    total = hiv_unknown + hiv_negative + not_on_art + total_on_art

    patient_counts = [hiv_unknown, hiv_negative, hiv_positive, not_on_art, on_art_under_24, on_art_25_29, on_art_30_34, on_art_35_39, on_art_40_49, on_art_50_59, on_art_over_60, total_on_art, total]
    
    return patient_counts

# Get patient counts for each of the three visit types
def indicator_rows(args):
    indicators = args[0]
    visit_type_ids = args[1]
    visit_location_id = args[2]
    visit_month = args[3]
    connection_pool = args[4]
    connection_list = args[5]
    connection = connection_pool.get_connection()
    print(multiprocessing.current_process().name,', Connection: ', connection.connection_id, 'opened.')
    connection_list.append(connection.connection_id)
    print('\nCurrent connections: ', len(connection_list))
    # Initial Visit - visit_type_id = 2
    initial_visit = visit_type_func(indicators, visit_type_ids[0], visit_location_id, visit_month, connection)

    # One Year Follow-up - visit_type_id = 5
    one_year_followup = visit_type_func(indicators, visit_type_ids[1], visit_location_id, visit_month, connection)

    # Routine Visit - visit_type_id = 6
    routine_visit = visit_type_func(indicators, visit_type_ids[2], visit_location_id, visit_month, connection)

    # return result as a dictionary
    result = {'initial_visit': initial_visit, 'one_year_followup': one_year_followup, 'routine_visit': routine_visit}
    print(multiprocessing.current_process().name,', Connection: ', connection.connection_id, 'closed.')
    connection_list.remove(connection.connection_id)
    print('\nCurrent connections: ', len(connection_list))
    connection.close()

    return result

# Get counts for each of the indicators
def indicator_list(location, month, connection_pool, connection_list):
    result = {}
    indicators = [
        ([{'question':165182, 'answer':165183}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([{'question':165155, 'answer':1}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([{'question':165160, 'answer':165162}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([{'question':165219, 'answer':165174, 'answer1':165175}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([], [3, 3, 3], location, month, connection_pool, connection_list),
        ([{'question':165219, 'answer':165176, 'answer1':165177}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([{'question':165143, 'answer':165144, 'answer1':165145, 'answer2':165146}], [2, 5, 6], location, month, connection_pool, connection_list),
        ([{'question':165182, 'answer':165184}], [2, 5, 6],  location, month, connection_pool, connection_list)
    ]

    '''with ThreadPoolExecutor(max_workers = 1) as executor:            
        indicator_names = ('suspect_cancer', 'via_screening', 'positive_via', 'cryo_thermal', 'prev_delayed_cryo_thermal', 'delayed_cryo_thermal', 'post_treatment_complication', 'lesions')
        result = dict(zip(indicator_names, executor.map(indicator_rows, indicators)))'''

    indicator_names = ('suspect_cancer', 'via_screening', 'positive_via', 'cryo_thermal', 'prev_delayed_cryo_thermal', 'delayed_cryo_thermal', 'post_treatment_complication', 'lesions')
    result = dict(zip(indicator_names, map(indicator_rows, indicators)))

    suspect_cancer = result['suspect_cancer']
    via_screening = result['via_screening']
    suspect_cancer_via_screening = aggregate_indicators(suspect_cancer, via_screening)
    result['suspect_cancer_via_screening'] = suspect_cancer_via_screening
    
    del result['prev_delayed_cryo_thermal']['one_year_followup']
    del result['prev_delayed_cryo_thermal']['routine_visit']
    
    #Total number of clients treated with cryotherapy/thermal coagulation (5+6)
    list_of_rows = [result['cryo_thermal']['initial_visit'], result['cryo_thermal']['one_year_followup'], result['cryo_thermal']['routine_visit'], result['prev_delayed_cryo_thermal']['initial_visit']]
    total_cryo_thermal = {}
    total_cryo_thermal['initial_visit'] = sum_of_rows(list_of_rows)
    result['total_cryo_thermal'] = total_cryo_thermal
        
    return result

# Aggregate two dictionaries
def aggregate_indicators(dict1, dict2):
    result = {}
    for key in dict1:
        lists = [dict1[key], dict2[key]]
        sumOfLists = [sum(x) for x in zip(*lists)]
        result[key] = sumOfLists
    return result

# Sum up the corresponding values in several lists
def sum_of_rows(lists):
    result = [sum(x) for x in zip(*lists)]
    return result

# Create list of dictionary values
def get_data_elements(location, month, connection_pool, facility_name, connection_list):
    data_elements = []
    category_option_combos = {
        'initial_visit': ['ZxsS9HGdhV1', 'AHS2fnqf971', 'VMMUi2HZOpS', 'PqG5oFcHpLf', 'I5LyQqIyqX9', 'eb9lrs8dq64', 'NmBwhTMgYDK', 'CNKn8YRApww', 'kImwcR75U8J', 'wJlF4hrj7IA', 'ZDexPKuoua5', 'nc4NgEe5n91', 'mpAMSeHtHhc'],
        'one_year_followup': ['q7v6tWrRqF9', 'haZd47S1HFE', 'N1dUQRNIVLr', 'qhI88d1Z0F9', 'ntHyHb6e0wx', 'M9CKfD4v6lw', 'lD5EVDDcNqQ', 'ICJkUDl3DdT', 'Uxb6dihTO4D', 'ApDZ1vSzys3', 'HCWBLRB75Q7', 'vMJjcIX61tu', 'kyymytyxhpr'],
        'routine_visit': ['s6jpj7PK07u', 'fnBBGZhN67e', 'cnpi9Pp8CBl', 'Nq3oHg0fGVl', 'AGtLpLlS4h5', 'Czh8BvXx7w8', 'IPAqVQzuV3q', 'hOpp3nH7TCm', 'N8VF37JEOQz', 'ScCwG7tlcOM', 'mhq0Hm025Kn', 'fF4dYOFqQCs', 'BOd06N4seRE']
    }
    data_element_ids = {
        'suspect_cancer': 'NGNkmwHNCwE',
        'via_screening': 'BGsWghec94N',
        'suspect_cancer_via_screening': 'q1lRGMdyYGD',
        'positive_via': 'DOkKxneLJ9q',
        'cryo_thermal': 'sU5nvr2O0p6',
        'prev_delayed_cryo_thermal': 'njmiQgcpM71',
        'total_cryo_thermal': 'ujgl9EvEZip',
        'delayed_cryo_thermal': 'rFwBlzJqYM4',
        'post_treatment_complication': 'yrGIgSDtA3s',
        'lesions': 'VXXbhsUFBEY'
    }
    
    indicator_values = indicator_list(location, month, connection_pool, connection_list)
    #print(facility_name, ' : ', indicator_values)
    for indicator in indicator_values:
        data_element = data_element_ids[indicator]
        for visit_type in indicator_values[indicator]:
            value_list = indicator_values[indicator][visit_type]
            for i in range(0, len(value_list)):
                category_option_combo = category_option_combos[visit_type][i]
                data_elements.append({
                    "dataElement": data_element,
                    "categoryOptionCombo": category_option_combo,
                    "value": value_list[i]
                })
    return data_elements

# Get facility information
def get_facility_ids(cursor):
    query = 'SELECT facility_name, facility_id, facility_dhis_ou_id FROM location_data_matvw WHERE facility_retired = 0 AND facility_dhis_ou_id IS NOT NULL'
    cursor.execute(query)
    facility_ids = cursor.fetchall()
    return facility_ids

#Get formatted complete date and period for the report
def get_formatted_dates(month):
    #Format the complete date
    dateList = month.split('-')
    report_month = int(dateList[0])
    report_year = int(dateList[1])
    if report_month == 12:
        complete_month = 1
        complete_year = report_year + 1
    else:
        complete_month = report_month + 1
        complete_year = report_year
    date = datetime.datetime(complete_year, complete_month, 1)
    complete_date = '{}-{}-{}'.format(date.strftime('%Y'), date.strftime('%m'), date.strftime('%d'))
    #Format the period
    formatted_month = month.split('-')
    formatted_period = '{}-{}'.format(formatted_month[1], formatted_month[0])
    period = formatted_period[:7].replace("-","")
    result = {'period': period, 'complete_date': complete_date}
    return result

# Generate json payload for POST request
def generate_json_payload(args):
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name = 'connection_pool',
            pool_size = 5,
            host = smartcerv_config.OPENMRS_HOST, 
            database = smartcerv_config.OPENMRS_DB, 
            user = smartcerv_config.OPENMRS_USER, 
            password = smartcerv_config.OPENMRS_PASS
    )

    #multiprocessing.current_process().name = 'smartcerv_process'
    print('\n', multiprocessing.current_process().name, 'started.\n')

    location = args[0]
    org_unit_id = args[1]
    facility_name = args[2]
    month = args[3]
    url = args[4]
    dhis_credentials = args[5]
    connection_list = args[6]
    data_set_id = smartcerv_config.DHIS2_DATASET
    data_elements = get_data_elements(location, month, connection_pool, facility_name, connection_list)
    dates = get_formatted_dates(month)
    period = dates['period']
    complete_date = dates['complete_date']
    json_payload = {
        "dataSet": data_set_id,
        "completeDate": complete_date,
        "period": period,
        "orgUnit": org_unit_id,
        "dataValues": data_elements
    }
    
    #POST to dhis api
    response = requests.post(url, auth = dhis_credentials, json = json_payload, headers = {"Content-Type":"application/json"})
    #print(facility_name, ' : ', response.json())
    print('\n', multiprocessing.current_process().name, 'terminated.\n')
    return response

# Main thread
def main():
    try:
        #log time
        print('\n\nScript started: ['+datetime.datetime.now().strftime('%c')+']')
        start_time = round(time.time(), 4)
        # Refresh the materialized views
        manager = multiprocessing.Manager()
        connection_list = manager.list()
        connection = mysql.connector.connect(host = smartcerv_config.OPENMRS_HOST, database = smartcerv_config.OPENMRS_DB, user = smartcerv_config.OPENMRS_USER, password = smartcerv_config.OPENMRS_PASS)
        if connection.is_connected():
            cursor = connection.cursor(dictionary = True)
            cursor.execute('CALL RefreshMaterializedViews()')
            connection.commit()
        
        url = smartcerv_config.DHIS2_HOST+smartcerv_config.DHIS2_DATA_VALUE_SET_REST_API_ENDPOINT   
        dhis_credentials = (smartcerv_config.DHIS2_USER, smartcerv_config.DHIS2_PASS)
        month = sys.argv[1]
        facility_ids = get_facility_ids(cursor)
        facility_info = []
        facilities = []

        for facility in facility_ids:
            facility_info.append((facility['facility_id'], facility['facility_dhis_ou_id'], facility['facility_name'], month, url, dhis_credentials, connection_list))
            facilities.append(facility['facility_name'])

        with ProcessPoolExecutor(max_workers = 1) as executor:
            responses = dict(zip(facilities, executor.map(generate_json_payload, facility_info)))

        print('Connections: ', len(connection_list))
        
        duration = round(round(time.time(), 4) - start_time)
        print('Script completed: ['+datetime.datetime.now().strftime('%c')+'] in', duration, 's')
  
    except Error as e:
        print('An error occurred: ', e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        print('Database connection closed.')

if __name__ == '__main__': main()