import os

#read environmental variables
OPENMRS_USER = str(os.environ['OPENMRS_USER'])
OPENMRS_PASS = str(os.environ['OPENMRS_PASS'])
OPENMRS_DB = str(os.environ['OPENMRS_DB'])
OPENMRS_HOST = str(os.environ['OPENMRS_HOST'])
DHIS2_USER = str(os.environ['DHIS2_USER'])
DHIS2_PASS = str(os.environ['DHIS2_PASS'])
DHIS2_HOST = str(os.environ['DHIS2_HOST'])
DHIS2_DATASET = str(os.environ['DHIS2_DATASET'])
DHIS2_DATA_VALUE_SET_REST_API_ENDPOINT = str(os.environ['DHIS2_DATA_VALUE_SET_REST_API_ENDPOINT'])