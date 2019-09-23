import os

#read environmental variables
POSTGRES_USER = str(os.environ['POSTGRES_USER'])
POSTGRES_PASS = str(os.environ['POSTGRES_PASS'])
POSTGRES_DB = str(os.environ['POSTGRES_DB'])
POSTGRES_HOST = str(os.environ['POSTGRES_HOST'])
DHIS_USER = str(os.environ['DHIS_USER'])
DHIS_PASS = str(os.environ['DHIS_PASS'])
DHIS_HOST = str(os.environ['DHIS_HOST'])
DHIS_DATASET = str(os.environ['DHIS_DATASET'])
DHIS_DATASET_REST_API_ENDPOINT = str(os.environ['DHIS_DATASET_REST_API_ENDPOINT'])
DHIS_DATA_VALUE_SET_REST_API_ENDPOINT = str(os.environ['DHIS_DATA_VALUE_SET_REST_API_ENDPOINT'])

OPENMRS_USER = str(os.environ['OPENMRS_USER'])
OPENMRS_PASS = str(os.environ['OPENMRS_PASS'])
OPENMRS_DB = str(os.environ['OPENMRS_DB'])
OPENMRS_HOST = str(os.environ['OPENMRS_HOST'])
DHIS2_USER = str(os.environ['DHIS2_USER'])
DHIS2_PASS = str(os.environ['DHIS2_PASS'])
DHIS2_HOST = str(os.environ['DHIS2_HOST'])
DHIS2_DATASET = str(os.environ['DHIS2_DATASET'])
DHIS2_DATASET_REST_API_ENDPOINT = str(os.environ['DHIS2_DATASET_REST_API_ENDPOINT'])
