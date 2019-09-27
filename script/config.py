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
