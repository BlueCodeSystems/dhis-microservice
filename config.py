import os

#read environmental variables
POSTGRES_USER = str(os.environ['POSTGRES_USER'])
POSTGRES_PASS = str(os.environ['POSTGRES_PASS'])
POSTGRES_DB = str(os.environ['POSTGRES_DB'])
POSTGRES_HOST = str(os.environ['POSTGRES_HOST'])
DHIS_USER = str(os.environ['DHIS_USER'])
DHIS_PASS = str(os.environ['DHIS_PASS'])
DHIS_HOST = str(os.environ['DHIS_HOST'])