#!/bin/bash

#set script to execute
export SCRIPT=script.py

#set enviromental variables
export POSTGRES_USER=postgres
export POSTGRES_PASS=XXXXXXXXXXXXXXXXXXX
export POSTGRES_DB=db_name
export POSTGRES_HOST=127.0.0.1
export DHIS_USER=dhis_username
export DHIS_PASS=XXXXXXXXXXXXXX
export DHIS_HOST=https://dhis.example.org
export DHIS_DATASET=KD5kmrkn5
export DHIS_DATASET_REST_API_ENDPOINT=/api/26/dataSets
export DHIS_DATA_VALUE_SET_REST_API_ENDPOINT=/api/26/dataValueSets
export REPORT_MONTH=`date +%Y-%m`
export LOG_FILE=./logs

if [ ! -e $LOG_FILE ]; then
    touch $LOG_FILE
fi

exec 3>&1 1>>${LOG_FILE} 2>&1

env/bin/python3.7 script/$SCRIPT $REPORT_MONTH

unset POSTGRES_USER
unset POSTGRES_PASS
unset POSTGRES_DB
unset POSTGRES_HOST
unset DHIS_USER
unset DHIS_PASS
unset DHIS_HOST
unset SCRIPT
