#!/bin/bash

#set script to execute
export SCRIPT=etlscript.py

#set enviromental variables
export OPENMRS_USER=openmrs
export OPENMRS_PASS=XXXXXXXXXXXXXXXXXXX
export OPENMRS_DB=db_name
export OPENMRS_HOST=127.0.0.1
export DHIS2_USER=dhis_username
export DHIS2_PASS=XXXXXXXXXXXXXX
export DHIS2_HOST=https://dhis.example.org
export DHIS2_DATASET=oIZPVojzsdH
export DHIS2_DATA_VALUE_SET_REST_API_ENDPOINT=/api/dataValueSets
export REPORT_MONTH=`date +%m-%Y`
export LOG_FILE=./logs

if [ ! -e $LOG_FILE ]; then
    touch $LOG_FILE
fi

exec 3>&1 1>>${LOG_FILE} 2>&1

env/bin/python3.7 script/$SCRIPT $REPORT_MONTH

unset OPENMRS_USER
unset OPENMRS_PASS
unset OPENMRS_DB
unset OPENMRS_HOST
unset DHIS2_USER
unset DHIS2_PASS
unset DHIS2_HOST
unset SCRIPT