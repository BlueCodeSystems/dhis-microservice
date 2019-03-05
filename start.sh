#!/bash/sh

#set script to execute
export SCRIPT=script.py

#set enviromental variables
export POSTGRES_USER=postgres
export POSTGRES_PASS=password
export POSTGRES_DB=opensrp
export POSTGRES_HOST=127.0.0.1
export DHIS_USER=admin
export DHIS_PASS=pass
export DHIS_HOST=https://dhis.zeir-stage.smartregister.org

python script/$SCRIPT `date +%Y-%m`

unset POSTGRES_USER
unset POSTGRES_PASS
unset POSTGRES_DB
unset POSTGRES_HOST
unset DHIS_USER
unset DHIS_PASS
unset DHIS_HOST
unset SCRIPT
