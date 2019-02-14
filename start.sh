#!/bash/sh

#enviromental variables

export POSTGRES_USER=zita
export POSTGRES_PASS=zita
export POSTGRES_DB=zita
export POSTGRES_HOST=127.0.0.1
export DHIS_USER=zita
export DHIS_PASS=zita
export DHIS_HOST=127.0.0.1
python script.py
unset POSTGRES_USER
unset POSTGRES_PASS
unset POSTGRES_DB
unset POSTGRES_HOST
unset DHIS_USER
unset DHIS_PASS
unset DHIS_HOST
