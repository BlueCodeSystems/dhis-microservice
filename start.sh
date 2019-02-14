#!/bash/sh

#enviromental variables

export POSTGRES_USER=zita
export POSTGRES_PASS=zita
export POSTGRES_DB=zita
export POSTGRES_HOST=127.0.0.1
python script.py
unset POSTGRES_USER
unset POSTGRES_PASS
unset POSTGRES_DB
unset POSTGRES_HOST
