#!/usr/bin/env bash

HOST='jdbc:hive2://localhost:10000'

function beeline-cli() {
  # Call beeline cli with execute argument
  beeline --autoCommit=true --fastConnect=true -u ${1} --outputformat=csv2 --showHeader=falase -e "${@:1}" 2>/dev/null
}

echo "[--] Working on MySQL references [--]"
for location in `mysql -N -B -u root hive -e "select DB_LOCATION_URI from DBS"`; do
  if [[ ${location} =~ .*${1}/.* ]]; then
    echo -e "\t${location}... OK"
  else
    echo "\tCorrecting ${location} to point ${location/$2/$1}... "
    mysql -u root hive -e "UPDATE DBS set DB_LOCATION_URI=\"${location/$2/$1}\" where DB_LOCATION_URI=\"${location}\"" && echo OK || echo ERROR
  fi
done

echo "[--] Working on Hive Metastore [--]"
for db in $(beeline-cli ${HOST} 'show databases'); do
  echo "[*] Analysing DB: $db"
  for table in $(beeline-cli ${HOST}/${db} 'show tables'); do
    LOCATION=$(beeline-cli ${HOST}/${db} "show create table ${table}" | grep LOCATION -A 1 | tail -n 1)
    if [[ ${LOCATION} =~ .*${1}/.* ]]; then
      echo -e "\t ${db}.${table}... OK"
    else
      NEW_LOCATION=${LOCATION/$2/$1}
      echo -n -e "\tUpdating ${LOCATION}\n\tto point ${NEW_LOCATION}... "
      beeline-cli ${HOST}/${db} "alter table ${table} set location ${NEW_LOCATION}"
      echo "OK"
    fi
  done
done

