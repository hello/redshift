#!/bin/bash

## e.g.  ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
BLUE_BG='\033[0;44m'
NO_BG='\033[0;49m'

args=$#           # Number of args passed.
if [ $args -ne 1 ]
then
    echo "Get row count of a table"
    echo "Usage: ./get_count.sh [DB-tablename]"
    echo
    exit
fi

# Set arguments
tablename=$1

# compute DB limit and offsets
echo "table: $tablename"
echo

# get data from DB
dbname="sensors1"
username="sensors"
# hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"
hostname="sensors-2-replica-1.cdawj8qazvva.us-east-1.rds.amazonaws.com"

db_query="\COPY (SELECT count(1) FROM ${tablename}) TO '${tablename}.rds_count' DELIMITER ',' CSV"

echo "-- Get row count for table $tablename"
printf "${BLUE}"
psql -h $hostname $dbname -U $username -W<< EOF
 ${db_query}
EOF
printf "${NC}"
