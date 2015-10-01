#!/bin/bash

## e.g.  ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
BLUE_BG='\033[0;44m'
NO_BG='\033[0;49m'

args=$#           # Number of args passed.
if [ $args -ne 3 ]
then
    echo "Migrate data from Postgres to S3"
    echo "Usage: ./get_count.sh [start_date] [end_date] [prefix]"
    echo
    exit
fi

# Set arguments
start_date=$1
end_date=$2
prefix=$3

# compute DB limit and offsets
echo "counting from $start_date to $end_date"
echo

# get data from DB
dbname="sensors1"
username="sensors"
# hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"
hostname="sensors-2-replica-1.cdawj8qazvva.us-east-1.rds.amazonaws.com"

db_query="\COPY (SELECT count(1) FROM tracker_motion_par_2015_08 WHERE local_utc_ts >= '${start_date}' AND local_utc_ts < '${end_date}') TO '${prefix}.rds_count' DELIMITER ',' CSV"

echo "-- Get data for $datafile from database $dbname"
printf "${BLUE}"
psql -h $hostname $dbname -U $username << EOF
 ${db_query}
EOF
printf "${NC}"
