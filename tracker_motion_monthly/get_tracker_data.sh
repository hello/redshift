#!/bin/bash

## e.g.  ./test.sh device_sensors_par_2015_06 device_0 tracker_motion

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
    echo "Usage: ./get_tracker_data.sh [start-date] [end-date] [prefix]"
    echo "Example ./get_tracker_data.sh 2015-08-16 2015-08-17 tracker_motion_par_2015_08_16"
    echo
    exit
fi

# Set arguments
start_date=$1
end_date=$2
prefix=$3    # e.g. tracker_motion_master or device_sensors_par_2015_06_?
folder=$prefix

# compute DB limit and offsets

datafile="${prefix}.csv"

echo

# get data from DB
dbname="sensors1"
username="sensors"

# hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"
hostname="sensors-2-replica-1.cdawj8qazvva.us-east-1.rds.amazonaws.com"

copy_tracker_daily="\COPY (select *  FROM tracker_motion_par_2015_08 where local_utc_ts >= '${start_date}' and local_utc_ts < '${end_date}') TO '${folder}/${datafile}' DELIMITER ',' CSV"

echo "SQL command: ${copy_tracker_daily}"
echo "-- Get data for $datafile from database $dbname"
echo

printf "${BLUE}"
psql -h $hostname $dbname -U $username << EOF
 ${copy_tracker_daily}
EOF
printf "${NC}"

echo `wc -l ${folder}/${datafile} > ${prefix}.count`
