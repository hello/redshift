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
    echo "Usage: ./get_data [DB-tablename] [datafile-prefix]"
    echo "skip_db = yes/no"
    echo "offset should integers like 1, 2, etc..."
    echo "Example ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no"
    echo
    exit
fi

# Set arguments
tablename=$1
prefix=$2    # e.g. tracker_motion_master or device_sensors_par_2015_06_?
folder=$3

# compute DB limit and offsets

datafile="${prefix}.csv"

echo "table: $tablename, datafile: $datafile, offset: $offset"
echo

# get data from DB
dbname="sensors1"
username="sensors"

# hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"
hostname="sensors-2-replica-1.cdawj8qazvva.us-east-1.rds.amazonaws.com"

copy_whole_table="\COPY ${tablename} TO '${folder}/${datafile}' DELIMITER ',' CSV"

echo "SQL command: ${copy_whole_table}"
echo "-- Get data for $datafile from database $dbname"

printf "${BLUE}"
psql -h $hostname $dbname -U $username << EOF
 ${copy_whole_table}
EOF
printf "${NC}"

echo `wc -l ${folder}/${datafile} > ${tablename}.count`
