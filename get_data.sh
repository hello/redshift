#!/bin/bash

## e.g.  ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
BLUE_BG='\033[0;44m'
NO_BG='\033[0;49m'

args=$#           # Number of args passed.
if [ $args -ne 4 ]
then
    echo "Migrate data from Postgres to S3"
    echo "Usage: ./get_data [DB-tablename] [datafile-prefix] [offset]"
    echo "skip_db = yes/no"
    echo "offset should integers like 1, 2, etc..."
    echo "Example ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no"
    echo
    exit
fi

# Set arguments
tablename=$1
prefix=$2	# tracker_motion_master or device_sensors_par_2015_06_?
folder=$3
offset_arg=$4

# compute DB limit and offsets
limit=100000000
offset=$((offset_arg*limit))  # 100 million

datafile="${prefix}.csv"

echo "table: $tablename, datafile: $datafile, offset: $offset"
echo

# get data from DB
dbname="sensors1"
username="sensors"
hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"

echo "-- Get data for $datafile from database $dbname"
printf "${BLUE}"
psql -h $hostname $dbname -U $username << EOF
\COPY (SELECT * FROM ${tablename} LIMIT ${limit} OFFSET ${offset}) TO '${folder}/${datafile}' DELIMITER ',' CSV
EOF
printf "${NC}"
