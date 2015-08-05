#!/bin/bash

## e.g.  ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
BLUE_BG='\033[0;44m'
NO_BG='\033[0;49m'

args=$#           # Number of args passed.
if [ $args -ne 5 ]
then
    echo "Migrate data from Postgres to S3"
    echo "Usage: ./migrator.sh [DB-tablename] [datafile-prefix] [s3-bucket] [skip_db] [offset]"
    echo "skip_db = yes/no"
    echo "offset should integers like 1, 2, etc..."
    echo "Example ./test.sh device_sensors_par_2015_06 device_0 tracker_motion 0 no"
    echo
    exit
fi

# Set arguments
tablename=$1
prefix=$2	# tracker_motion_master or device_sensors_par_2015_06_?
s3_bucket=$3	# tracker_motion or device_sensors_2015_MM
offset_arg=$4
skip_db=$5

# compute DB limit and offsets
limit=100000000
offset=$((offset_arg*limit))  # 100 million

datafile="${prefix}.csv"

echo "table: $tablename, bucket: $s3_bucket, offset: $offset"
echo

# get data from DB
if [ "$skip_db" = 'no' ]; then
    dbname="sensors1"
    username="sensors"
    hostname="sensros-1-replica-2.cdawj8qazvva.us-east-1.rds.amazonaws.com"
    
    echo "-- Get data for $datafile from database $dbname"
    printf "${BLUE}"
    psql -h $hostname $dbname -U $username << EOF
    \COPY (SELECT * FROM ${tablename} LIMIT ${limit} OFFSET ${offset}) TO '${datafile}' DELIMITER ',' CSV
EOF
    printf "${NC}"
fi

echo

# split files into smaller chunks
splitfile="${prefix}-"
echo "-- Split $datafile into chunks $splitfile"
split_size=$((limit/10))
split -l $split_size $datafile $splitfile

echo

# compress all files
echo "-- Gzip files"
echo "    process $datafile"
gzip $datafile

for f in $splitfile*
do
    echo "    process file $f"
    gzip $f
done

echo
exit
# upload to S3
echo "-- Upload to S3 $s3_bucket"

for f in $prefix*.gz
do
    aws s3 cp $f s3://hello-db-exports/$s3_bucket/$f --region us-east-1

    s3="`aws s3 ls s3://hello-db-exports/$s3_bucket/$f`"
    s3_res=($s3)

    local="`ls -l $f`"
    loc_res=($local)

    result="${GREEN}okay${NC}"
    if [ "${s3_res[2]}" != "${loc_res[4]}" ]
    then
        result="${RED}**WRONG SIZE**${NC}"
    fi
    printf "$f -- s3 = ${s3_res[2]} -- local = ${loc_res[4]} -- ${result}\n\n"
done
