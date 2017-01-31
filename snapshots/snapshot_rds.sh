#!/bin/bash

if [[ -z "$1" ]] || [[ -z "$2" ]]
  then
    echo "[ERROR] ./copy_rds.sh [table_name] [gzip | nozip]"
    echo "valid tables: "
    echo "questions response_choices account_questions responses timeline_feedback"
    echo "account_device_map account_tracker_map"
    exit 1
fi

table_name=$1
gzipped=$2

TABLES=(questions response_choices account_questions responses timeline_feedback account_device_map account_tracker_map)

if [[ " ${TABLES[@]} " =~ " $1 " ]]; then
  echo "table $1 is valid"
else
  echo "[ERROR] table $1 is not valid"
  exit 1
fi

tmp_filename="/tmp/SNAPSHOTS/${table_name}.csv"
s3_filename="s3://hello-db-exports/snapshots/${table_name}.csv"

## copy specific columns for questions
if [ "$table_name" == "questions" ]; then
  rds_copy="\copy (SELECT id, parent_id, question_text, lang, frequency, response_type, dependency, ask_time, account_info, created, category FROM ${table_name} ORDER BY id) TO '${tmp_filename}' CSV"
else
  rds_copy="\COPY ${table_name} TO '${tmp_filename}' DELIMITER ',' CSV"
fi


########################
#### START Snapshot ####
########################

echo "migrating ${table_name} ${gzipped} to ${tmp_filename}"


#### download RDS table
echo "RDS Download: ${rds_copy}"
psql -h common-replica-1.cdawj8qazvva.us-east-1.rds.amazonaws.com -d common -U common << EOF
${rds_copy};
EOF

sleep 1

if [ "$gzipped" == "gzip" ]; then
  gzip ${tmp_filename}
  tmp_filename="${tmp_filename}.gz"
  s3_filename="${s3_filename}.gz"
fi


#### copy to S3, gzipped if needed
aws s3 cp ${tmp_filename} ${s3_filename}


#### DROP temp tables and re-create "new" table
echo "create temp tables"
psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
DROP TABLE ${table_name}_old;
EOF

sleep 1

psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
CREATE TABLE ${table_name}_new (like ${table_name});
EOF

sleep 1


#### copy data from S3 to redshift
AWS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
AWS_ACCESS_KEY=$AWS_ACCESS_KEY_ID

copy_to_redshift="COPY ${table_name}_new FROM '${s3_filename}' credentials 'aws_access_key_id=${AWS_ACCESS_KEY};aws_secret_access_key=${AWS_SECRET_KEY}' CSV"

echo "copying ${s3_filename} to redshift"

if [ "$gzipped" == "gzip" ]; then
  copy_to_redshift="${copy_to_redshift} GZIP"
fi

echo "REDSHIFT command: ${copy_to_redshift}"

psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
${copy_to_redshift};
EOF

sleep 1


# RENAME tables
echo "table gymnastics"
psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
 ALTER TABLE ${table_name} RENAME TO ${table_name}_old;
 ALTER TABLE ${table_name}_new RENAME TO ${table_name};
EOF

sleep 1


# RE-GRANT permissions
echo "enable permissions"
psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
 GRANT SELECT ON ${table_name} TO GROUP data;
 GRANT ALL ON ${table_name} TO GROUP ops;
EOF

#### remove temp file
echo "removing tmp file"
rm ${tmp_filename}
