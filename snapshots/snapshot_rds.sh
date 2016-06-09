#!/bin/bash

if [[ -z "$1" ]] || [[ -z "$2" ]]
  then
    echo "[ERROR] ./copy_rds.sh [table_name] [gzip | nozip]"
    echo "valid tables: questions response_choices account_questions responses timeline_feedback"
    exit 1
fi

table_name=$1
gzipped=$2

TABLES=(questions response_choices account_questions responses timeline_feedback)

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

sleep 1

#### truncate redshift table
psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
TRUNCATE ${table_name};
EOF

sleep 2

#### copy data from S3 to redshift
AWS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
AWS_ACCESS_KEY=$AWS_ACCESS_KEY_ID

copy_to_redshift="COPY ${table_name} FROM '${s3_filename}' credentials 'aws_access_key_id=${AWS_ACCESS_KEY};aws_secret_access_key=${AWS_SECRET_KEY}' CSV"

if [ "$gzipped" == "gzip" ]; then
  copy_to_redshift="${copy_to_redshift} GZIP"
fi

echo "REDSHIFT command: ${copy_to_redshift}"

psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
${copy_to_redshift};
EOF


#### remove temp file
rm ${tmp_filename}
