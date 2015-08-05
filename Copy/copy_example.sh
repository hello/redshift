#!/bin/bash

psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U <redshift-username> -p 5439 -d sensors1 << EOF
copy device_sensors_master from 's3://<bucket>/device_sensors_2015_02/device_sensors_par_2015_02.manifest' credentials 'aws_access_key_id=<access-key-id>;aws_secret_access_key=<secret-access-key' delimiter ',' gzip manifest
EOF
