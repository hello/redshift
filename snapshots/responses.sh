#!/bin/bash
psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
COPY responses FROM 's3://hello-db-exports/snapshots/responses.csv.gz' credentials 'aws_access_key_id=AKIAIYSCDJAGNVXVWN4Q;aws_secret_access_key=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' CSV GZIP;
EOF
