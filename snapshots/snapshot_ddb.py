import os
import sys
import logging
import time

import arrow
import json
import requests
import yaml

from boto import dynamodb2
from boto.dynamodb2.table import Table

SLACK_URL = "https://hooks.slack.com/services/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"
SLACK_HEADERS = {'content-type': 'application/json'}

CAPACITY_CHANGE_WAIT_TIME = 6; # * 10secs
WAIT_TIME_CHUNK = 10
CAPACITY_CHANGE_MAX_MINUTES = 10
COPY_READ_THROUGHPUTS = 500
COPY_READ_RATIO = 95 # use 95% of original reads

REDSHIT_HOSTNAME = 'sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com'
REDSHIT_USER = 'migrator'
REDSHIT_DB = 'sensors1'
REDSHIT_PSQL = "psql -h %s -U %s -p 5439 -d %s" % (
    REDSHIT_HOSTNAME, REDSHIT_USER, REDSHIT_DB)

COPY_FIELDS = yaml.load(open("./ddb_copy_fields.yml"))

def update_read_throughput(table, updated_read_throughput, write_throughput):
    res = table.update(throughput = 
        {'read': updated_read_throughput,
        'write': write_throughput})
    print "table.update result = %s" % (res)

    new_reads = 0
    counts = 0
    waiting = 0
    while new_reads != updated_read_throughput:
        print "...... wait for read capacity change to %d ......"  % updated_read_throughput
        for i in range(CAPACITY_CHANGE_WAIT_TIME):
            time.sleep(WAIT_TIME_CHUNK)
            waiting += WAIT_TIME_CHUNK
            print "...... %ds" % waiting

        new_desc = table.describe()
        new_reads = new_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits']
        counts += 1
        if counts > CAPACITY_CHANGE_MAX_MINUTES:
            print "waited more than %s minutes, something not right, abort" % (
                CAPACITY_CHANGE_MAX_MINUTES)
            return False

    return True

def post_slack(text):
    payload = {'text': text,
        'channel': '#research',
        'username': 'redshit_snapshots'}

    try:
        requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
    except requests.exceptions.RequestException as err:
        logging.error("Fail to send to Slack")
        logging.error(err)


def main(table_name):
    """main method"""

    local_now = arrow.utcnow().to('US/Pacific').format('YYYY-MM-DD HH:mm')
    print "start: %s" % local_now

    ddb_table = Table(table_name)
    print "Copying DynamoDB table %s to redshit" % table_name


    # get orignal capacity
    table_desc = ddb_table.describe()
    org_reads = table_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits']
    org_writes = table_desc['Table']['ProvisionedThroughput']['WriteCapacityUnits']
    item_count = table_desc['Table']['ItemCount']

    print "table=%s item_count=%d org_reads=%d org_writes=%d" % (
        table_name, item_count, org_reads, org_writes)
    print "Updating read throughput to %d for copying" % COPY_READ_THROUGHPUTS


    # update read capacity before copying
    item_count = table_desc['Table']['ItemCount']
    if item_count < 1000000:
        copy_throughput = 500
    elif item_count < 5000000:
        copy_throughput = 1000
    elif item_count < 50000000:
        copy_throughput = 2000
    else:
        copy_throughput = 5000

    update_res = update_read_throughput(ddb_table, copy_throughput, org_writes)
    if not update_res:
        print "ERROR: reads not increased after X minutes"
        slack_text = "Fail to increase read capacity of DDB table %s" % table_name
        slack_text += "\nAborting table copy to redshift. cc @kingshy: :redshit:"
        post_slack(slack_text)
        sys.exit(1)

    if copy_throughput >= 2000:
        time.sleep(180) # 2 mins
    else:
        time.sleep(60)

    # truncate table
    truncate_cmd = "%s -c \"TRUNCATE %s \"" % (REDSHIT_PSQL, table_name)
    print "TRUNCATE current redshift table %s command:\n%s" % (table_name, truncate_cmd)
    os.system(truncate_cmd)


    # start copying
    aws_secret = os.getenv("aws_secret_access_key") # needed to access dynamo
    aws_access_key = os.getenv("aws_access_key_id")
    fields = ""
    if table_name in COPY_FIELDS and COPY_FIELDS[table_name] is not None:
        fields = "(%s)" % (','.join(field for field in COPY_FIELDS[table_name]))

    copy_command = """
    psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
    COPY %s %s 
    FROM 'dynamodb://%s' 
    credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' 
    READRATIO 95 timeformat 'auto'
    """ % (table_name, fields, table_name, aws_access_key, aws_secret)

    print "Copy command: %s" % copy_command
    os.system(copy_command)


    # change table throughput to original values
    update_res = update_read_throughput(ddb_table, org_reads, org_writes)
    if not update_res:
        print "ERROR: reads not decreased after 5 minutes"
        slack_text = "Fail to reduce read capacity to original value for DDB table %s" % table_name
        slack_text += "\nAborting table copy to redshift cc @kingshy: :redshit:"
        post_slack(slack_text)
        sys.exit(1)


    local_now = arrow.utcnow().to('US/Pacific').format('YYYY-MM-DD HH:mm')
    print "end: %s" % local_now
    slack_text = "%s DDB table %s snapshot success :boom:" % (
        local_now, table_name)
    post_slack(slack_text)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: python snapshot_ddb.py [dynamodb_table]"
        print "Available tables:"
        for table_name in COPY_FIELDS:
            print "\t%s" % table_name
        print
        sys.exit()

    ddb_table_name = sys.argv[1]
    if ddb_table_name not in COPY_FIELDS:
        print "ERROR: %s table is not in config file" % ddb_table_name
        sys.exit()

    main(ddb_table_name)
