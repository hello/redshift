"""copy data from DynamoDB to Redshift"""
import os
import sys
import json
import logging
import subprocess
import time

import arrow
import requests
import yaml

from boto.dynamodb2.table import Table

SLACK_URL = "https://hooks.slack.com/services/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"
SLACK_HEADERS = {'content-type': 'application/json'}

CAPACITY_CHANGE_WAIT_TIME = 12 # * 10secs
WAIT_TIME_CHUNK = 10
CAPACITY_CHANGE_MAX_MINUTES = 20

AWS_SECRET = os.getenv("aws_secret_access_key") # needed to access dynamo
AWS_ACCESS_KEY = os.getenv("aws_access_key_id")

REDSHIT_HOSTNAME = 'sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com'
REDSHIT_USER = 'migrator'
REDSHIT_DB = 'sensors1'
REDSHIT_PSQL = "psql -h %s -U %s -p 5439 -d %s" % (
    REDSHIT_HOSTNAME, REDSHIT_USER, REDSHIT_DB)

COPY_FIELDS = yaml.load(open("./ddb_copy_fields.yml"))

def update_read_throughput(table, updated_read_throughput, write_throughput):
    """Update throughputs on table"""
    res = table.update(
        throughput={'read': updated_read_throughput,
                    'write': write_throughput})

    print "table.update result = %s" % (res)

    new_reads = 0
    counts = 0
    waiting = 0
    while new_reads != updated_read_throughput:
        print "...... wait for read capacity change to %d ......"  % (
            updated_read_throughput)
        for i in range(CAPACITY_CHANGE_WAIT_TIME):
            time.sleep(WAIT_TIME_CHUNK)
            waiting += WAIT_TIME_CHUNK
            print "%d ...... %ds" % (i, waiting)

        new_desc = table.describe()
        new_reads = new_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits']
        counts += 1
        if counts > CAPACITY_CHANGE_MAX_MINUTES:
            print "waited more than %s minutes, something not right, abort" % (
                CAPACITY_CHANGE_MAX_MINUTES)
            return False

    return True

def post_slack(text):
    """Update Slack"""
    payload = {'text': text, 'channel': '#redshift', 'username': 'redshit_snapshots'}

    try:
        requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
    except requests.exceptions.RequestException as err:
        logging.error("Fail to send to Slack")
        logging.error(err)

def get_new_throughput(item_count):
    """get new throughputs based on current item count"""
    if item_count < 1000000:
        return 500
    elif item_count < 5000000:
        return 1000
    elif item_count < 50000000:
        return 2000
    else:
        return 5000

def rename_table(from_table_name, to_table_name, drop=False):
    """rename"""
    if drop:
        drop_table(to_table_name);

    rename_cmd = "%s -c \"ALTER TABLE %s RENAME TO %s \"" % (
        REDSHIT_PSQL, from_table_name, to_table_name)
    print "Step 3: rename %s table to %s" % (from_table_name, to_table_name)
    rename_out = subprocess.check_output(rename_cmd, shell=True)
    if "ERROR" in rename_out:
        print "RENAME table FAIL, bailing"
        return False
    return True

def grant_permissions(table_name):
    """grant"""
    if table_name == 'key_store':
        grant_cmd = "%s -c \"GRANT ALL ON %s TO GROUP ops\"" % (
            REDSHIT_PSQL, table_name, table_name)
    else:
        grant_cmd = "%s -c \"GRANT SELECT ON %s to tim, GROUP data; GRANT ALL ON %s TO GROUP ops\"" % (
            REDSHIT_PSQL, table_name, table_name)
    print "Step 5: Grant permissions"
    grant_out = subprocess.check_output(grant_cmd, shell=True)
    if "ERROR" in grant_out:
        print "GRANT error, bailing"
        return False
    return True

def drop_table(table_name):
    drop_cmd = "%s -c \"DROP TABLE %s \"" % (REDSHIT_PSQL, table_name)
    os.system(drop_cmd)

def create_table(new_table_name, table_name):
    """create"""
    # drop new_table_name if exist
    drop_table(new_table_name)

    create_cmd = "%s -c \"CREATE TABLE %s (LIKE %s) \"" % (REDSHIT_PSQL, new_table_name, table_name)
    print "Step 1: CREATE TABLE %s_new" % (table_name)
    create_new_out = subprocess.check_output(create_cmd, shell=True)
    if "ERROR" in create_new_out:
        print "CREATE TABLE error, bailing"
        return False
    return True

def extra_key_store_ops():
    drop_cmd = "%s -c \"DROP TABLE IF EXISTS key_store_admin_old\"" % (REDSHIT_PSQL)
    os.system(drop_cmd)

    drop_cmd = "%s -c \"DROP TABLE IF EXISTS key_store_admin_new\"" % (REDSHIT_PSQL)
    os.system(drop_cmd)
   
    create_cmd = "%s -c \"CREATE TABLE key_store_admin_new (like key_store_admin)\"" % (REDSHIT_PSQL)
    os.system(create_cmd)

    select_cmd = "%s -c \"INSERT INTO key_store_admin_new SELECT created_at, device_id, hw_version, metadata, note FROM key_store\"" % (REDSHIT_PSQL)
    os.system(select_cmd)

    alter_cmd = "%s -c \"ALTER TABLE key_store_admin RENAME TO key_store_admin_old\"" % (REDSHIT_PSQL)
    os.system(alter_cmd)
   
    alter_cmd = "%s -c \"ALTER TABLE key_store_admin_new RENAME TO key_store_admin\"" % (REDSHIT_PSQL)
    os.system(alter_cmd)

    grant_cmd = "%s -c \"GRANT SELECT ON key_store_admin to admin_tool\"" % (REDSHIT_PSQL)
    os.system(grant_cmd)

def main(table_name, skip_throughput=False):
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

    # update read capacity before copying
    if not skip_throughput:
        if table_name in ['key_store', 'pill_key_store']:
            copy_throughput = 2 * org_reads
        else:
            copy_throughput = get_new_throughput(item_count)
        print "Updating read throughput to %d for copying" % copy_throughput

        update_res = update_read_throughput(ddb_table, copy_throughput, org_writes)
        if not update_res:
            print "ERROR: reads not increased after X minutes"
            slack_text = "Fail to increase read capacity of DDB table %s" % table_name
            slack_text += "\nAborting table copy to redshift. cc @kingshy: :redshit:"
            post_slack(slack_text)
            sys.exit(1)

        print "extra 10 mins sleep for capacity change ...."
        time.sleep(900) # wait another 10 minutes

    read_ratio = 95
    if table_name in ['key_store', 'pill_key_store']:
        read_ratio = 60

    # PREVIOUSLY, truncate table
    # truncate_cmd = "%s -c \"TRUNCATE %s \"" % (REDSHIT_PSQL, table_name)
    # print "TRUNCATE current redshift table %s command:\n%s" % (table_name, truncate_cmd)
    # truncate_out = subprocess.check_output(truncate_cmd, shell=True)
    # if "ERROR" in truncate_out:
    #     print "Truncate error, bailing"
    #     sys.exit(1)
    #os.system(truncate_cmd)

    # table switcheroo
    # copy process
    # 1. create new_table (like original_table)
    # 3. rename original_table to old_table
    # 4. rename new_table to original_table
    # 5. re-grant permissions
    new_table_name = "%s_new" % table_name
    old_table_name = "%s_old" % table_name

    if create_table(new_table_name, table_name) is False:
        sys.exit(1)

    # 2. copy dynamodb data to new_table
    fields = ""
    if table_name in COPY_FIELDS and COPY_FIELDS[table_name] is not None:
        fields = "(%s)" % (','.join(field for field in COPY_FIELDS[table_name]))

    copy_command = """
    psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF
    COPY %s %s FROM 'dynamodb://%s' 
    credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' 
    READRATIO %d timeformat 'auto'
    """ % (new_table_name, fields, table_name, AWS_ACCESS_KEY, AWS_SECRET, read_ratio)

    print "Step 2: Copy command: %s" % copy_command
    os.system(copy_command)

    time.sleep(60)

    # 3. rename original_table to old_table
    if rename_table(table_name, old_table_name, drop=True) is False:
        sys.exit()

    # 4. rename new_table to original_table
    if rename_table(new_table_name, table_name) is False:
        sys.exit()

    # 5. re-grant permissions
    if grant_permissions(table_name) is False:
        sys.exit()

    # for key_store, create new table w/o aes
    if table_name == 'key_store':
        extra_key_store_ops()

    # change table throughput to original values
    if not skip_throughput:
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
    if len(sys.argv) < 3:
        print "Usage: python snapshot_ddb.py [dynamodb_table] [skip_throughput 1=yes/0=no]"
        print "Available tables:"
        for name in COPY_FIELDS:
            print "\t%s" % name
        print
        sys.exit()

    DDB_TABLE = sys.argv[1]
    if DDB_TABLE not in COPY_FIELDS:
        print "ERROR: %s table is not in config file" % DDB_TABLE
        sys.exit()

    SKIP_INCREASE_THROUGHPUT = False
    if len(sys.argv) == 3:
        SKIP_INCREASE_THROUGHPUT = bool(int(sys.argv[2]))
    main(DDB_TABLE, SKIP_INCREASE_THROUGHPUT)
