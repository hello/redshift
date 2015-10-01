"""
script to migrate sensors1 database from RDS to Redshift
"""

import os
import sys

import arrow
from datetime import datetime
import time

import json
import hashlib
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto').setLevel(logging.ERROR)

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import boto
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item

REDSHIFT_HOST = 'sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com'
REDSHIFT_PSQL = "psql -h %s -U migrator -p 5439 -d sensors1" % (REDSHIFT_HOST)
DYNAMODB_TABLE = 'redshift_log' # logs
S3_MAIN_BUCKET = "hello-db-exports" # S3 bucket to store gzipped files
MAX_LINES = 100000000
CHUNK_SIZE = 1000000 # testing  5000000 # lines

ERROR_NOT_FOUND = 'File in S3 bucket, but not in local drive'
ERROR_CHECKSUM = "Checksum do not match"

# terminal colors
GREEN = '\033[92m'
ENDC = '\033[0m'
BLUE = '\033[94m'
BOLD = '\033[1m'


def main(args):
    logging.debug("\n\nStart %s", str(datetime.now()))

    if not os.getenv("aws_secret_access_key") or \
        not os.getenv("aws_access_key_id"):
        logging.error("Please set credentials and password env")
        return

    table_prefix = args[1]
    year = args[2]
    month = args[3]
    day = args[4]
    get_db = args[5]
    do_split = args[6]

    yy_mm = "%s_%s" % (year, month)
    date = "%s_%s_%s" % (year, month, day)

    table_name = "%s_%s" % (table_prefix, date) # eg device_sensors_par_2015_08_01
    redshift_table = "%s_%s" % (table_prefix, yy_mm)
    prefix = "%s_%s" % (table_prefix, date) 
    folder = "%s_%s" % (table_prefix, date)

    # set s3 bucket name
    if "device" in table_prefix:
        bucket_name = "device_sensors_%s/%s" % (yy_mm, date)
    else:
        bucket_name = "tracker_motion_%s/%s" % (yy_mm, date)

    # create data folder if not exist
    if not os.path.isdir(folder):
        logging.debug("Creating folder for data: %s", folder)
        os.makedirs(folder)


    # 1. Get data from DB
    datafile = "%s/%s.csv" % (folder, prefix)
    if get_db == 'yes':
        logging.debug("\n\nGetting data from RDS table %s", table_name)
        start_ts = int(time.time())

        # get_data.sh
        start_date = "%s-%s-%s" % (year, month, day)
        end_date = arrow.get(start_date).replace(days=1).format('YYYY-MM-DD')
        if 'device' in table_prefix:
            os.system("./get_data.sh %s %s %s" % (table_name, prefix, folder))
        else:
            os.system("./get_tracker_data.sh %s %s %s" % (start_date, end_date, prefix))

        lapse = int(time.time()) - start_ts
        logging.debug("get data time taken: %d", lapse)

        line_count = 0
        with open("%s.count" % table_name) as fp:
            lines = fp.read().split("\n")
            line_count = int(lines[0].split()[0])

        if line_count == 0:
            logging.error("no data retrieved from RDS")
            sys.exit()

        logging.debug("\nGet table size from RDS")
        start_ts = int(time.time())

        # get_count.sh
        if 'device' in table_prefix:
            os.system("./get_count.sh %s" % (table_name))
        else:
            os.system("./get_tracker_count.sh %s %s %s" % (start_date, end_date, prefix))

        lapse = int(time.time()) - start_ts
        logging.debug("get count time taken: %d", lapse)

        # check counts
        logging.debug("check table size")
        lines = open("%s.rds_count" % table_name).read().split("\n")
        rds_line_count = int(lines[0].split()[0])

        if rds_line_count != line_count:
            logging.error("Downloaded file %s has insufficient data")
            sys.exit()
        else:
            logging.debug("line counts matches %d", line_count)


    # make sure data file exists
    gzip_datafile = "%s.csv.gz" % prefix
    logging.debug("\n\nCheck if datafile %s, %s exist",
                datafile, gzip_datafile)
    if not os.path.isfile(datafile) and not \
        os.path.isfile("%s/%s" % (folder, gzip_datafile)):
        logging.error("Data not downloaded to %s", datafile)
        logging.debug("exiting....")
        sys.exit()


    # 2. split data into smaller chunks
    logging.debug("\n\nSplit data into files of size %s", CHUNK_SIZE)
    start_ts = int(time.time())
    file_info = {}
    num_lines = 0
    if do_split == 'yes':
        num = 0
        with open(datafile) as filep:
            added = 0
            for i, line in enumerate(filep):
                if added == 0:
                    splitfile = "%s-0%d" % (prefix, num)
                    if num < 10:
                        splitfile = "%s-000%d" % (prefix, num)
                    elif num < 100:
                        splitfile = "%s-00%d" % (prefix, num)
                    elif num < 1000:
                        splitfile = "%s-00%d" % (prefix, num)

                    split_fp = open("%s/%s" % (folder, splitfile), 'w')

                split_fp.write(line)
                num_lines += 1
                added += 1

                if added >= CHUNK_SIZE:
                    logging.debug("split file %s: %d", splitfile, added)
                    split_fp.close()
                    file_info.setdefault(splitfile + ".gz", {})
                    file_info[splitfile + ".gz"]['size'] = added
                    file_info[splitfile + ".gz"]['created'] = datetime.strftime(
                            datetime.now(), "%Y-%m-%d %H:%M:%S")
                    added = 0
                    num += 1

        split_fp.close()
        logging.debug("split file %s: %d", splitfile, added)
        file_info.setdefault(splitfile + ".gz", {})
        file_info[splitfile + ".gz"]['size'] = added
        file_info[splitfile + ".gz"]['created'] = datetime.strftime(
                datetime.now(), "%Y-%m-%d %H:%M:%S")

    file_info[gzip_datafile] = {}
    file_info[gzip_datafile]['size'] = num_lines
    file_info[gzip_datafile]['created'] = datetime.strftime(
            datetime.now(), "%Y-%m-%d %H:%M:%S")

    lapse = int(time.time()) - start_ts
    logging.debug("split file time taken: %d", lapse)


    # 3. upload to S3
    logging.debug("\n\nPreparing to upload to S3 %s/%s/%s",
                S3_MAIN_BUCKET, bucket_name, date)
    start_ts = int(time.time())

    onlyfiles = [ f for f in os.listdir(folder)
                if os.path.isfile(os.path.join(folder,f)) ]

    aws_key = os.getenv("aws_access_key_id")
    aws_secret = os.getenv("aws_secret_access_key")

    conn = S3Connection(aws_key, aws_secret)
    bucket = conn.get_bucket(S3_MAIN_BUCKET)

    uploaded = bucket.list(bucket_name)
    up_files = []
    for k in uploaded: up_files.append(k.key)


    for org_filename in onlyfiles:
        if '.gz' not in org_filename:
            cmd = "pigz %s/%s" % (folder, org_filename)
            logging.debug(cmd)

            os.system(cmd)

            filename = "%s.gz" % org_filename
        else:
            filename = org_filename

        logging.debug("---- Uploading file %s", filename)
        k = Key(bucket)
        k.key = "%s/%s" % (bucket_name, filename)

        # get md5 checksum for verification
        os.system("md5sum %s/%s > md5info" % (folder, filename))
        file_md5 = open("md5info", 'r').read().split('  ')[0]
        logging.debug("md5sum = %s", file_md5)

        file_info.setdefault(filename, {})
        file_info[filename]['checksum'] = file_md5

        if 'gz' in org_filename:
            file_info[filename]['size'] = CHUNK_SIZE
            t = os.path.getmtime("%s/%s" %(folder, filename))
            modified_dt = datetime.fromtimestamp(t)
            file_info[filename]['created'] = datetime.strftime(
                modified_dt, "%Y-%m-%d %H:%M:%S")

        if "%s/%s" % (bucket_name, filename) in up_files:
            logging.debug("Already uploaded %s", filename)
            continue

        aws_md5 = Key.get_md5_from_hexdigest(k, file_md5)
        logging.debug("md5: %s, %r", file_md5, aws_md5)

        k.set_contents_from_filename("%s/%s" % (folder, filename), md5=aws_md5)

    lapse = int(time.time()) - start_ts
    logging.debug("s3 upload time taken: %d", lapse)


    # 4. check md5 checksum and save to dynamoDB
    logging.debug("\n\nChecking MD5 checksums of uploaded files")
    start_ts = int(time.time())
    rs_keys = bucket.list(bucket_name)
    num_files_uploaded = len(file_info)

    num_errors = 0
    not_found_errors = 0
    dynamo_table = Table('redshift_log')

    for key_val in rs_keys:
        if bucket_name not in key_val.key:
            continue

        filename = key_val.key.split("/")[-1]
        if not filename:
            logging.error("Empty filename %r", key_val.key)
            continue

        if filename == gzip_datafile:
            continue

        if filename not in file_info:
            logging.error("file %s not found!! key %r", filename, key_val.key)
            not_found_errors += 1
            continue
        # checksum
        logging.debug("check info for file %s: %r",
                filename, file_info[filename])

        local_checksum = file_info[filename]['checksum']
        s3_checksum = key_val.etag.strip('"')

        d_item = {
            'filename': filename,
            'uploaded_s3': True,
            'size': file_info[filename]['size'],
            'created': file_info[filename]['created'],
            'local_checksum': local_checksum,
            's3_checksum': s3_checksum}

        if local_checksum != s3_checksum:
            logging.error("fail checksum file: %s, checksum: %s, etag: %s",
                    filename, local_checksum, s3_checksum)
            num_errors += 1
            d_item['errors'] = ERROR_CHECKSUM

        logging.debug("dynamo item: %r", d_item)
        new_item = Item(dynamo_table, data=d_item)
        new_item.save(overwrite=True)


    lapse = int(time.time()) - start_ts
    logging.debug("md5 check time taken: %d", lapse)

    logging.debug("\n\nSummary for %s", prefix)
    logging.debug("Number of lines: %d", num_lines)
    logging.debug("Number of splitted files: %d", len(onlyfiles))
    logging.debug("Number of files uploaded: %d", num_files_uploaded)
    logging.debug("Errors-not-found: %d", not_found_errors)
    logging.debug("Errors-checksum: %d", num_errors)


    # create manifest file, format:
    # {
    # "entries": [
    #     {"url":"s3://mybucket/custdata.1","mandatory":true},
    #     {"url":"s3://mybucket/custdata.2","mandatory":true},
    #     {"url":"s3://mybucket/custdata.3","mandatory":true}
    # ]
    # }
    logging.debug("\n\nCreate manifest file")
    entries = []
    for filename in file_info:
        if filename == gzip_datafile:
            logging.debug("do not add original file %s to manifest", datafile)
            continue

        logging.debug("check file %s", filename)
        try:
            d_item = dynamo_table.get_item(filename=filename)
        except boto.dynamodb2.exceptions.ItemNotFound, e:
            logging.error("cannot find entry %s in dynamo logs", filename)
            logging.error("%r", e)
            continue
        logging.debug("adding to manifest: %s", filename)
        entries.append({"url": "s3://%s/%s/%s" % (
            S3_MAIN_BUCKET, bucket_name, filename),
            "mandatory": True})
        d_item['added_manifest'] = True
        d_item.partial_save()


    manifest_data = {"entries": entries}
    manifest_file = "%s.manifest" % prefix
    filep = open("%s/%s" % (folder, manifest_file), "w")
    filep.write(json.dumps(manifest_data, separators=(',', ':'), indent=2))
    filep.close()

    # upload the manifest file
    k = Key(bucket)
    k.key = "%s/%s" % (bucket_name, manifest_file)

    file_md5 = hashlib.md5(
            open("%s/%s" % (folder, manifest_file), 'rb').read()).hexdigest()
    aws_md5 = Key.get_md5_from_hexdigest(k, file_md5)
    k.set_contents_from_filename("%s/%s" % (folder, manifest_file),
            md5=aws_md5)


    # 5. set up COPY command to import from S3 to Redshift
    logging.debug("\n\nPreparing to COPY data to Redshift")

    # create copy script
    copy_filename = "Copy/copy_%s.sh" % prefix
    filep = open(copy_filename, "w")
    filep.write("#!/bin/bash" + "\n")
    filep.write(REDSHIFT_PSQL + " << EOF\n")  # psql connection

    credential = "'aws_access_key_id=%s;aws_secret_access_key=%s'" % (
                aws_key, aws_secret)
    manifest_file = "'s3://hello-db-exports/%s/%s.manifest'" % (
                bucket_name, prefix)
    copy_cmd = "COPY %s from %s credentials %s delimiter ',' gzip manifest;" % (
                redshift_table, manifest_file, credential)

    filep.write(copy_cmd + "\nEOF")  # COPY
    filep.close()

    os.system("chmod +x %s" % copy_filename)
    logging.debug("COPY command: %s", copy_cmd)


    # okay = raw_input("\n\n" + GREEN + BOLD + "Review manifest and command\n" +
    #     "Okay to proceed with Redshift COPY?(Y/n)" + ENDC)

    okay = 'Y'

    if okay == 'Y':
        logging.debug("Executing COPY command")
        start_ts = int(time.time())
        os.system("./%s" % (copy_filename))

        lapse = int(time.time()) - start_ts
        logging.debug("COPY to Redshift time taken: %d", lapse)

    logging.debug("\n\nDone %s", str(datetime.now()))


if __name__ == "__main__":
    """Example:
    python upload.py device_sensors_par 2015 08 03 yes yes >
    migrate_2015_08_03.log 2>&1
    """

    if len(sys.argv) != 7:
        print "Usage: python upload.py [table_prefix] [YYYY] [MM] [DD] " + \
                "[get_db] [do_split]\n\n"
        print "get_db: yes/no"
        print "do_split: yes/no"
        print "set DD to -1 to get a monthly table"
        sys.exit()

    main(sys.argv)
