import os
import sys

from datetime import datetime
import json
import gzip
import hashlib
import logging

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import boto.dynamodb2
from boto.dynamodb2.table import Table

DYNAMODB_TABLE = 'redshift_log'
S3_MAIN_BUCKET = "hello-db-exports"
MAX_LINES = 100000000
CHUNK_SIZE = 1000000 # testing  5000000 # lines

ERROR_NOT_FOUND = 'File in S3 bucket, but not in local drive'
ERROR_CHECKSUM = "Checksum do not match"

# terminal colors
GREEN = '\033[92m'
ENDC = '\033[0m'
BLUE = '\033[94m'
BOLD = '\033[1m'

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto').setLevel(logging.ERROR)

logging.debug("\n\nStart %s", str(datetime.now()))

# python upload.py device_sensors_2015_02 device_sensors_par_2015_02
# device_sensors 0 device_sensors_2015_02 yes no
if len(sys.argv) != 8:
    print "Usage: python upload.py [table_name] [prefix] [folder] [offset] [bucket_name] [get_db] [do_split]\n\n"
    print "get_db: yes/no"
    print "do_split: yes/no"
    sys.exit()

table_name = sys.argv[1]
prefix = sys.argv[2]
folder = sys.argv[3]
offset_arg = sys.argv[4]
bucket_name = sys.argv[5]
get_db = sys.argv[6]
do_split = sys.argv[7]

if not os.path.isdir(folder):
    logging.debug("Creating folder for data: %s", folder)
    os.makedirs(folder)

# 1. Get data from DB
if get_db == 'yes':
    logging.debug("\n\nGetting data from RDS table %s", table_name)
    os.system("get_data.sh %s %s %s %s" % (table_name, prefix, folder, offset_arg))

# check if file exist
gzip_datafile = "%s.csv.gz" % prefix
datafile = "%s/%s.csv" % (folder, prefix)
logging.debug("\n\nCheck if datafile %s exist", datafile)
if not os.path.isfile(datafile):
    logging.error("Data not downloaded to %s", datafile)
    logging.debug("exiting....")
    sys.exit()
        

# 2. split data into smaller chunks
logging.debug("\n\nSplit data into files of size %s", CHUNK_SIZE)
file_info = {}
num_lines = 0
if do_split == 'yes':
    num = 0
    with open(datafile) as fp:
        added = 0
        for i, line in enumerate(fp):
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


# 3. upload to S3
logging.debug("\n\nPreparing to upload to S3 %s/%s", S3_MAIN_BUCKET, bucket_name)

onlyfiles = [ f for f in os.listdir(folder)
            if os.path.isfile(os.path.join(folder,f)) ]

aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

conn = S3Connection(aws_key, aws_secret)
bucket = conn.get_bucket(S3_MAIN_BUCKET)

for org_filename in onlyfiles:
    cmd = "gzip %s/%s" % (folder, org_filename)
    logging.debug(cmd)
    os.system(cmd)

    filename = "%s.gz" % org_filename
    logging.debug("---- Uploading file %s", filename)
    k = Key(bucket)
    k.key = "%s/%s" % (bucket_name, filename)

    # get md5 checksum for verification
    file_md5 = hashlib.md5(open("%s/%s" % (folder, filename), 'rb').read()).hexdigest()
    file_info.setdefault(filename, {})
    file_info[filename]['checksum'] = file_md5

    aws_md5 = Key.get_md5_from_hexdigest(k, file_md5)
    logging.debug("md5: %s, %r", file_md5, aws_md5)

    k.set_contents_from_filename("%s/%s" % (folder, filename), md5=aws_md5)

# 4. check md5 checksum and save to dynamoDB
logging.debug("\n\nChecking MD5 checksums of uploaded files")
rs_keys = bucket.get_all_keys()
num_files_uploaded = len(file_info)

num_errors = 0
not_found_errors = 0
dynamo_table = Table('redshift_log')
dynamo_generic_item = {'filename': 'none',
    'size': 0,
    'created': str(datetime.now()),
    'uploaded_s3': False,
    'added_manifest': False,
}

ERROR_NOT_FOUND = 'File in S3 bucket, but not in local drive'
ERROR_CHECKSUM = "Checksum do not match"

for key_val in rs_keys:
    if bucket_name not in key_val.key:
        continue

    filename = key_val.key.split("/")[-1]
    if not filename:
        logging.error("Empty filename %r", key_val.key)
        continue

    d_item = dict(dynamo_generic_item)
    d_item['filename'] = filename
    d_item['uploaded_s3'] = True

    if filename not in file_info:
        logging.error("file %s not found!! key %r", filename, key_val.key)
        not_found_errors += 1
        d_item['errors'] = ERROR_NOT_FOUND
        logging.debug("dynamo item: %r", d_item)
        dynamo_table.put_item(data=d_item)
        continue

    # checksum
    logging.debug("info for file %s: %r", filename, file_info[filename])
    d_item['size'] = file_info[filename]['size']
    d_item['created'] = file_info[filename]['created']

    local_checksum = file_info[filename]['checksum']
    d_item['local_checksum'] = local_checksum

    s3_checksum = key_val.etag.strip('"')
    d_item['s3_checksum'] = s3_checksum

    if local_checksum != s3_checksum:
        logging.error("checksum error! file: %s, checksum: %s, etag: %s",
                filename, local_checksum, s3_checksum)
        num_errors += 1
        d_item['errors'] = ERROR_CHECKSUM

    logging.debug("dynamo item: %r", d_item)
    dynamo_table.put_item(data=d_item)



logging.debug("Summary for %s", prefix)
logging.debug("Number of lines: %d", num_lines)
logging.debug("Number of splitted files: %d", len(onlyfiles))
logging.debug("Number of files uploaded: %d", num_files_uploaded)
logging.debug("Errors-not-found: %d", not_found_errors)
logging.debug("Errors-checksum: %d", num_errors)


# create manifest file
# manifest file format
logging.debug("\n\nCreate manifest file")
manifest = """
{
  "entries": [
      {"url":"s3://mybucket/custdata.1","mandatory":true},
      {"url":"s3://mybucket/custdata.2","mandatory":true},
      {"url":"s3://mybucket/custdata.3","mandatory":true}
  ]
}
"""
entries = []
for filename in file_info:
    if filename == gzip_datafile:
        logging.debug("do not add original file %s to manifest", datafile)
        continue

    logging.debug("adding to manifest: %s", filename)
    entries.append({"url": "s3://%s/%s/%s" % (S3_MAIN_BUCKET, bucket_name, filename),
        "mandatory": True})
    d_item = dynamo_table.get_item(filename=filename)
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

file_md5 = hashlib.md5(open("%s/%s" % (folder, manifest_file), 'rb').read()).hexdigest()
aws_md5 = Key.get_md5_from_hexdigest(k, file_md5)
k.set_contents_from_filename("%s/%s" % (folder, manifest_file), md5=aws_md5)


# set up COPY command to import from S3 to Redshift
logging.debug("\n\nPreparing to COPY data to Redshift")
redshift_pw = os.getenv("PGPASSWORD")

logging.debug("command line to copy data")
redshift_psql = "psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U migrator -p 5439 -d sensors1 << EOF"
redshift_copy = "COPY device_sensors_master from 's3://hello-db-exports/%s/%s.manifest' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter ',' gzip manifest;" % (bucket_name, prefix, aws_key, aws_secret)

# create copy script
copy_filename = "Copy/copy_%s.sh" % prefix
filep = open(copy_filename, "w")
filep.write("#!/bin/bash" + "\n")
filep.write(redshift_psql + "\n")
filep.write(redshift_copy + "\n")
filep.write("EOF")
filep.close()
os.system("chmod +x %s" % copy_filename)
logging.debug("Executing %s", redshift_copy)

okay = raw_input("\n\n" + GREEN + BOLD +"Okay to proceed with Redshift COPY?(Y/n)" + ENDC)
if okay == 'Y':
    logging.debug("Executing %s", redshift_copy)
    os.system("./%s" % (copy_filename))

logging.debug("\n\nDone %s", str(datetime.now()))
