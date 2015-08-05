import os
import sys

import json
import gzip
import hashlib
import logging

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

S3_MAIN_BUCKET = "hello-db-exports"
MAX_LINES = 100000000

logging.basicConfig(level=logging.DEBUG)

# python upload.py device_sensors_2015_02 device_sensors_par_2015_02
# device_sensors 0 device_sensors_2015_02 yes no
if len(sys.argv) != 8:
    print "Usage: python upload.py [table_name] [prefix] [folder] [offset] [bucket_name] [skip_db=yes/no] [skip_split]\n\n"
    sys.exit()

table_name = sys.argv[1]
prefix = sys.argv[2]
folder = sys.argv[3]
offset_arg = sys.argv[4]
bucket_name = sys.argv[5]
skip_db_step = sys.argv[6]
skip_split = sys.argv[7]

if not os.path.isdir(folder):
    logging.debug("Creating folder for data: %s", folder)
    os.makedirs(folder)

# 1. Get data from DB
if skip_db_step == 'no':
    os.system("get_data.sh %s %s %s %s" % (table_name, prefix, folder, offset_arg))

# check if file exist
gzip_datafile = "%s.csv.gz" % prefix
datafile = "%s/%s.csv" % (folder, prefix)
if not os.path.isfile(datafile):
    logging.error("Data not downloaded to %s", datafile)
    logging.debug("exiting....")
    sys.exit()
        

# 2. split data into smaller chunks
num_lines = 0
if skip_split == 'no':
    CHUNK_SIZE = 100000 # testing  5000000 # lines
    num = 0
    with open(datafile) as fp:
        added = 0
        for i, line in enumerate(fp):
            if added == 0:
                splitfile = "%s-0%d" % (prefix, num)
                if num < 10:
                    splitfile = "%s-00%d" % (prefix, num)
                elif num < 100:
                    splitfile = "%s-0%d" % (prefix, num)

                split_fp = open("%s/%s" % (folder, splitfile), 'w')

            split_fp.write(line)
            num_lines += 1
            added += 1

            if added >= CHUNK_SIZE:
                split_fp.close()
                added = 0
                num += 1

    split_fp.close()


# upload to S3
onlyfiles = [ f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder,f)) ]

aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

conn = S3Connection(aws_key, aws_secret)
bucket = conn.get_bucket(S3_MAIN_BUCKET)

file_md5_dict = {}
for org_filename in onlyfiles:
    cmd = "gzip %s/%s" % (folder, org_filename)
    logging.debug(cmd)
    os.system(cmd)

    filename = "%s.gz" % org_filename
    logging.debug("---- Uploading file %s", filename)
    k = Key(bucket)
    k.key = "%s/%s" % (bucket_name, filename)


    # multipart upload
    # chunk_size = 1024 # 52428800   # 50MB chunks
    # mp = bucket.initiate_multipart_upload(filename)

    file_md5 = hashlib.md5(open("%s/%s" % (folder, filename), 'rb').read()).hexdigest()
    file_md5_dict[filename] = file_md5

    aws_md5 = Key.get_md5_from_hexdigest(k, file_md5)
    logging.debug("md5: %s, %r", file_md5, aws_md5)

    k.set_contents_from_filename("%s/%s" % (folder, filename), md5=aws_md5)

# Check md5 checksum
rs_keys = bucket.get_all_keys()
num_files_uploaded = len(file_md5_dict)

num_errors = 0
not_found_errors = 0
for key_val in rs_keys:
    if bucket_name not in key_val.key:
        continue
    filename = key_val.key.split("/")[-1]
    if filename not in file_md5_dict:
        logging.error("file %s not found!!", filename)
        not_found_errors += 1
        continue

    checksum = key_val.etag.strip('"')
    if file_md5_dict[filename] != checksum:
        logging.error("checksum error! file: %s, checksum: %s, etag: %s",
                filename, file_md5_dict[filename], checksum)
        num_errors += 1


logging.debug("Summary for %s", prefix)
logging.debug("Number of lines: %d", num_lines)
logging.debug("Number of splitted files: %d", len(onlyfiles))
logging.debug("Number of files uploaded: %d", num_files_uploaded)
logging.debug("Errors-not-found: %d", not_found_errors)
logging.debug("Errors-checksum: %d", num_errors)

# save checksum in dynamo


# create manifest file
# manifest file format
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
for filename in file_md5_dict:
    if filename == gzip_datafile:
        logging.debug("do not add original file %s to manifest", datafile)
        continue

    logging.debug("adding to manifest: %s", filename)
    entries.append({"url": "s3://%s/%s/%s" % (S3_MAIN_BUCKET, bucket_name, filename),
        "mandatory": True})

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

redshift_pw = os.getenv("PGPASSWORD")

logging.debug("command line to copy data")
redshift_psql = "psql -h sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com -U hello_sensors2 -p 5439 -d sensors1 << EOF"
redshift_copy = "copy device_sensors_master from 's3://hello-db-exports/%s/%s.manifest' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter ',' gzip manifest;" % (bucket_name, prefix, aws_key, aws_secret)

copy_filename = "Copy/copy_%s.sh" % prefix
filep = open(copy_filename, "w")
filep.write("#!/bin/bash" + "\n")
filep.write(redshift_psql + "\n")
filep.write(redshift_copy + "\n")
filep.write("EOF")
filep.close()
os.system("chmod +x %s" % copy_filename)
logging.debug("%s", redshift_copy)

