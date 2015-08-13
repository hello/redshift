# Data migration for RDS to Redshift

## Configure ec2 instance for migration
- Launch an instance with sufficient memory and cpu (160G SSD disk, and c3.large), set up with the right security configuration

- Install these on the instance:
```
sudo apt-get update
sudo apt-get install awscli
sudo apt-get install postgresql-client-common
sudo apt-get install postgresql-client
sudo apt-get install pigz
sudo apt-get install htop
sudo apt-get install python-pip
sudo pip install boto
```

- Run `aws configure` to set up your AWS keys

- Setup the following env parameters (used by different modules):
```
export AWS_ACCESS_KEY_ID=<ke>
export AWS_SECRET_ACCESS_KEY=<secret>
export aws_access_key_id=<key>
export aws_secret_access_key=<secret>
unset PGPASSWORD
```

- Create `~/.pgpass` for `psql` to run from bash. On each for RDS replica, and Redshift cluster with the following format: `hostname:port:database:user:password`

- `chmod 600 ~/.pgpass`
- Create a directory named `Copy` to the bash script for copying data from S3 to Redshift
- Copy these scripts into the home directory
```
upload.py
get_data.sh
get_count.sh
```

## To run the migration
The script is set up to import daily tables from device_sensor and tracker_motion.
```
python upload.py [table-name-prefix] [YYYY] [MM] [DD] [download_data] [split_data]
```
- download_data ('yes'/'no'): input 'yes' if you want to download the data directly from RDS, or 'no' if you already have a copy
- split_data ('yes'/'no'): input 'yes' if you want to split the data into smaller chunks, **highly recommended**

## Example
To migrate device_sensor for 2015-08-12:
```
python upload.py device_sensors_par 2015 08 12 yes yes > migrate_2015_08_12.log 2>&1
```

The sequence of events:
- a directory `device_sensors_par_2015_08_12` will be created
- a csv file of the daily table will be downloaded, named `device_sensors_par_2015_08_12.csv`
- the csv file will be split into files of 1000000 lines each, with names starting from `device_sensors_par_2015_08_12-00000`
- all data files will be gzipped and uploaded to and S3 bucket `[main-bucket]/device_sensors_2015_08/2015_08_12/`
- a manifest file will be created and also uploaded to the same S3 bucket
- you will find `copy_device_sensors_par_2015_08_12.sh` in the `Copy` directory with the command to upload data to Redshift.
