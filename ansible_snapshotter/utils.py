import argparse
import os
import subprocess
import json
import shutil
import zipfile
import re

try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser # python3

import boto3
import botocore


# Ansible Functions
def run_playbook(play, args):
    # pass args as a dict
    
    if not play.endswith('.yml'):
        play += '.yml'
    if not os.path.isfile(play):
        raise Exception('File does not exist: %s' % play)

    cmd = ['ansible-playbook', play]
    
    if args:
        cmd.append('--extra-vars')
        cmd.append(json.dumps(args))

    return subprocess.call(cmd) # 0 success 1 fail


# AWS S3 Functions
def get_s3_bucket(s3_access_key, s3_secret_key, s3_region, s3_bucket):

    # bucket.upload_file('path', 'key')
    # bucket.download_file('key', 'path')
    s3 = boto3.resource(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name=s3_region
    )
    bucket = s3.Bucket(s3_bucket)
    
    try:
        s3.meta.client.head_bucket(Bucket=s3_bucket)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            raise Exception('Bucket does not exist')
        else:
            raise e

    return bucket


def s3_bucket():
    
    config = ConfigParser()
    if len(config.read('config.ini')) == 0:
        raise Exception('ERROR: Cannot find config.ini in script directory')
    bucket = config.get('s3-aws-info', 'bucket')
    region = config.get('s3-aws-info', 'region')
    account = config.get('s3-aws-info', 'account')
    password = config.get('s3-aws-info', 'password')
    if not(bucket and region and account and password):
        raise Exception('AWS arguments in config.ini not specified')
    try:
        return get_s3_bucket(account, password, region, bucket)
    except ValueError as e:
        print('ERROR: Invalid config.ini options')
        raise e


def s3_list_snapshots(s3_bucket):
    options = []
    for obj in s3_bucket.objects.all():
        match = re.match('cassandra-snapshot-[^\s]*', obj.key)
        if match:
            options.append(obj.key)
    return options
            

def s3_delete_object(s3_bucket, key):
    
    return s3_bucket.delete_objects(
        Delete={
            'Objects': [
                {'Key': key }
            ]
        }
    )


# File System Functions
def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')
    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def check_file(f):

    if not os.path.isfile(f):
        raise argparse.ArgumentTypeError('File does not exist')
    if os.access(f, os.R_OK):
        if zipfile.is_zipfile(f):
            return f
        else:
            raise argparse.ArgumentTypeError('File is not a zip file')
    else:
        raise argparse.ArgumentTypeError('File is not readable')


def zip_dir(root_path, save_path, title): # use shutil.make_archive in python2.7

    rootlength = len(root_path)
    z = zipfile.ZipFile(save_path + '/' + title + '.zip',
                        'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(root_path):
        for f in files:
            filename = os.path.join(root_path, f)
            z.write(filename, filename[rootlength:])
    z.close()


def clean_dir(path):

    # removes all files and directories in a directory
    for f in os.listdir(path):
        if os.path.isdir(path + '/' + f):
            shutil.rmtree(path + '/' + f)
        else:
            os.remove(path + '/' + f)


def make_dir(path):

    # makes a directory if it does not exist
    exists = False
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        exists = True
    return exists


def prepare_dir(path, output=False):
    
    # makes a directory if it does not exist, clears it if it does
    if output:
        print('Preparing directory: %s' % path)
    if make_dir(path):
        clean_dir(path)


# Miscellaneous Functions
def confirm(prompt=None):

    options = set(['y', 'Y', 'n', 'N'])
    confirm = ''
    while confirm not in options:
        confirm = raw_input(prompt)
    if confirm.lower() == 'y':
        return True
    else:
        return False
