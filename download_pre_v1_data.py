#!/usr/bin/env python
import boto3
import time
import datetime
import os
from opensearchpy import OpenSearch
import argparse
from astropy.io import ascii
import sys

def get_s3_prefix_from_s3(filename):
    # For older data you will need to query the whole prefix list from s3 like the following
    paginator = s3.get_paginator('list_objects_v2')
    object_keys = []
    pages = paginator.paginate(Bucket=args.bucket, Prefix='')
    for page in pages:
        for obj in page['Contents']:
            object_keys.append(obj['Key'])

    # Once you have all the object keys, just find the one that matches the filename
    for key in object_keys:
        if filename in key:
            return key

    return None

def make_s3_prefix_from_filename(filename):
    # Note that this only works if the data is in our current standard structure.
    site = filename[0:3]
    instrument = filename.split('-')[1]
    dayobs = filename.split('-')[2]

    # Ensure that we use the .fits.fz filename irrespective of what was provided
    compressed_filename = filename.replace('.fits.fz', '.fits').replace('.fits', '.fits.fz')
    prefix = f'{site}/{instrument}/{dayobs}/raw/{compressed_filename}'
    return prefix


def get_frame_metadata(filename, search_host, index_name):
    query = {
        "query": {
            "match_phrase": {
                # Scrub the filename of the extension to make sure we find the file 
                # irrespective of compression
                "filename": filename.replace('.fits', '').replace('.fz', '')
            }
        }
    }

    response = search_host.search(index=index_name, body=query)
    if response['hits']['hits'] is None or not response['hits']['hits']:
        print(filename)
        return None
    return response['hits']['hits'][0]['_source']


# Lifted from BANZAI
def parse_date_obs(date_obs_string):
    if date_obs_string == 'N/A':
        return None
    # Check if there are hours
    if 'T' not in date_obs_string:
        return datetime.datetime.strptime(date_obs_string, '%Y-%m-%d')
    # Check if there is fractional seconds in the time
    date_fractional_seconds_list = date_obs_string.split('.')
    if len(date_fractional_seconds_list) > 1:
        # Pad the string with zeros to include microseconds
        date_obs_string += (6 - len(date_fractional_seconds_list[-1])) * '0'
    else:
        # Pad the string with zeros to include microseconds
        date_obs_string += '.000000'
    return datetime.datetime.strptime(date_obs_string, '%Y-%m-%dT%H:%M:%S.%f')


def get_nearest_calibration_frames(obstype, n_frames, filename, search_host, search_index):
    frame_metadata = get_frame_metadata(filename, search_host, search_index)
    if frame_metadata is None:
        return []

    match_filters = [{"match_phrase": {"OBSTYPE": obstype}},
                     {"match_phrase": {"SITEID": frame_metadata['SITEID']}},
                     {"match_phrase": {"INSTRUME": frame_metadata['INSTRUME']}}]
    if obstype == 'SKYFLAT':
        match_filters.append({"match_phrase": {"FILTER": frame_metadata['FILTER']}})

    input_date_obs = parse_date_obs(frame_metadata['DATE-OBS'])
    # Opensearch only likes 3 decimal places on the time so we need to truncate the microseconds
    input_date_obs = f"{input_date_obs.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}UTC"
    query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": match_filters
                    }
                },
                "functions": [
                    {
                        "linear": {
                            "DATE-OBS": {
                                "origin": input_date_obs,
                                "scale": "1h",
                                "decay": 0.999
                            }
                        }
                    }
                ],
                "boost_mode": "multiply"
            }
        },
        'size': n_frames
    }
    response = search_host.search(index=search_index, body=query)
    filenames = [result['_source']['filename'] for result in response['hits']['hits']
                 if '00.fits' in result['_source']['filename']]
    return [make_s3_prefix_from_filename(f) for f in filenames]


def thaw_files(object_keys, bucket_name, s3, thaw_mode='Standard'):
    dead_references = []
    for object_key in object_keys:
        try:
            s3.restore_object(Bucket=bucket_name,
                              Key=object_key,
                              RestoreRequest={
                                  'Days': 1,  # Number of days to keep the restored copy
                                  'GlacierJobParameters': {
                                      'Tier': thaw_mode
                                      }
                                  }
                              )
        except Exception as e:
            if 'NoSuchKey' in str(sys.exc_info()):
                # There are some inconsistencies in the S3 bucket. We may have found a
                # broken reference.
                dead_references.append(object_key)
                print(f"Unable to thaw file {object_key}. No such key.")
            elif 'RestoreAlreadyInProgress' not in str(sys.exc_info()):
                raise e
    # Tidy object list before returning
    object_keys -= set(dead_references)


def wait_for_files_to_thaw(object_keys, bucket_name, s3):
    restored_objects = []
    while len(restored_objects) < len(object_keys):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for object_key in object_keys:
            if object_key not in restored_objects:
                response = s3.head_object(Bucket=bucket_name, Key=object_key)
                if 'Restore' in response and 'ongoing-request="false"' in response['Restore']:
                    restored_objects.append(object_key)
        print(f"{now}: Waiting for files to thaw: {len(object_keys) - len(restored_objects)} left to be restored.")
        # Wait for 5 minutes before checking again
        time.sleep(300)


def download_thawed_files(object_keys, base_dir, bucket_name, s3):
    for object_key in object_keys:
        os.makedirs(os.path.join(base_dir, os.path.dirname(object_key)), exist_ok=True)
        download_path = os.path.join(base_dir, object_key)
        try:
            s3.download_file(bucket_name, object_key, download_path)
            print(f"Downloaded {os.path.basename(object_key)}")
        except Exception as e:
            print(f"Skipped {os.path.basename(object_key)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull data from pre-v1 S3 deep storage', 
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--bucket', type=str, default='archive-prev1-data-lco-global', help='S3 bucket name')
    parser.add_argument('--frame-list', required=True, help='File with of frames to pull from s3')
    parser.add_argument('--output-dir', default=os.getcwd(), help='Output base directory for the data')
    parser.add_argument('--opensearch-host', default='https://opensearch.lco.global/', help='OpenSearch host')
    parser.add_argument('--opensearch-index', default='fitsheaders-prev1', help='OpenSearch index')
    parser.add_argument('--aws-access-key', required=True, help='AWS access key id')
    parser.add_argument('--aws-secret-key', required=True, help='AWS secret access key')
    parser.add_argument('--aws-region', default='us-west-2', help='AWS region for the bucket')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode')
    parser.add_argument('--thaw-mode', default='Standard', choices=['Standard', 'Bulk'])

    args = parser.parse_args()

    # Pick column to match fits file name in image list.
    files_to_restore = ascii.read(args.frame_list, format='no_header')['col1']

    search_host = OpenSearch(args.opensearch_host, timeout=60)

    aws_session = boto3.Session(aws_access_key_id=args.aws_access_key,
                                aws_secret_access_key=args.aws_secret_key,
                                region_name=args.aws_region)
    s3 = aws_session.client('s3')

    # Download the files from s3
    download_thawed_files(files_to_restore, args.output_dir, args.bucket, s3)
