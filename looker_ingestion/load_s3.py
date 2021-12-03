""" To be imported to extraction scripts to push data to S3. """

import os
import shutil
import tempfile
import boto3
import json
import logging
import csv

BUCKET_NAME = os.getenv("bucket_name")

def load_object_to_s3(data, local_file_name, output_filename, s3_bucket=BUCKET_NAME):
    """ This saves a json, list of json, or CSV object
    locally to a temporary file and then uploads it to the S3 bucket"""

    temp_dir = tempfile.TemporaryDirectory()
    input_filename = os.path.join(temp_dir.name, local_file_name)
    ## allow for it to be csv
    with open(input_filename, 'w') as f:
        if isinstance(data, dict) or isinstance(data, list):
            json.dump(data, f)
        else:
            fp = csv.DictWriter(f, data[0].keys())
            fp.writeheader()
            fp.writerows(data)
    load_s3(s3_bucket, input_filename, output_filename)
    # remove temp directory if done
    if os._exists(temp_dir.name):
        shutil.rmtree(temp_dir.name)


def load_s3(s3_bucket, input_filename, output_filename):
    """ Pushes file to S3. """

    s3_storage = boto3.resource("s3")
    s3_storage.meta.client.upload_file(input_filename, s3_bucket, output_filename)
    logging.info(
        f"COMPLETE: {input_filename} loaded into \
    s3:// {s3_bucket} as {output_filename}"
    )

def find_existing_data(prefix, s3_bucket=BUCKET_NAME):
    """ Given a key within an S3 buckets, reads through all files and returns content"""
    s3_storage = boto3.resource("s3")
    my_bucket = s3_storage.Bucket(s3_bucket)
    json_objects = []
    for object_summary in my_bucket.objects.filter(Prefix=prefix):
        content_object = s3_storage.Object(s3_bucket, object_summary.key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = [json.loads(line) for line in file_content.splitlines()]
        json_objects.extend(json_content)
    return json_objects