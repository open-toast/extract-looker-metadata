""" To be imported to extraction scripts to push data to S3. """

import csv
import json
import logging

import boto3
from smart_open import open


def load_object_to_s3(
    data,
    output_filename,
    s3_bucket,
    aws_server_public_key=None,
    aws_server_secret_key=None,
):
    """Uploads a json, list of json, or CSV object to the S3 bucket"""

    if aws_server_public_key is not None:
        session = create_session(aws_server_public_key, aws_server_secret_key)
        s3_storage = session.resource("s3")
    else:
        s3_storage = boto3.resource("s3")

    if not s3_bucket.startswith("s3://"):
        s3_bucket = f"s3://{s3_bucket}"

    s3_url = f"{s3_bucket}/{output_filename}"
    with open(s3_url, "w", transport_params={"client": s3_storage.meta.client}) as f:
        if isinstance(data, dict) or isinstance(data, list):
            f.write(json.dumps(data))
        else:
            f.writelines(data)

    return s3_url


def create_session(aws_server_public_key, aws_server_secret_key):
    """If AWS credentials are passed, create a session with them"""

    return boto3.Session(
        aws_access_key_id=aws_server_public_key,
        aws_secret_access_key=aws_server_secret_key,
    )


def find_existing_data(
    prefix, s3_bucket, aws_server_public_key=None, aws_server_secret_key=None
):
    """Given a key within an S3 buckets, find the most recently stored file and its greatest metadata date"""

    if aws_server_public_key is not None:
        session = create_session(aws_server_public_key, aws_server_secret_key)
        s3_storage = session.resource("s3")
        s3_client = session.client("s3")
    else:
        s3_storage = boto3.resource("s3")
        s3_client = boto3.client("s3")

    json_row_objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        try:
            contents = page["Contents"]
        except KeyError:
            break
        json_row_objects.extend(contents)

    ### if this is the first goaround, return an empty []
    if len(json_row_objects) == 0:
        return json_row_objects

    most_recent_file = max(json_row_objects, key=lambda x: x["LastModified"])

    content_object = s3_storage.Object(s3_bucket, most_recent_file["Key"])
    file_content = content_object.get()["Body"].read().decode("utf-8")
    if content_object.key.endswith(".json"):
        for line in file_content.splitlines():
            for json_line in json.loads(line):
                json_row_objects.append(json_line)
    elif content_object.key.endswith(".csv"):
        for row in csv.DictReader(file_content.splitlines(True)):
            json_row_objects.append(
                {k.lower().strip(): v.strip() for k, v in row.items()}
            )
    else:
        logging.info("Found file of invalid type, not processing for most recent date")
    return json_row_objects
