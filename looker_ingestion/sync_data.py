""" Extracts data from a given JSON file path, converts it to a Looker query
and writes the data to S3 """

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import looker_sdk

from .exceptions import NoDataException
from .load_s3 import find_existing_data, load_object_to_s3

PARENT_PATH = os.path.dirname(__file__)
NOW = str(time.time()).split(".")[0]
BUCKET_NAME = os.getenv("bucket_name")


def extract_query_details(json_filename):
    """Given literal file path to a JSON object, returns the query contents
    Parameters:
        json_filename (text): full path to the details of the query object

    Returns:
        queries (list): A list of dictionaries with the query details"""
    json_file_path = os.path.join(PARENT_PATH, json_filename)
    with open(json_file_path, "r") as json_file:
        queries = json.load(json_file)
    if isinstance(queries, dict):
        queries = [queries]
    return queries


def find_last_date(
    file_prefix,
    datetime_index,
    default_days,
    aws_storage_bucket_name,
    aws_server_public_key,
    aws_server_secret_key,
):
    """For the relevant file path, find the date to start extracting data with
    Parameters:
        file_prefix (string): The unique prefix for this query to use to read from S3
        datetime_index (string): The field from the query to use as the bookmark datetime
        default_days (int): The number of days to default to if there is no historical data
        aws_storage_bucket_name (string): Can be None - AWS bucket name
        aws_server_public_key (string): Can be None - AWS server public key
        aws_server_secret_key (string): Can be None - AWS secret key

    Returns:
        lookml filter (string): (Default: today) If historical data is found, a string in the form
        start_date to end_date
    """

    ## if there's no data, get the last day
    first_date = f"{default_days} day"
    ## get the largest query time in the data warehouse
    json_objects = find_existing_data(
        file_prefix,
        aws_storage_bucket_name,
        aws_server_public_key,
        aws_server_secret_key,
    )
    last_date = "1990-01-01 00:00:00"
    for row in json_objects:
        if file_prefix.endswith("csv"):
            datetime_index = datetime_index.replace(".", " ").replace("_", " ")
        last_date = max(last_date, row[datetime_index])
    if last_date is None or last_date == [] or last_date == "1990-01-01 00:00:00":
        logging.info(f"No date found; running with {first_date}")
        last_date = (datetime.now() - timedelta(days=default_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    times = []
    times = find_date_range(last_date)
    if times == -1:
        sys.exit(0)
    if times is None or times == []:
        raise ValueError("No valid time range found")
    start_time = times[0] - timedelta(minutes=5)
    return f"""{start_time.strftime('%Y-%m-%d %H:%M:%S')}
                to {times[1].strftime('%Y-%m-%d %H:%M:%S')}"""


def find_date_range(start_time):
    """If an incremental extraction, find the start and end date to use in the query
    Parameters:
        start_time (string): The datetime that we want to begin the range from

    Returns:
        [start_time, end_time] (list):  A list of the start_time and ending 24 hours later
        or now, whichever is later
        If the start time is within 10 minutes of the current time, return NoDataException to indicate not to run
    """
    try:
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        start_time = datetime.strptime(start_time + " 00:00:00", "%Y-%m-%d %H:%M:%S")

    hours_old = (datetime.now() - start_time).total_seconds() // 3600
    ## given it a ten minute time difference
    if hours_old <= 0.16:
        msg = "All up to date, not running any data"
        logging.warning(msg)
        raise NoDataException(msg)
    hours_old = min(int(hours_old) + 1, 24)
    end_time = start_time + timedelta(hours=hours_old, minutes=0)
    logging.info(f"{start_time} to {end_time}")
    return [start_time, end_time]


def extract_data(
    json_filename,
    aws_storage_bucket_name=BUCKET_NAME,
    aws_server_public_key=None,
    aws_server_secret_key=None,
    file_prefix="looker",
    file_name=None,
):
    """Read in the JSON file, iterate through query info,
    run the queries, and write to s3. If no data, don't do SQL parts
    It takes one argument: the filename of the JSON file in this folder
    that holds the query info
        Parameters:
            json_filename (string): Literal path to the location of the query details

        Optional Paramters:
            aws_storage_bucket_name (string): Defaults to the bucket_name enviromental variable;
                    the AWS bucket this extract should be stored in and read from
            aws_server_public_key (string): The AWS public server key with access to where this file should be
                    written and read from
            aws_server_secret_key (string):The AWS secret server key with access to where this file should be
                    written and read from
            file_prefix (string): The path to write the file on S3. Defaults to 'looker'
            file_name (string): The name of the file to write to S3. Defaults to 'looker_<query_name>_<timestamp>'

        Returns:
            None
    """

    queries = extract_query_details(json_filename)

    REQUIRED_KEYS = ["name", "model", "explore", "fields"]
    files_upload = []
    for query_body in queries:
        for key in REQUIRED_KEYS:
            if query_body.get(key) is None:
                raise KeyError(f"{key} is a mandatory element in the JSON")

        query_name = query_body["name"]
        metadata = query_body.get("metadata")
        result_format = metadata.get("result_format") or "json"
        if result_format not in ["json", "csv"]:
            raise ValueError("Invalid instance type; please use only json or csv")
        filters = query_body.get("filters")
        datetime_index = metadata.get("datetime")
        sorts = query_body.get("sorts")
        default_days = metadata.get("default_days")
        row_limit = query_body.get("limit") or 5000
        is_incremental_extraction = datetime_index is not None
        fields = query_body["fields"]
        full_file_prefix = f"{file_prefix}/{query_name}/{result_format}"
        if not file_name:
            file_name = f"looker_{query_name}_{NOW}"
        full_file_name = f"{full_file_prefix}/{file_name}.{result_format}"

        ## if it's an incremental load by datetime, it must be sorted by that datetime
        ## in asc ordering as its primary sort
        if (
            is_incremental_extraction
            and sorts[0] != datetime_index
            and sorts[0].lower() != datetime_index.lower() + " asc"
        ):
            raise ValueError(
                "For an incremental job, the first sort must be the metadata.datetime field ASC"
            )

        ## if the filter already exists, dont run it
        ## if there's no datetime, don't run it
        ## if there no datetime defined
        if is_incremental_extraction and filters.get(datetime_index) is None:
            try:
                int(default_days)
            except ValueError:
                logging.info(
                    "Please provide a valid integer for the default date; using 1 day"
                )
                default_days = 1
            else:
                default_days = int(default_days)
            date_filter = find_last_date(
                full_file_prefix,
                datetime_index,
                default_days,
                aws_storage_bucket_name,
                aws_server_public_key,
                aws_server_secret_key,
            )
            filters[datetime_index] = f"{date_filter}"

        ## hit the Looker API
        write_query = looker_sdk.models.WriteQuery(
            model=query_body["model"],
            view=query_body["explore"],
            fields=fields,
            filters=filters,
            sorts=sorts,
            limit=row_limit,
        )
        sdk = looker_sdk.init31()
        query_run = sdk.run_inline_query(result_format, write_query)
        if result_format == "json":
            query_run = json.loads(query_run)
            if query_run != []:
                if query_run[0].get("looker_error") is not None:
                    logging.error(
                        f"Error {query_run[0].get('looker_error')} returned when attempting to fetch Looker query history for {date_filter}"
                    )

        if query_run == [] or query_run is None:
            logging.error(
                f"No data returned when attempting to fetch Looker query history for {date_filter}"
            )
        else:
            file_uploaded = load_object_to_s3(
                query_run,
                full_file_name,
                aws_storage_bucket_name,
                aws_server_public_key,
                aws_server_secret_key,
            )
            files_upload.append(file_uploaded)

    return files_upload


def parse_args():
    """Parses arguments via the command line"""
    parser = argparse.ArgumentParser(
        prog="extract_looker_metadata",
        description="Intakes a file name to parse to create a Loooker query",
    )
    parser.add_argument(
        "--json_file",
        dest="json_file",
        type=str,
        required=True,
        help="the JSON file location that contains the data to run the Looker query or queries",
    )
    parser.add_argument(
        "--aws_server_public_key",
        dest="aws_server_public_key",
        help="AWS public key (not needed if stored as env variables)",
    )
    parser.add_argument(
        "--aws_server_secret_key",
        dest="aws_server_secret_key",
        help="AWS secret key (not needed if stored as env variables)",
    )
    parser.add_argument(
        "--aws_storage_bucket_name",
        dest="aws_storage_bucket_name",
        help="AWS bucket name (not needed if stored as env variables)",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    if args.aws_server_public_key is not None:
        extract_data(
            args.json_file,
            args.aws_storage_bucket_name,
            args.aws_server_public_key,
            args.aws_server_secret_key,
        )
    else:
        extract_data(args.json_file)


if __name__ == "__main__":
    main()
