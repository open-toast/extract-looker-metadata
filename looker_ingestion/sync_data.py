""" Extracts data from a given JSON file path, converts it to a Looker query
and writes the data to S3 """
import time
import json
import os
import logging
from datetime import timedelta, datetime
import argparse

import looker_sdk
from .load_s3 import load_object_to_s3, find_existing_data

PARENT_PATH = os.path.dirname(__file__)
NOW = str(time.time()).split(".")[0]


def extract_query_details(json_filename):
    """ Gets the query/body for each query from the JSON file """
    json_file_path = os.path.join(PARENT_PATH, json_filename)
    with open(json_file_path, "r") as json_file:
        queries = json.load(json_file)
    return queries


def find_last_date(query_name, datetime_index):
    """ For the relevant file path, find the date to start extracting data with
    Default: today """

    ## if there's no data, get the last day
    first_date = "1 day"
    ## get the largest query time in the data warehouse
    date_object = find_existing_data(f"looker/{query_name}/looker_{query_name}")
    last_date = "1990-01-01 00:00:00"
    for last_date_object in date_object:
        for row in last_date_object:
            last_date = max(last_date, row[datetime_index])
    if last_date is None or last_date == [] or last_date == "1990-01-01":
        logging.error(f"No date found; running with {first_date}")
        return first_date
    else:
        times = []
        times = find_date_range(last_date)
        if times is None or times == []:
            raise ValueError("No valid time range found")
        return f"""{times[0].strftime('%Y-%m-%d %H:%M:%S')} 
                    to {times[1].strftime('%Y-%m-%d %H:%M:%S')}"""

def find_date_range(start_time):
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    hours_old = (
        datetime.now() - start_time
    ).total_seconds() // 3600
    ## given it a ten minute time difference
    if hours_old <= 0.16:
        logging.warning("All up to date, not running any data")
        return None
    hours_old = min(int(hours_old) + 1, 24)
    end_time = start_time + timedelta(hours=hours_old, minutes=0)
    logging.info(f"{start_time} to {end_time}")
    return [start_time, end_time]


def extract_data(json_filename):
    """ Read in the JSON file, iterate through query info,
    run the queries, and write to s3. If no data, don't do SQL parts
    It takes one argument: the filename of the JSON file in this folder
    that holds the query info"""

    sdk = looker_sdk.init31()
    queries = extract_query_details(json_filename)
    REQUIRED_KEYS = ["name", "model", "explore", "fields"]
    for query_body in queries:
        
        for key in REQUIRED_KEYS:
            if query_body.get(key) is None:
                raise KeyError(f"{key} is a mandatory element in the JSON")

        query_name = query_body["name"]
        file_name = f"looker_{query_name}_{NOW}"
        filters = query_body.get("filters")
        metadata = query_body.get("metadata")
        datetime_index = metadata.get("datetime")
        row_limit = query_body.get("limit")
        result_format = metadata.get("result_format") or "json"

        if result_format not in ["json", "csv"]:
            raise ValueError("Invalid instance type; please use only json or csv")
    
        ## if the filter already exists, dont run it
        ## if there's no datetime, don't run it
        if not (datetime_index is None or filters.get(datetime_index) is not None):
            date_filter = find_last_date(query_name, datetime_index)
            filters[datetime_index] = f"{date_filter}"

        ## hit the Looker API
        write_query = looker_sdk.models.WriteQuery(
            model=query_body["model"],
            view=query_body["explore"],
            fields=query_body["fields"],
            filters=filters,
            sorts=query_body.get("sorts"),
            limit=row_limit,
            force_production=True
        )

        query_run = sdk.run_inline_query(result_format, write_query)
        data = json.loads(query_run)

        if data == []:
            logging.error(
                f"No data returned when attempting to fetch Looker query history for {date_filter}"
            )
        elif data[0].get("looker_error") is not None:
            logging.error(
                f"""Looker query history fetch failed with {data[0].get("looker_error")}"""
            )
        elif len(data) == row_limit:
            logging.error(
                f"""Hit the limit of {row_limit} rows, try again a smaller window than {date_filter} """
            )
        else:
            load_object_to_s3(data, file_name, f"looker/{query_name}/{file_name}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--json_file', type=str,
                    help='the JSON file location that contains the data to run the Looker query or queries')
    parser.add_argument('--sum', dest='accumulate', action='store_const',
                    const=sum, default=max,
                    help='sum the integers (default: find the max)')

    args = parser.parse_args()

