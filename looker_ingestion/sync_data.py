import looker_sdk
from load_s3 import load_s3_json, read_json
import time 
import json
import os
import logging
from datetime import timedelta
import sys

PARENT_PATH = os.path.dirname(__file__)
LIMIT = 60000
NOW = str(time.time()).split(".")[0]

def import_json(json_file_path):
    """ Gets the query/body for each query from the JSON file """
    with open(json_file_path, "r") as json_file:
        uploaded_tables = json.load(json_file)
    return uploaded_tables

def find_last_date(query_name, datetime_index):
    """ For the relevant file path, find the date to start extracting data with 
    Default: today """
    
    ## if there's no data, get the last day
    first_date  = '1 day'
    ## get the largest query time in the data warehouse
    try:
        last_date_object = read_json(f'looker/{query_name}/looker_{query_name}')
        last_date = max(last_date_object[f"'{datetime_index}'"])
    except Exception as e:
        logging.info(f"Received error {e} when trying to extact date from table; running with {first_date}")
        return first_date
    if last_date is None or last_date == []:
        logging.info(f"No date found; running with {first_date}")
        return first_date
    start_time = last_date
    hours_old = (NOW - last_date).seconds // 3600
    ## given it a ten minute time difference
    if hours_old <= 0.16:
        logging.warn("All up to date!")
        sys.exit(0)
    hours_old = min(int(hours_old) + 1, 24)
    end_time = start_time + timedelta(hours=hours_old, minutes=0)
    logging.info(f"{start_time} to {end_time}")
    return  f"{start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}"

def extract_data(json_filename):
    """ Read in the JSON file, iterate through query info, 
    run the queries, and write to s3. If no data, don't do SQL parts 
    It takes one argument: the filename of the JSON file in this folder
    that holds the query info"""

    sdk = looker_sdk.init31()
    json_file_path = os.path.join(PARENT_PATH, json_filename)
    query = import_json(json_file_path)
    query_name = list(query.keys())[0]
    file_name = f"looker_{query_name}_{NOW}"
    this_query = query[query_name]
    body = this_query.get('body')
    model = body.get('model')
    fields = body.get('fields')
    filters = body.get('filters')
    sorts = body.get('sorts')
    datetime_index = body.get('datetime')
    view = body.get('view')
    ## if the filter already exists, dont run it
    ## if there's no datetime, don't run it
    if not (datetime_index is None or filters.get(datetime_index) is not None):
        date_filter = find_last_date(query_name, datetime_index)
        filters[datetime_index] = f'{date_filter}'

    ## hit the Looker API
    write_query = looker_sdk.models.WriteQuery(model=model, view=view, fields=fields, filters=filters, sorts=sorts, limit=LIMIT)
    query_run = sdk.run_inline_query("json", write_query)
    data = json.loads(query_run)

    if data == []:
        logging.error(f"No data returned when attempting to fetch Looker query history for {date_filter}")
    elif data[0].get("looker_error") is not None:
        logging.error(f"""Looker query history fetch failed with {data[0].get("looker_error")}""")
    elif len(data) == LIMIT:
        logging.error(f"""Hit the limit of {LIMIT} rows, try again a smaller window than {date_filter} """)
    else:
        load_s3_json(data, file_name, f'looker/{query_name}/{file_name}')