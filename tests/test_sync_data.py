import sys
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
looker_ingestion_dir = os.path.join(parentdir, 'looker_ingestion')
sys.path.append(parentdir)
sys.path.append(looker_ingestion_dir)

from looker_ingestion import sync_data
from datetime import datetime, timedelta

def test_extract_query_details():
    test_query = sync_data.extract_query_details("../tests/test_query.json")

    assert test_query["name"] == "test_query_name"
    assert test_query["body"]["model"] == "test_model"
    assert test_query["body"]["sorts"][1] == "sort_by_name"
    assert test_query["metadata"]["result_format"] == "json"

def test_find_date_range():
    def convert_string(actual_datetime):
        return actual_datetime.strftime('%Y-%m-%d %H:%M:%S')

    today = datetime.now().replace(microsecond=0)
    one_day_ago = (today - timedelta(days = 1)).replace(microsecond=0)
    two_days_ago = (today - timedelta(days = 2)).replace(microsecond=0)
    fortynine_days_ago = (today - timedelta(days = 49)).replace(microsecond=0)
    fifty_days_ago = (today - timedelta(days = 50)).replace(microsecond=0)
    today_string = today.strftime('%Y-%m-%d %H:%M:%S')

    assert sync_data.find_date_range(today_string) == None
    assert sync_data.find_date_range(convert_string(one_day_ago)) == [one_day_ago, today]
    assert sync_data.find_date_range(convert_string(two_days_ago)) == [two_days_ago, one_day_ago]
    assert sync_data.find_date_range(convert_string(fifty_days_ago)) == [fifty_days_ago, fortynine_days_ago]