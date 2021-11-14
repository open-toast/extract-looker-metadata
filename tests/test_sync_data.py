import sys
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(os.path.dirname(os.path.dirname(currentdir)))
sys.path.append(parentdir)

from looker_ingestion import sync_data
from datetime import datetime, timedelta

def test_extract_query_details():
    test_query_name, test_query_body = sync_data.extract_query_details("../tests/test_query.json")

    assert test_query_name == "test_query_name"
    assert test_query_body["model"] == "test_model"
    assert test_query_body["sorts"][1] == "sort_by_name"


def test_find_date_range():
    def convert_string(actual_datetime):
        return actual_datetime.strftime('%Y-%m-%d %H:%M:%S')

    today = datetime.now()
    one_day_ago = today - timedelta(days = 1)
    two_days_ago = today - timedelta(days = 2)
    fortynine_days_ago = today - timedelta(days = 49)
    fifty_days_ago = today - timedelta(days = 50)
    today_string = today.strftime('%Y-%m-%d %H:%M:%S')

    assert sync_data.find_date_range(today_string) == None
    assert sync_data.find_date_range(convert_string(one_day_ago)) == one_day_ago, today
    assert sync_data.find_date_range(two_days_ago) == two_days_ago, one_day_ago
    assert sync_data.find_date_range(fifty_days_ago) == fifty_days_ago, fortynine_days_ago