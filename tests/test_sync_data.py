import sys
import os
import pytest

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
looker_ingestion_dir = os.path.join(parentdir, "looker_ingestion")
sys.path.append(parentdir)
sys.path.append(looker_ingestion_dir)

from looker_ingestion import sync_data, NoDataException
from datetime import datetime, timedelta


def test_extract_data():
    """Tests the extract_data() function to ensure that, if invalid JSON schemas
    are given, a specific error is returned to the user"""

    ## trying to use parquet instead of csv/json
    with pytest.raises(ValueError):
        sync_data.extract_data("../tests/invalid_query.json")

    ## not sending query name (a required key)
    with pytest.raises(KeyError):
        sync_data.extract_data("../tests/missing_keys.json")

    ## invalid JSON (missing comma)
    with pytest.raises(ValueError):
        sync_data.extract_data("../tests/invalid_json.json")

    ## does not have sort when incremental
    ## or has incorrect sort order
    with pytest.raises(ValueError):
        sync_data.extract_data("../tests/missing_sort.json")
        sync_data.extract_data("../tests/incorrect_sort_order.json")


def test_extract_query_details():
    """Ensures that the script can read in a JSON file and produce the
    right attributes from the JSON."""

    test_queries = sync_data.extract_query_details("../tests/test_queries.json")

    assert test_queries[0]["name"] == "test_query_name"
    assert test_queries[0]["model"] == "test_model"
    assert test_queries[0]["sorts"][1] == "sort_by_name"
    assert test_queries[0]["metadata"]["result_format"] == "json"
    assert test_queries[1]["metadata"]["result_format"] == "csv"

    test_query = sync_data.extract_query_details("../tests/test_query.json")
    assert test_query[0]["name"] == "test_singular_query"


def test_find_date_range():
    """
    This test ensures that, given the right last date of data, the script will pick
    the right range to query Looker with.
    """

    def convert_string(actual_datetime):
        return actual_datetime.strftime("%Y-%m-%d %H:%M:%S")

    today = datetime.now().replace(microsecond=0)
    one_day_ago = (today - timedelta(days=1)).replace(microsecond=0)
    two_days_ago = (today - timedelta(days=2)).replace(microsecond=0)
    fortynine_days_ago = (today - timedelta(days=49)).replace(microsecond=0)
    fifty_days_ago = (today - timedelta(days=50)).replace(microsecond=0)
    today_string = today.strftime("%Y-%m-%d %H:%M:%S")

    with pytest.raises(NoDataException):
        sync_data.find_date_range(today_string)
    assert sync_data.find_date_range(convert_string(one_day_ago)) == [
        one_day_ago,
        today,
    ]
    assert sync_data.find_date_range(convert_string(two_days_ago)) == [
        two_days_ago,
        one_day_ago,
    ]
    assert sync_data.find_date_range(convert_string(fifty_days_ago)) == [
        fifty_days_ago,
        fortynine_days_ago,
    ]
