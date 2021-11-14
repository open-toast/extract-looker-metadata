import looker_ingestion
from datetime import datetime, timedelta

def test_extract_query_details():
    test_query_name, test_query_body = looker_ingestion.extract_query_details("test_query.json")

    assert test_query_name == "test_query_name"
    assert test_query_body["model"] == "test_model"
    assert test_query_body["sorts"][1] == "sort_by_name"


def test_find_date_range():
    today = datetime.datetime.now()
    one_day_ago = today - timedelta(days = 1)
    two_days_ago = today - timedelta(days = 2)
    fortynine_days_ago = today - timedelta(days = 49)
    fifty_days_ago = today - timedelta(days = 50)

    assert looker_ingestion.find_date_range(datetime.now()) == None
    assert looker_ingestion.find_date_range(one_day_ago) == one_day_ago, today
    assert looker_ingestion.find_date_range(two_days_ago) == two_days_ago, one_day_ago
    assert looker_ingestion.find_date_range(fifty_days_ago) == fifty_days_ago, fortynine_days_ago