import boto3
import os
from moto import mock_s3
import sys
import json 

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
looker_ingestion_dir = os.path.join(parentdir, "looker_ingestion")
sys.path.append(parentdir)
sys.path.append(looker_ingestion_dir)

from looker_ingestion import load_s3

@mock_s3
def test_load_object_to_s3():
    pass
    ##load_s3.load_object_to_s3(data, local_file_name, output_filename, s3_bucket)

@mock_s3
def test_find_existing_data():
    file_contents_json = [{"query.id": 4845015, "history.created_time": "2021-12-02 14:40:18"}, 
                            {"query.id": 4845015, "history.created_time": "2021-12-02 14:40:20"}]
    second_file_contents_json = [{"query.id": 4845015, "history.created_time": "2021-12-02 14:40:22"}, 
                                {"query.id": 4845015, "history.created_time": "2021-12-02 14:40:24"}]

    file_contents_csv = """Query ID, History Created Time
    845015,2021-12-02 14:40:18
    845015,2021-12-02 14:40:20"""
    second_file_contents_csv = """Query ID, History Created Time
    845015,2021-12-02 14:40:22
    845015,2021-12-02 14:40:24"""
    first_csv_results = [{"query id": 845015, "history created time": "2021-12-02 14:40:18"}, 
                    {"query id": 845015, "history created time": "2021-12-02 14:40:20"}]
    second_csv_results = [{"query id": 845015, "history created time": "2021-12-02 14:40:22"}, 
                    {"query id": 845015, "history created time": "2021-12-02 14:40:24"}
                    ]
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="databucket")
    
    s3.put_object(Bucket="databucket", Key="json/looker_output.json", Body=json.dumps(file_contents_json))
    assert load_s3.find_existing_data("json/looker_output.json", "databucket") == file_contents_json
    s3.put_object(Bucket="databucket", Key="json/looker_output2.json", Body=json.dumps(second_file_contents_json))
    assert load_s3.find_existing_data("json/looker_output", "databucket") == file_contents_json + second_file_contents_json

    s3.put_object(Bucket="databucket", Key="csv/looker_output.csv", Body=file_contents_csv)
    assert load_s3.find_existing_data("csv/looker_output.csv", "databucket") == first_csv_results
    s3.put_object(Bucket="databucket", Key="csv/looker_output2.csv", Body=second_file_contents_csv)
    assert load_s3.find_existing_data("csv/looker_output", "databucket") == first_csv_results + second_csv_results