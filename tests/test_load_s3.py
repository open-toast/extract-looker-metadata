import boto3
import os
from moto import mock_s3
import sys

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
looker_ingestion_dir = os.path.join(parentdir, 'looker_ingestion')
sys.path.append(parentdir)
sys.path.append(looker_ingestion_dir)

from looker_ingestion import load_s3

@mock_s3
def test_find_existing_data():
    file_contents_json = """[{'query.id': 4845015, 'history.created_time': '2021-12-02 14:40:18'}, 
                                {'query.id': 4845015, 'history.created_time': '2021-12-02 14:40:18'}]"""
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='databucket')
    
    s3.put_object(Bucket='databucket', Key='json/looker_output.json', Body=file_contents_json)
    assert load_s3.find_existing_data('json/looker_output.json', 'databucket') == file_contents_json