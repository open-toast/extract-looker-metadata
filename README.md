<!-- Might be worth running an auto format on this to split lines and make it a bit easier to read without scrolling left and right. Intellij will format it nicely. -->
# Looker Metadata Extractor

This project takes a JSON file of information about a Looker query and runs it on a Looker instance and sends the results to an S3 bucket.

## Getting Started

### Prerequisites

[Looker SDK](https://docs.looker.com/reference/api-and-integration/api-sdk) and credentials. Configure your [Looker variables](https://github.com/looker-open-source/sdk-codegen#configuring-lookerini-or-env)
[S3 credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) and [boto3](https://pypi.org/project/boto3/)
In addition to AWS creds, bucket name should be an environment variable "bucket_name"

### Installing

```sql
python -m pip install looker_ingestion
```

* Fill out either the looker.ini or store your Looker credentials as environment variables
* Ensure you have credentials that can access your S3 bucket
* Store both the creds and the bucket name as an environmental variable

You can use the function directly:

```python
from looker_ingestion import sync_data

sync_data.extract_data('query_history_hourly.json')
```

You can call the function as part of an Airflow task:

```python
from looker_ingestion import sync_data
from airflow.operators.python_operator import PythonOperator

with DAG(
    'extract_weekly_data',
    default_args=default_args,
    catchup=False
) as dag:
    RUN_CUSTOM_HISTORY_WEEKLY_QUERIES = PythonOperator(
        task_id="run_custom_history_weekly_queries",
        python_callable=sync_data.extract_data,
        op_kwargs={'json_filename': 'weekly_query_history.json'}
    )
```

### Adding a custom extraction

If there are unavailable objects or another prohibition from getting data, you can write a query against i_looker and extract data with that query. You can also query any other model.
The explores available in the i__looker model are:

* History
* Look
* Dashboard
* User
* Event
* Event Attribute
* Field Usage

The fields to fill out in the JSON file are:

**query_history_weekly**: whatever you want to call this query, it will definitely be the table name so choose wisely

**body.model**: the name of the model you want to extract from, should be in the URL of your query

**body.view**: the name of the explore you’re using, circled in red. yes, they call an explore a view throughout all the Looker API / database.

**body.fields**: a list of the fields you want in the form <table name>.<field name>. Note to use the names from SQL which may vary from the sidebar. In addition, you can't do calculations/custom fields unless they’re already made.

**body.filters**: A list of filters you want to see. Use this format

**body.sorts**: Not shown, but a list of fields you want to sort by. ASC by default, you can also put DESC

**datetime**: This is effectively “what field do you want to use so we can extract only new data?” Extracting all data is probably unrealistic because of time and row limits. If this field also exists in your filters, we will defer to the filter value. If not, we will calculate the next chunk of data we can bring in

In order to do incremental, we find the MAX(datetime) found in S3 and then add up to 24 hours to that, stopping if we reach the current time. We also have a row limit of 60000 and a time limit of 5 minutes (adjustable as a Looker env variable). If it hits the row limit, you get all the data it has pulled so far (a good reason to use sorts), but if you hit the timeout limit you get nothing. For this reason, we like to keep the increments small.

If there is an error or no new rows are found or the row limit is reached, the script will log an error.

## Running the tests

to run the tests:

```bash
pytest tests/
```

### test_extract_query_details

This test ensures that the script can read in a JSON file and produce the
right attributes from the JSON.

### test_find_date_range

This test ensures that, given the right last date of data, the script will pick
the right range to query Looker with.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct,
and the process for submitting pull requests to us.


## Versioning

See [CHANGELOG.md](CHANGELOG.md) for a history of changes to this repo.

## Authors

[Alisa Aylward](mailto:alisa.aylward@toasttab.com)

## License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

The idea to use a custom query to get Looker metadata came from [Singer Looker Tap](https://github.com/singer-io/tap-looker)