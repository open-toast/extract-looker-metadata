# Looker Metadata Extractor

This project takes a JSON file of information about a Looker query and runs it on a Looker instance and sends the results to an S3 bucket. This uses the Looker SDK to query the Looker API, translating to this call:

`POST/api/3.1/queries/run/{result_format}`

## Getting Started

### Prerequisites

[Looker SDK](https://docs.looker.com/reference/api-and-integration/api-sdk) and credentials. Configure your [Looker variables](https://github.com/looker-open-source/sdk-codegen#configuring-lookerini-or-env)
[S3 credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) and [boto3](https://pypi.org/project/boto3/)
In addition to AWS creds, bucket name should be an environment variable "bucket_name"

### Installing

```bash
python -m pip install looker_ingestion
```

* Fill out either the looker.ini or store your Looker credentials as environment variables
* Ensure you have credentials that can access your S3 bucket
* Store both the creds and the bucket name as an environmental variable
* You can also pass your AWS credentials on the command line instead of using enviromental variables

You can use the function directly:

```python
from looker_ingestion import sync_data

sync_data.extract_data('my_looker_query.json')
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
        op_kwargs={'json_filename': 'my_looker_query.json'}
    )
```

You can also call this on the command line:

```bash
python sync_data.py --json_file /usr/extract-looker-metadata/looker_ingestion/my_looker_query.json
```

### Terminology

The following Looker terms are referenced throughout the project:

* Model and View
  * A model refers to a file in Looker that defines a single database connection and a collection of Explores to run on that database
  * More information [here](https://docs.looker.com/data-modeling/getting-started/model-development)
* View
  * A view is a file in Looker of a single database table or a derived table
  * A view is not referenced explicitly in this project; instead, field names begin with their view name
  * More information [here](https://docs.looker.com/data-modeling/getting-started/model-development#view_files)
* Explore
  * Explores exist within a model and can be one to many views joined
  * This called a view throughout the Looker database/API
  * More information [here](https://docs.looker.com/reference/explore-params/explore)

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

This query is generated using a JSON object. Each file can have one or many JSON objects.

An example JSON file looks like this:

```json
[{
    "name": "query_history",
    "model": "i__looker",
    "explore": "history",
    "fields": [
        "query.id",
        "history.created_time",
        "query.model",
        "query.view",
        "space.id",
        "look.id",
        "dashboard.id",
        "user.id",
        "query.fields",
        "history.id",
        "history.message",
        "history.dashboard_id",
        "query.filter_expression",
        "query.filters",
        "query.filter_config"
    ],
    "filters": {
        "query.model": "-EMPTY",
        "history.runtime": "NOT NULL",
        "user.is_looker": "No"
    },
    "sorts": [
        "history.created_time"
    ],
    "limit": "10000",
    "metadata": {
        "datetime": "history.created_time",
        "default_days": "4",
        "result_format": "csv"
    }
},
{
    "name": "query_history",
    "model": "i__looker",
    "explore": "history",
    "fields": [
        "query.id",
        "history.created_time",
        "query.model",
        "query.view",
        "space.id",
        "look.id",
        "dashboard.id",
        "user.id",
        "query.fields",
        "history.id",
        "history.message",
        "history.dashboard_id",
        "query.filter_expression",
        "query.filters",
        "query.filter_config"
    ],
    "filters": {
        "query.model": "-EMPTY",
        "history.runtime": "NOT NULL",
        "user.is_looker": "No"
    },
    "sorts": [
        "history.created_time"
    ],
    "limit": "5000",
    "metadata": {
        "datetime": "history.created_time",
        "default_days": "1",
        "result_format": "json"
    }
}
]
```

The fields to fill out in the JSON file are:

**name**: whatever you want to call this query; this will be used to store and reference this specific query in S3

**model**: the name of the Looker model you want to extract from, should be in the URL of your query, e.g. i__looker

**explore**: the name of the explore you’re using to generate this query, should be in the URL of your query,e.g. history

**fields**: a list of the fields you want in the form table name.field name. Note to use the names from SQL which may vary from the sidebar. In addition, you can't do calculations/custom fields unless they’re already made.

**filters**: A dictionary of filters you want to see. Use this format:

```python
{"field": "filter_expression",
"field2": "filter_expression2"}
```

For the format of Looker filters, see this [page](https://docs.looker.com/reference/filter-expressions)

**sorts**: A list of fields you want to sort by. ASC by default, you can also put DESC to sort by descending

**limits**: The row limit enforced on the query. If the row limit is reached, the data will still return, but only up to the row limit amount. Default is 5000.

**metadata.datetime**: In order to allow for incremental extractions, this field functions as the field that time ranges can be filtered on. This is effectively “what field do you want to use so we can extract only new data?” Extracting all data is probably unrealistic because of time and row limits. If this field also exists in your filters, we will defer to the filter value. If not, we will calculate the next chunk of data we can bring in

In order to do incremental, we find the MAX(datetime) found in S3 and then add up to 24 hours to that, stopping if we reach the current time. We also have a row limit of 60000 and a time limit of 5 minutes (adjustable as a Looker env variable). If it hits the row limit, you get all the data it has pulled so far (a good reason to use sorts), but if you hit the timeout limit you get nothing. For this reason, we like to keep the increments small.

**metadata.datetime**: If using a datetime to create an incremental extraction, this is the default number of days to filter for when doing the first extraction. Please enter only the number of days, e.g. "4".
If invalid, such as "4 days" instead of "4", it will run with a default of 1 day

**metadata.result_format**: The format that the results can be retrieved as, all supported are listed [here](https://docs.looker.com/reference/api-and-integration/api-reference/v3.1/query). However, because of the original use of this project is for databases, this project only supports JSON and CSV.
Note: although you can run a query with a CSV format and then change it to JSON or vice versa, these will write to different folders and therefore restart the incremental date if using incrementals.

If there is an error or no new rows are found or the row limit is reached, the script will log an error.

## Running the tests

to run the tests:

```bash
pytest tests/
```

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
Made possible by the [Looker SDK](https://github.com/looker-open-source/sdk-codegen)