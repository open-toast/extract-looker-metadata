# Looker Metadata Extractor

This is a data infrastructure tool that extracts the results of any valid query against Looker,  a popular data visualization tool, and stores the results in S3. In the tool itself, metadata can be queried and manually extracted. However, not all objects that are available in the front end are exposed via the API. Instead, the data can be accessed by writing a custom query against the relevant Looker object. For instance, query history is not an API object that one can access directly, but the data can be extracted with a custom query.
For definitions of terms used in this readme, please see the Terminology section.

## Getting Started

### Prerequisites

This tool can be run as from the command on Linux or macOS, or imported as a package and used in a Python script.
You need access to your Looker instance's API client_id and client_secret. credentials. Follow the directions Configure your Looker variables in in the Looker documentation for how to access the Looker API from the environment that you plan to run this script in.
You need AWS credentials (aws_access_key_id, aws_secret_access_key) that can write to the bucket where you want to store the results. These can be stored according to the instructions here.
and boto3 In addition to AWS credentialscreds, the name ofbucket the bucket that you want to write to name should be an environment variable called "bucket_name", for instance:

```export bucket_name="s3://intended-bucket"```

### Installing

Run the following in your terminal:

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
You can also call this on the command line:

```bash
python sync_data.py --json_file /usr/extract-looker-metadata/looker_ingestion/my_looker_query.json
```

### Terminology

The following Looker terms are referenced throughout the project:

* Model and View
  * A model refers to a file in Looker that defines a single database connection and a collection of Explores to run on that database. The model that we use in the example here is [i__looker](https://docs.looker.com/admin-options/tutorials/i__looker).
  * More information [here](https://docs.looker.com/data-modeling/getting-started/model-development)
* View
  * A view is a file in Looker of a single database table or a derived table
  * A view is not referenced explicitly in this project; instead, field names begin with their view name
  * More information [here](https://docs.looker.com/data-modeling/getting-started/model-development#view_files)
* Explore
  * Explores exist within a model and can be one to many views joined
  * This called a view throughout the Looker database/API
  * More information [here](https://docs.looker.com/reference/explore-params/explore)

### Define a custom extraction

The query to extract Looker data is generated using a JSON object. Create a JSON with one to many valid query objects (defined below) and pass the absolute location of the file as an argument to the script, and the script will run that query against Looker.

### Example custom extraction file

An example JSON file looks like this. Note that it is referencing the [i__looker](https://docs.looker.com/admin-options/tutorials/i__looker) model, which contains event data about users interactions with Looker.

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

### Custom extraction JSON value reference

The fields to fill out in the JSON file are:

**name**: whatever you want to call this query; this will be used to store and reference this specific query in S3

**model**: the name of the Looker model you want to extract from, should be in the URL of your query, e.g. i__looker

**explore**: the name of the explore you're using to generate this query, should be in the URL of your query,e.g. history

**fields**: a list of the fields you want in the form table name.field name. Note to use the names from SQL which may vary from the sidebar. In addition, you can't do calculations/custom fields unless they're already made.

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

**metadata.default_days**: If using a datetime to create an incremental extraction, this is the default number of days to filter for when doing the first extraction. Please enter only the number of days, e.g. "4".
If invalid, such as "4 days" instead of "4", it will run with a default of 1 day

**metadata.result_format**: The format that the results can be retrieved as, all supported are listed [here](https://docs.looker.com/reference/api-and-integration/api-reference/v3.1/query). However, because of the original use of this project is for databases, this project only supports JSON and CSV.
Note: although you can run a query with a CSV format and then change it to JSON or vice versa, these will write to different folders and therefore restart the incremental date if using incrementals.

If there is an error or no new rows are found or the row limit is reached, the script will log an error.

## Running the tests

To run the tests, please ensure you have cloned this repository. In addition, please install all requirements by running this in your terminal at the top of this repository:

```pip install -r requirements.txt```

This should install pytest, which is necessary to run these tests. Once completed, use the following command under the top directory of this repository (extract-looker-metadata) on your terminal:

```pytest tests/```

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