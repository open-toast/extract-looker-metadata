# Changelog

## [1.4.0] - 2023-04-13

- Paginating calls to the s3 `list_objects_v2`

## [1.3.0] - 2023-04-13

- Update to use Looker API 4.0

## [1.2.0] - 2022-09-01

- Add support for overriding default S3 prefix and filename
- Return list of files uploaded to S3
- Stream files directly to S3 instead of using temporary file

## [1.1.5] - 2022-08-31

- Return an `NoDataException` if all data has been processed
- Add additional examples include example Airflow DAG

## [1.1.4] - 2022-04-27

- Bug fix: Import error

## [1.1.3] - 2022-04-27

- Find the most recent S3 file when parsing to save memory

## [1.1.2] - 2022-01-03

- Bug fix: doesn't run the filter at the right time

## [1.1.1] - 2022-01-03

- Add Github URL to the PyPi page

## [1.1.0] - 2022-01-03

- Initial release
