#!/usr/bin/env python3
import re
import pandas as pd
from google.cloud import bigquery


PROJECT_ID = "pycontw-225217"


def upload_dataframe_to_bigquery(df, dataset_name, table_name, project_id=PROJECT_ID):
    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.dataset.DatasetReference(PROJECT_ID, dataset_name)
    table_ref = bigquery.table.TableReference(dataset_ref, table_name)

    # dump the csv into bigquery
    job = client.load_table_from_dataframe(df, table_ref)

    job.result()

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name))


def sanitize_column_name(column_name):
    regex = re.compile('[^a-zA-Z]')
    return regex.sub("", column_name)


def sanitize_column_names(df):
    sanitized_columns = {}
    for column in df.columns:
        sanitized_column = sanitize_column_name(column)
        sanitized_columns[column] = sanitized_column

    return df.rename(columns=sanitized_columns)


def main():
    #csv_file = "/home/tai271828/work-my-projects/pycontw-projects/post-events/2020/data/tickets/without-identities
    # -rm-cols-for-bigquery-upload-01-sed-reedit-column-field-title/corporate-attendees.csv"
    csv_file = "/home/tai271828/work-my-projects/pycontw-projects/PyCon-ETL-working/tmp.csv"
    dataset_name = "ods"
    table_name = "tai_test"

    # load the csv into bigquery
    with open(csv_file, "rb") as source_file:
        df = pd.read_csv(csv_file)
        sanitized_df = sanitize_column_names(df)
        upload_dataframe_to_bigquery(sanitized_df, dataset_name, table_name)


if __name__ == "__main__":
    main()
