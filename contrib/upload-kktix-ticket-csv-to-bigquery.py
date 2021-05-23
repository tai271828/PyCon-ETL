#!/usr/bin/env python3
import argparse
import re

import pandas as pd
from google.cloud import bigquery


def upload_dataframe_to_bigquery(df, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.dataset.DatasetReference(project_id, dataset_name)
    table_ref = bigquery.table.TableReference(dataset_ref, table_name)

    # dump the csv into bigquery
    job = client.load_table_from_dataframe(df, table_ref)

    job.result()

    print(
        "Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name)
    )


def reserved_alphabet_space(string_as_is):
    regex = re.compile("[^a-zA-Z 0-9]")
    return regex.sub("", string_as_is)


def reserved_only_one_space_between_words(string_as_is):
    string_as_is = string_as_is.strip()
    # two or more space between two words
    # \w : word characters, a.k.a. alphanumeric and underscore
    match = re.search("\w[ ]{2,}\w", string_as_is)

    if not match:
        return string_as_is

    regex = re.compile("\s+")
    string_as_is = regex.sub(" ", string_as_is)

    return string_as_is


def sanitize_column_name(column_name):
    column_name = reserved_alphabet_space(column_name)
    column_name = reserved_only_one_space_between_words(column_name)
    column_name = column_name.replace(" ", "_")
    column_name = column_name.lower()

    return column_name


def sanitize_column_names(df):
    sanitized_columns = {}
    for column in df.columns:
        sanitized_column = sanitize_column_name(column)
        sanitized_columns[column] = sanitized_column

    return df.rename(columns=sanitized_columns)


def main():
    """
    Commandline entrypoint
    """
    parser = argparse.ArgumentParser(
        description="Sanitize ticket CSV and upload to BigQuery"
    )

    parser.add_argument(
        "csv_file", type=str, help="Ticket CSV file",
    )

    parser.add_argument("-p", "--project-id", help="BigQuery project ID")

    parser.add_argument(
        "-d", "--dataset-name", help="BigQuery dataset name to create or append"
    )

    parser.add_argument(
        "-t", "--table-name", help="BigQuery table name to create or append"
    )

    parser.add_argument(
        "--upload", action="store_true", help="Parsing the file but not upload it", default=False
    )

    args = parser.parse_args()

    # load the csv into bigquery
    df = pd.read_csv(args.csv_file)
    sanitized_df = sanitize_column_names(df)

    if args.upload:
        upload_dataframe_to_bigquery(
            sanitized_df, args.project_id, args.dataset_name, args.table_name
        )
    else:
        print("Dry-run mode. Data will not be uploaded.")
        print("Column names (as-is):")
        print(df.columns)
        print("Column names (to-be):")
        print(sanitized_df.columns)

    return sanitized_df.columns


if __name__ == "__main__":
    main()
