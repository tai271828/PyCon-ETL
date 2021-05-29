#!/usr/bin/env python3
import argparse
import re

import unittest

import pandas as pd
from google.cloud import bigquery


CANONICAL_COLUMN_NAMES = [
    'ticket_type',
    'payment_status',
    'tags',
    'paid_date',
    'price',
    'invoice_policy',
    'invoiced_company_name_optional',
    'unified_business_no_optional',
    'dietary_habit',
    'years_of_using_python',
    'area_of_interest',
    'organization',
    'job_role',
    'country_or_region',
    'departure_from_region',
    'how_did_you_know_pycon_tw',
    'have_you_ever_attended_pycon_tw',
    'do_you_know_we_have_financial_aid_this_year',
    'gender',
    'pynight_attendee_numbers',
    'pynight_attending_or_not',
    'email_from_sponsor',
    'email_to_sponsor',
    'privacy_policy_of_pycon_tw',
    'ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw'
]

HEURISTIC_COMPATIBLE_MAPPING_TABLE = {
    # from 2020 reformatted column names
    'years_of_using_python_python': "years_of_using_python",
    'company_for_students_or_teachers_fill_in_the_school_department_name': "organization",
    'job_title_if_you_are_a_student_fill_in_student': "job_role",
    'come_from': "country_or_region",
    'departure_from_regions': "departure_from_region",
    'how_did_you_find_out_pycon_tw_pycon_tw': "how_did_you_know_pycon_tw",
    'have_you_ever_attended_pycon_tw_pycon_tw': "have_you_ever_attended_pycon_tw",
    'privacy_policy_of_pycon_tw_2020_pycon_tw_2020_bitly3eipaut': "privacy_policy_of_pycon_tw",
    'ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw_2020':
        "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    # from 2020 reformatted column names which made it duplicate
    "PyNight 參加意願僅供統計人數，實際是否舉辦需由官方另行公告": "pynight_attendee_numbers",
    "PyNight 參加意願": "pynight_attending_or_not",
    "是否願意收到贊助商轉發 Email 訊息": "email_from_sponsor",
    "是否願意提供 Email 給贊助商": "email_to_sponsor",
}


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


def reserved_alphabet_space_underscore(string_as_is):
    regex = re.compile("[^a-zA-Z 0-9_]")
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


def get_reformatted_style_columns(columns):
    reformatted_columns = {}
    for key, column_name in columns.items():
        reformatted_column_name = reserved_alphabet_space_underscore(column_name)
        reformatted_column_name = reserved_only_one_space_between_words(reformatted_column_name)
        reformatted_column_name = reformatted_column_name.replace(" ", "_")
        reformatted_column_name = reformatted_column_name.lower()

        reformatted_columns[key] = reformatted_column_name

    return reformatted_columns


def find_reformat_none_unique(columns):
    # reverse key-value of original dict to be value-key of reverse_dict
    reverse_dict = {}

    for key, value in columns.items():
        reverse_dict.setdefault(value, set()).add(key)

    result = [key for key, values in reverse_dict.items() if len(values) > 1]

    print(f"Found the following duplicate column names: {result}")
    return result


def apply_compatible_mapping_name(columns):
    """Unify names with a heuristic hash table"""
    updated_columns = apply_heuristic_name(columns)

    return updated_columns


def apply_heuristic_name(columns):
    updated_columns = dict(columns)

    for candidate in HEURISTIC_COMPATIBLE_MAPPING_TABLE.keys():
        for key, value in columns.items():
            if candidate == value:
                candidate_value = HEURISTIC_COMPATIBLE_MAPPING_TABLE[candidate]
                updated_columns[key] = candidate_value

    return updated_columns


def init_rename_column_dict(columns_array):
    columns_dict = {}

    for item in columns_array:
        columns_dict[item] = item

    return columns_dict


def sanitize_column_names(df):
    """
    Pre-process the column names of raw data

    Pre-checking rules of column name black list and re-formatting if necessary.

    The sanitized pre-process of data should follow the following rules:
        1. style of column name (which follows general SQL conventions)
        1-1. singular noun
        1-2. lower case
        1-3. snake-style (underscore-separated words)
        1-4. full word (if possible) except common abbreviations
        2. a column name SHOULD be unique
        3. backward compatible with column names in the past years
    """
    rename_column_dict = init_rename_column_dict(df.columns)

    # apply possible heuristic name if possible
    # this is mainly meant to resolve style-reformatted names duplicate conflicts
    applied_heuristic_columns = apply_heuristic_name(rename_column_dict)

    # pre-process of style of column name
    style_reformatted_columns = get_reformatted_style_columns(applied_heuristic_columns)
    df.rename(columns=style_reformatted_columns)

    # pre-process of name uniqueness
    duplicate_column_names = find_reformat_none_unique(style_reformatted_columns)

    # pre-process of backward compatibility
    compatible_columns = apply_compatible_mapping_name(style_reformatted_columns)

    return df.rename(columns=compatible_columns)


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
        print("")
        print("Column names (to-be):")
        print(sanitized_df.columns)

    return sanitized_df.columns


class Test2020Ticket(unittest.TestCase):
    """python -m unittest upload-kktix-ticket-csv-to-bigquery.py"""

    CANONICAL_COLUMN_NAMES_2020 = [
        'ticket_type',
        'payment_status',
        'tags',
        'paid_date',
        'price',
        'invoice_policy',
        'invoiced_company_name_optional',
        'unified_business_no_optional',
        'dietary_habit',
        'years_of_using_python',
        'area_of_interest',
        'organization',
        'job_role',
        'country_or_region',
        'departure_from_region',
        'how_did_you_know_pycon_tw',
        'have_you_ever_attended_pycon_tw',
        'do_you_know_we_have_financial_aid_this_year',
        'gender',
        'pynight_attendee_numbers',
        'pynight_attending_or_not',
        'email_from_sponsor',
        'email_to_sponsor',
        'privacy_policy_of_pycon_tw',
        'ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw'
    ]

    @classmethod
    def setUpClass(cls):
        cls.df = pd.read_csv("/home/tai271828/work-my-projects/pycontw-projects/PyCon-ETL-working/"
                             "corporate-attendees.csv")
        cls.sanitized_df = sanitize_column_names(cls.df)

    def test_column_number(self):
        assert len(self.sanitized_df.columns) == 25


if __name__ == "__main__":
    main()
