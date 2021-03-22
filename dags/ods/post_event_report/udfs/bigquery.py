from google.cloud import bigquery


def fetch():
    client = bigquery.Client(project='pycontw-225217')

    # Perform a query.
    QUERY = '''
        SELECT scenario.day2checkin.attr.diet FROM `pycontw-225217.ods.ods_opass_attendee_timestamp`
    '''
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
            print(row.diet)