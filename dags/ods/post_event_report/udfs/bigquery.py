from google.cloud import bigquery


def fetch():
    client = bigquery.Client(project='pycontw-225217')

    # Perform a query.
    QUERY = '''
        SELECT * FROM `pycontw-225217.ods.ods_ticket_corporate_attendees`
    '''
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        print(row.Price)
