# python3 -m pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Configuration
scopes = ['https://www.googleapis.com/auth/analytics.readonly']
credentials_path = '/Users/limtzekang/Documents/Motorist/UA Data/motorist-4c0e7-0ad1ab311965.json'  # Path to your service account key file

# Views configuration
VIEWS = {
    'sg': {
        'id': '104580148',
        'start_date': '2015-01-01',
        'end_date': '2023-06-30',
        'name': 'motorist_sg_ga_data.csv'
    },
    'my': {
        'id': '178363471',
        'start_date': '2015-01-01',
        'end_date': '2023-06-30',
        'name': 'motorist_my_ga_data.csv'
    }, 
    'th': {
      'id': '256609028',
      'start_date': '2015-01-01',
      'end_date': '2023-06-30',
      'name': 'motorist_th_ga_data.csv'
    }
}

# Provided metrics and dimensions
METRICS = [
  'ga:sessions', 'ga:sessionDuration', 'ga:avgSessionDuration', 'ga:users',
  'ga:newUsers', 'ga:bounceRate', 'ga:pageviews', 'ga:uniquePageviews', 'ga:bounces'
]
DIMENSIONS = [
  'ga:sessionCount', 'ga:pagePath', 'ga:deviceCategory', 'ga:pageTitle', 'ga:date', 'ga:medium', 'ga:source'
]

def get_report(analytics, view_id, start_date, end_date, metrics, dimensions):
    print(f"Fetching report for view ID: {view_id}")
    all_rows = []
    try:
        # Initialize variables for pagination
        page_token = None
        page_count = 0
        total_rows = 0

        # Loop until all pages have been retrieved
        while True:
            # Construct report request
            report_request = {
                'viewId': view_id,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': [{'expression': metric} for metric in metrics],
                'dimensions': [{'name': dimension} for dimension in dimensions],
                'pageSize': 100000,  # Max page size for initial request
                'pageToken': page_token  # Set page token for pagination
            }

            # Execute request
            response = analytics.reports().batchGet(body={'reportRequests': [report_request]}).execute()
            print(f"Received response for view ID: {view_id}, Page count: {page_count+1}")

            # Check for sampling information
            report = response.get('reports', [{}])[0]
            data = report.get('data', {})

            if 'samplesReadCounts' in data and 'samplingSpaceSizes' in data:
                print(f"Sampling applied: {data['samplesReadCounts']} samples read out of {data['samplingSpaceSizes']}")

            # Process response
            if 'reports' in response and response['reports']:
                report = response['reports'][0]
                data = report.get('data', {})

                # Append rows from current page
                rows = data.get('rows', [])
                all_rows.extend(rows)
                page_count += 1
                total_rows += len(rows)
                print(f"Retrieved {len(rows)} rows for view ID: {view_id}, Total rows: {total_rows}")

                # Check if more pages exist
                page_token = report.get('nextPageToken')

                if not page_token:
                    break  # No more pages to retrieve

            else:
                break  # No reports or empty response

    except Exception as e:
        print(f"Error fetching report: {e}")

    return all_rows

def response_to_dataframe(response, metrics, dimensions):
  list_rows = []
  for row in response:
    dimensions_data = row.get('dimensions', [])
    metrics_data = row.get('metrics', [])[0]['values']

    row_data = {}
    for dimension, value in zip(dimensions, dimensions_data):
      row_data[dimension] = value

    for metric, value in zip(metrics, metrics_data):
      row_data[metric] = value

    list_rows.append(row_data)

  return pd.DataFrame(list_rows)

def extract_data():
    try:
        print("Initializing Analytics Reporting API")
        credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        for view_key, view_info in VIEWS.items():
            view_id = view_info['id']
            start_date = view_info['start_date']
            end_date = view_info['end_date']
            output_filename = view_info['name']

            print(f"Processing view {view_key} (ID: {view_id})")

            rows = get_report(analytics, view_id, start_date, end_date, METRICS, DIMENSIONS)

            if rows:
                df = response_to_dataframe(rows, METRICS, DIMENSIONS)
                df.to_csv(output_filename, index=False)
                print(f'Data exported to {output_filename} for view {view_key}. Rows: {len(df)}')
            else:
                print(f"No data retrieved for view {view_key}")

    except Exception as e:
        print(f'Error occurred: {e}')

if __name__ == '__main__':
    extract_data()
