from google.cloud import bigquery
import os

def main():
    import_to_bq()

def import_to_bq():
    client = bigquery.Client.from_service_account_json(
        "/Users/limtzekang/Documents/Motorist/UA Data/motorist-4c0e7-4992af99b03a.json"
    )

    files_and_tables = [
        {
            "file_path": "/Users/limtzekang/Documents/Motorist/UA Data/motorist_sg_ga_data.csv",
            "table_id": "motorist-4c0e7.universal_analytics.Singapore_UA"
        },
        {
            "file_path": "/Users/limtzekang/Documents/Motorist/UA Data/motorist_my_ga_data.csv",
            "table_id": "motorist-4c0e7.universal_analytics.Malaysia_UA"
        },
        {
            "file_path": "/Users/limtzekang/Documents/Motorist/UA Data/motorist_th_ga_data.csv",
            "table_id": "motorist-4c0e7.universal_analytics.Thailand_UA"
        }
    ]

    for item in files_and_tables:
        file_path = item["file_path"]
        table_id = item["table_id"]

        print(f"Starting to upload file {os.path.basename(file_path)} to table {table_id}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )

        try:
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                while not job.done():
                    print(f"Uploading {os.path.basename(file_path)}...")
                    job.reload()  # Refreshes the state via a GET request.
            job.result()  # Waits for the job to complete.

            table = client.get_table(table_id)
            print(
                f"Successfully loaded to BigQuery with {table.num_rows} rows in table {table_id}"
            )
        except Exception as e:
            print(f"Error loading data into BigQuery for file {file_path} to table {table_id}: {str(e)}")

if __name__ == "__main__":
    main()