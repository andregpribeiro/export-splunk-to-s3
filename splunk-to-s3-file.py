import sys
import datetime
import json
import boto3
import yaml
from splunklib.client import connect
from splunklib.results import ResultsReader
import logging

def setup_logging(log_file):
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def export_data_from_splunk_to_s3(config):
    service = connect(username=config['splunk']['username'], password=config['splunk']['password'], host=config['splunk']['host'], port=config['splunk']['port'], scheme="https")

    # Configure the S3 client
    s3_client = boto3.client("s3", aws_access_key_id=config['aws']['access_key_id'], aws_secret_access_key=config['aws']['secret_access_key'])

    s3_bucket_name = config['aws']['s3_bucket_name']
    s3_key_prefix = config['aws']['s3_key_prefix']

    for entry in config['queries']:
        query = entry['query']
        start_day = entry['start_date']
        end_day = entry['end_date']

        # Convert start_day and end_day to datetime objects
        start_day = datetime.datetime.strptime(start_day, "%Y-%m-%d")
        end_day = datetime.datetime.strptime(end_day, "%Y-%m-%d")

        # Iterate through the time range, incrementing by one day
        current_day = start_day
        while current_day <= end_day:
            start_time = current_day.strftime("%Y-%m-%dT%H:%M:%S")
            next_day = current_day + datetime.timedelta(days=1)
            end_time = next_day.strftime("%Y-%m-%dT%H:%M:%S")

            # Add the time range to the query
            final_query = f'search earliest="{start_time}" latest="{end_time}" {query}'
            
            # Run the query
            logging.info(f"Executing search: {final_query}")
            job = service.jobs.create(final_query)

            # Wait for the job to finish
            while not job.is_done():
                job.refresh()

            # Export the data
            results = job.results()
            reader = ResultsReader(results)

            # Process the results and store in a list
            data = []
            for result in reader:
                data.append(result)

            # Upload the data to S3
            s3_key = f"{s3_key_prefix}/{current_day.strftime('%Y-%m-%d_%H-%M-%S')}.json"
            logging.info(f"Uploading data to S3: {s3_bucket_name}/{s3_key}")
            s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=json.dumps(data))

            # Increment the current day
            current_day = next_day

if __name__ == "__main__":
    log_file = "export_splunk_to_s3.log"
    setup_logging(log_file)

    config_file = "config.yaml"
    config = load_config(config_file)
    export_data_from_splunk_to_s3(config)