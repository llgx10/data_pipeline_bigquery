import pandas as pd
import os
import json
import sys
import uuid
import time
import random
from functools import wraps
import traceback
sys.path.append("./")
from node.gcs.gcs import GCSHandler
from node.redash.Redash import RedashClient
from datetime import datetime, timedelta
from node.chats.googlechat import chats
from node.hubspot.hubspotoop import HubspotAPI
from node.bigquery.bigquery import BigQuery
import logging
from io import StringIO

# Define a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Define a StringIO object to capture log messages
log_stream = StringIO()

# Define a handler to write logs to the StringIO object
stream_handler = logging.StreamHandler(log_stream)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)

def send_workflow_results_to_chat(webhooj_url, workflow_name,start,end,execution_time,row_proccessed):
    chat=chats(webhooj_url)
    payload= json.dumps(
            {
                "cardsV2": [
                    {
                        "cardId": "unique-card-id",
                        "card": {
                            "header": {
                                "title": f"{workflow_name}",
                                "subtitle": "Workflow Finished Running",
                                "imageUrl": "https://static-00.iconduck.com/assets.00/gitlab-icon-512x471-wfbmkpzi.png",
                                "imageType": "SQUARE",
                            },
                            "sections": [
                                {
                                    "header": "Informations: ",
                                    "collapsible": False,
                                    "widgets": [
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Started At: </b>{start} "
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Ended At: </b> {end}"
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Time run:</b> {execution_time} seconds"
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Row processed: </b>{row_proccessed}"
                                            }
                                        },
                                        {"divider": {}},
                                        {
                                            "buttonList": {
                                                "buttons": [
                                                    {
                                                        "text": "Source check",
                                                        "onClick": {
                                                            "openLink": {
                                                                "url": "https://app.hubspot.com/deals/6319203/objects/0-1/views/18689731/list"
                                                            }
                                                        },
                                                    },
                                                    {
                                                        "text": "Destination Check",
                                                        "color": {
                                                            "red": 0,
                                                            "green": 0.5,
                                                            "blue": 1,
                                                            "alpha": 1,
                                                        },
                                                        "onClick": {
                                                            "openLink": {
                                                                "url": "https://stats.bizzi.vn/queries/1405/source"
                                                            }
                                                        },
                                                    },
                                                ]
                                            }
                                        },
                                    ],
                                }
                            ],
                        },
                    }
                ]
            }
        )
    chat.post_message(payload)


def handle_errors(max_retries=3, base_delay=1):
    def decorator(func):
        global length
        global start_time, end_time
        execution_time=end_time-start_time
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    workflow_name="Hubspot email engagement to BigQuery"
                    webhook_url='https://chat.googleapis.com/v1/spaces/AAAA_CD3XNI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=ZfKHkcs8Zp55-XWda-wu9bMiugcJMbuVKrvVqdwoG7Q'
                    send_workflow_results_to_chat(webhook_url, workflow_name,start_time,end_time,execution_time,length)
                    return result  # Return the result if no exception occurred
                except FileNotFoundError:
                    print("HubSpot credentials file not found.")
                except json.JSONDecodeError:
                    print("Error decoding JSON in the credentials file.")
                except KeyError as keyerror:
                    print(f"Error with key: {keyerror}")
                except Exception as e:
                    traceback.print_exc()
                    print("An unexpected error occurred:", str(e))
                
                # Exponential backoff retry logic
                delay = (2 ** retries) * base_delay
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay + random.uniform(0, 1))
                retries += 1
            
            print("Max retries exceeded. Exiting.")
            return None  # Return None if max retries exceeded
        return wrapper
    
    # Check if the decorator was called with arguments or not
    if callable(max_retries):
        func = max_retries
        max_retries = 3
        base_delay = 1
        return decorator(func)
    else:
        return decorator

start_time = 0
end_time = 0
length=0
def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        global start_time, end_time  # Declare the variables as global
        start_time = time.time()  # Update start time
        result = func(*args, **kwargs)
        end_time = time.time()  # Update end time
        execution_time = end_time - start_time  # Calculate execution time
        print(f"Function '{func.__name__}' executed in {execution_time:.2f} seconds")
        return result
    return wrapper


def convert_datetime(datetime_str):
    try:
        # Try parsing datetime string with microseconds
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            # If parsing fails, try parsing without microseconds
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            # If parsing fails again, handle the conversion based on BYTE_ARRAY
            datetime_obj = datetime.fromtimestamp(int(datetime_str) / 1000)
    
    # Format datetime object as desired string format
    formatted_datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime_str



google_credentials_path = "gitlab_ci/google/bizzi-home-creds.json"
hubspot_creds_path = "gitlab_ci/hubspot/hubspotcrentials_ci.json"


@measure_execution_time
@handle_errors
def workflow(google_credentials_path,engagement_type):
    global length
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emitted_at = current_datetime
    emitted_id = str(uuid.uuid4())
    try:
        # Log the start of the workflow
        logger.info("Workflow started")
        
        with open(f"workflows\\hubspot_engagement\\temp_json\\{engagement_type}.json", encoding='utf-8') as json_file:
            data = json.load(json_file)
        data_size = sys.getsizeof(data)
        print(f"data size {data_size}")

        logger.info(f"Loaded JSON data from file, size: {data_size} KB")
        
        flat_list_of_dicts = [item for sublist in data for item in sublist]
        logger.info("Flattened list of dictionaries")
        
        print(f"len: {len(data)}")
        print(flat_list_of_dicts)
        
        for item in flat_list_of_dicts:
            item["createdAt"] = convert_datetime(item["createdAt"])
            item["updatedAt"] = convert_datetime(item["updatedAt"])



        
        
        logger.info("Converted datetime strings")
        
        df = pd.DataFrame(flat_list_of_dicts)
        
        data = []
        print(df)
        logger.info("Created DataFrame")
        df['owner_id'] = df['properties'].apply(lambda x: x.get('hubspot_owner_id') if isinstance(x, dict) else None)
        df['activityDate'] = df['properties'].apply(lambda x: x.get('hs_timestamp') if isinstance(x, dict) else None)
        df['activityDate'] = df['activityDate'].apply(convert_datetime)
        df['properties'] = df['properties'].apply(lambda x: json.dumps(x))
        df['associations'] = df['associations'].apply(lambda x: json.dumps(x))
        df['emitted_id'] = [emitted_id] * len(df)
        df['emitted_at'] = [emitted_at] * len(df)
        
        logger.info("Applied JSON dumps to properties and associations columns")
        # return True
        df_dict=df.to_dict(orient='records')
        bq=BigQuery(google_credentials_path)
        bigquery_config={
            "project_id": "bizzi-home",
            "dataset_id": "hubspot",
            "table_id": f"{engagement_type}_engagement"
        }
        schema={
            "id":"STRING",
            "properties":"STRING",
            "associations":"STRING",
            "createdAt":"TIMESTAMP",
            "updatedAt":"TIMESTAMP",
            "activityDate":"TIMESTAMP",
            "archived":"BOOL",
            "owner_id":"STRING",
            "emitted_id":"STRING",
            "emitted_at":"TIMESTAMP",
            
        }
        pk="id"
        cursor='updatedAt'
        bq.json_upsert_to_bigquery(df_dict,bigquery_config,pk,cursor,schema)
        return df.head(1)
    except Exception as e:
        logger.exception("An error occurred in the workflow: %s", str(e))
        raise  # Re-raise the exception after logging it




# result = workflow(google_credentials_path)
# print(result)


# def workflow2(google_credentials_path):
#     bucket_name='bizzi-bi-data-hubspot'
#     path='hubspot/engagements/emails/engagements-snappy2.parquet'
#     gcs=GCSHandler(google_credentials_path)
#     gcs.delete_files_in_path(bucket_name,path)
    

# engagement_type = "emails"
# workflow(google_credentials_path,engagement_type)

