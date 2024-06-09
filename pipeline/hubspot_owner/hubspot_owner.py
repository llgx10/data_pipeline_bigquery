import pandas as pd
import sys
import json
sys.path.append("./")
from node.hubspot.hubspotoop import HubspotAPI
from node.bigquery.bigquery import BigQuery
from datetime import datetime


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

def workflow(app_token):
    hb=HubspotAPI(app_token)
    data=hb.get_owner_list()
    data["createdAt"]=data["createdAt"].apply(lambda x:convert_datetime(x))
    data["updatedAt"]=data["updatedAt"].apply(lambda x:convert_datetime(x))
    print(data)
    return (data)




with open ("gitlab_ci\hubspot\hubspotcrentials_ci.json", "r") as file:
    hubspot_key = json.load(file)
app_token = hubspot_key.get("app_token")
data=workflow(app_token)
data["teams"]=data["teams"].apply(lambda x:json.dumps(x))
data_dict=data.to_dict(orient="records")
print(data_dict)
bigquery_config={
    "project_id":"bizzi-home",
    "dataset_id":"hubspot",
    "table_id":"owners"
}

schema = {
    'id': 'STRING',
    'email': 'STRING',
    'firstName': 'STRING',
    'lastName': 'STRING',
    'userId': 'STRING',
    'userIdIncludingInactive': 'STRING',
    'createdAt': 'TIMESTAMP',
    'updatedAt': 'TIMESTAMP',
    'archived': 'BOOL',
    'teams': 'STRING'
}
google_credentials_path="gitlab_ci\\google\\bizzi-home-creds.json"
bq=BigQuery(google_credentials_path)
pk="id"
cursor="updatedAt"
bq.json_upsert_to_bigquery(data_dict,bigquery_config,pk,cursor,schema)