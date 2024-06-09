import pandas as pd
import os
import json
import sys
import uuid
import time
import random
from functools import wraps
from tqdm import tqdm
sys.path.append("./")
from config.redash.Redash import Redash
from datetime import datetime, timedelta
from dateutil import parser
import ast
from config.hubspot.hubspotoop import HubspotAPI
import logging
from io import StringIO
from config.bigquery.bigquery import BigQuery
google_credentials_path = "gitlab_ci/google/bizzi-home-creds.json"
def convert_datetime(datetime_str):
    try:
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            datetime_obj = datetime.fromtimestamp(int(datetime_str) / 1000)
    
    formatted_datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime_str


def workflow(hubspot_crendtial_path, engagement_type):
    with open(hubspot_crendtial_path, "r") as file:
        hubspot_key = json.load(file)
    
    app_token = hubspot_key.get("app_token2")
    hubspot = HubspotAPI()
    
    # Fetch id of engagement
    url = f"https://api.hubapi.com/crm/v3/objects/{engagement_type}/search"
    payload_search = json.dumps({"filters": [{"value": 0, "propertyName": "hs_createdate", "operator": "GT"}], "limit": 100})
    results = hubspot._search_records(url, payload_search)
    
    engagement_list = []
    payload_get = {"properties": hubspot.object_properties_v2(engagement_type), "associations": "companies,contacts,deals"}
    
    with tqdm(total=len(results), desc="Processing") as pbar:
        for i, item in enumerate(results, start=1):
            pbar.update(1)  # Increment progress bar by 1
            id = item.get("id")
            url = f"https://api.hubapi.com/crm/v3/objects/{engagement_type}/{id}"
            engagement_data = hubspot._get_single_record(url, payload_get)
            engagement_list.append(engagement_data)
    
    with open(f'workflows/hubspot_engagement/temp_json/{engagement_type}.json', 'w', encoding='utf-8') as f:
        json.dump(engagement_list, f, ensure_ascii=False, indent=4)
    
    print(f"Successfully downloaded {engagement_type} records")


def transformation(engagement_list):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emitted_at = current_datetime
    emitted_id = str(uuid.uuid4())
 
    

    
    print(f"len: {len(engagement_list)}")
    
    for item in engagement_list:
        item["createdAt"] = convert_datetime(item["createdAt"])
        item["updatedAt"] = convert_datetime(item["updatedAt"])
    print("x")
    df = pd.DataFrame(engagement_list)
    
    print("xx")
    df['owner_id'] = df['properties'].apply(lambda x: x.get('hubspot_owner_id') if isinstance(x, dict) else None)
    df['activityDate'] = df['properties'].apply(lambda x: x.get('hs_timestamp') if isinstance(x, dict) else None)
    df['activityDate'] = df['activityDate'].apply(convert_datetime)
    df['properties'] = df['properties'].apply(lambda x: json.dumps(x, ensure_ascii=False))
    # df['associations'] = {}
    df['emitted_id'] = [emitted_id] * len(df)
    df['emitted_at'] = [emitted_at] * len(df)
    
    # return True
    df_dict=df.to_dict(orient='records')
    bq=BigQuery(google_credentials_path)
    bigquery_config={
        "project_id": "bizzi-home",
        "dataset_id": "hubspot",
        "table_id": f"{engagement_type}_engagement"
    }
    print("xxx")
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

    
hubspot_credtial_path = "gitlab_ci\hubspot\hubspotcrentials_ci.json"
engagement_type = "notes"
# workflow(hubspot_credtial_path,engagement_type)
# test_engagement_properties( hubspot_credtial_path,engagement_type)

def workflow2(hubspot_crendtial_path, engagement_type):
    with open(hubspot_crendtial_path, "r") as file:
        hubspot_key = json.load(file)
    
    app_token = hubspot_key.get("app_token2")
    hubspot = HubspotAPI()
    
    # Fetch id of engagement
    payload_get = {"properties": hubspot.object_properties_v2(engagement_type),"limit": 100}
    results = hubspot._get_records(engagement_type, payload_get)
    return results
        


def generate_15_day_range(start_year, start_month, start_day, end_year, end_month, end_day):
    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)
    
    while start_date <= end_date:
        first_day = start_date
        last_day = start_date + timedelta(days=10)
        if last_day > end_date:
            last_day = end_date
        
        first_day_ms = int(first_day.timestamp()) * 1000
        last_day_ms = int(last_day.timestamp()) * 1000
        
        yield (first_day_ms, last_day_ms)
        
        start_date += timedelta(days=10)

def workflow3(hubspot_crendtial_path,engagement_type):
    with open(hubspot_crendtial_path, "r") as file:
        hubspot_key = json.load(file)
    
    app_token = hubspot_key.get("app_token2")
    hubspot = HubspotAPI(app_token)
    payload = json.dumps(
        {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": 1711929600000
                        }
                    ]
                }
            ],
            "properties": hubspot.object_properties_v2(engagement_type),
            "associations": "companies,companies,deals,call,note",
            "limit": 100
        }
    )
    results = hubspot._search_records(engagement_type, payload)
    transformation(results)
        
# workflow3(hubspot_credtial_path,engagement_type)



notes=workflow2(hubspot_credtial_path,engagement_type)
with open(f'data/raw/{engagement_type}.json', 'w', encoding='utf-8') as f:
    json.dump(notes, f, ensure_ascii=False, indent=4)


#patch 1: less than 1/9/2023 1693526400000
#patch 2: from 1/9/2023 1693526400000 to 31/12/2023 1693526400000