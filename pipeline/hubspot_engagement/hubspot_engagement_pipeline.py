import pandas as pd
import json
import sys
from functools import wraps
from tqdm import tqdm
sys.path.append("./")
from config.redash.Redash import Redash
from datetime import datetime, timedelta
import traceback
from config.hubspot.hubspotoop import HubspotAPI
from config.bigquery.bigquery import BigQuery
from pipeline.functions.functions import genegrate_emitted_info,workflow_monitoring



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
@workflow_monitoring
def Hubspot_engagement_to_BigQuery(engagement_type):
    emitted_at,emitted_id=genegrate_emitted_info()
    try:
        rd=Redash()
        query_id=rd.get_query_id('hubspot_engagement_cursor')
        params={"max_age":0,"parameters":{"engagement_type":f"{engagement_type}"}}
        cursor_data=rd.get_fresh_query_result(query_id,params)
        cursor_date=cursor_data[0].get("DATE")
        print(cursor_date)

        hubspot = HubspotAPI()
        payload_search = json.dumps({"filters": [{"value": cursor_date, "propertyName": "hs_lastmodifieddate", "operator": "GT"}], "limit": 100})
        recently_modified_engagement = hubspot._search_records(f"{engagement_type}", payload_search)
        id_list= [{"id": item["id"]} for item in recently_modified_engagement]
        recently_modified_engagement=hubspot.get_object_in_batch(engagement_type,id_list)

        
        for item in recently_modified_engagement:
            item["createdAt"] = convert_datetime(item["createdAt"])
            item["updatedAt"] = convert_datetime(item["updatedAt"])

        engagement_df = pd.DataFrame(recently_modified_engagement)
        
        engagement_df['owner_id'] = engagement_df['properties'].apply(lambda x: x.get('hubspot_owner_id') if isinstance(x, dict) else None)
        engagement_df['activityDate'] = engagement_df['properties'].apply(lambda x: x.get('hs_timestamp') if isinstance(x, dict) else None)
        
        engagement_df['activityDate'] = engagement_df['activityDate'].apply(convert_datetime)
        engagement_df['properties'] = engagement_df['properties'].apply(lambda x: json.dumps(x))

        
        engagement_df['emitted_id'] = [emitted_id] * len(engagement_df)
        engagement_df['emitted_at'] = [emitted_at] * len(engagement_df)
        
    
        engagement_data=engagement_df.to_dict(orient='records')        
        bq=BigQuery()
        bigquery_config=bq.get_table_config('hubspot',f'{engagement_type}_engagement')
        schema=bq.get_table_schema('hubspot',f'{engagement_type}_engagement')
        pk="id"
        cursor='updatedAt'
        bq.json_upsert_to_bigquery(engagement_data,bigquery_config,pk,cursor,schema)
        
        return len(engagement_data)
    except Exception as e:
        traceback_message = traceback.format_exc()
        print(f"Traceback (most recent call last): {e}")
        print(traceback_message)

if __name__ == "__main__":
    Hubspot_engagement_to_BigQuery(engagement_type="emails")








    
