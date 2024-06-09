from datetime import datetime,timezone,timedelta
import sys
from functools import wraps
import pandas as pd
import platform
import uuid
import yaml
sys.path.append("./")
from config.google_chat.google_chat import Chats
from config.redash.Redash import Redash

os_name = platform.system()

def get_recheck_url(function_name):
    try:
            with open('config/google_chat/google_chat_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
            urls= data.get('result_url')
            for url in urls:
                if url['name'] == function_name:
                    return url['url']    
    except Exception as e:
            print(f"Error in load chat-config-yml file: {e}")
    
    
def workflow_monitoring(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        gmt7 = timezone(timedelta(hours=7))
        function_name = str(func.__name__).replace("_", " ")
        resuls_url= get_recheck_url(str(func.__name__))
        start_time = datetime.now(gmt7)
        row_processed = func(*args, **kwargs)
        end_time = datetime.now(gmt7)
        elapsed_time = end_time - start_time
        elapsed_time_seconds = round(elapsed_time.total_seconds(), 2)
        chat = Chats('workflow-monitoring')
        chat.workflow_end_message(function_name,os_name, start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"), elapsed_time_seconds, row_processed,resuls_url)
        return row_processed
    return wrapper

def test_workflow_monitoring(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        gmt7 = timezone(timedelta(hours=7))
        resuls_url= get_recheck_url(str(func.__name__))
        function_name = str(func.__name__).replace("_", " ")
        start_time = datetime.now(gmt7)
        result = func(*args, **kwargs)
        end_time = datetime.now(gmt7)
        elapsed_time = end_time - start_time
        elapsed_time_seconds = round(elapsed_time.total_seconds(), 2)
        chat = Chats('test-webhook')
        chat.workflow_end_message(function_name,os_name, start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"), elapsed_time_seconds, result,resuls_url)
        return result
    return wrapper

def get_object_cursor_date(object_type):
    try:
        rd=Redash()
        param={"parameters": {"objects": object_type}, "max_age": 0}
        cursor_date_results=rd.get_fresh_query_result(rd.get_query_id('hubspot-object-cursor'), params=param)
        cursor_date = cursor_date_results[0].get("DATE")    
        return cursor_date
    except Exception as e:
        print(f"Error in get_cursor_date: {e}")
        return None
    
def get_engagement_cursor_date(engagement_type):
    try:
        rd=Redash()
        cursor_date=rd.get_fresh_query_result(query_id="1424",params={"max_age":0,"parameters":{"engagement_type":f"{engagement_type}"}})
        cursor_date = cursor_date[0].get("DATE")    
        return cursor_date
    except Exception as e:
        print(f"Error in get_cursor_date: {e}")
        return None
    

def genegrate_emitted_info():
    try:
        current_utc_date = datetime.now(timezone.utc)
        emitted_at = current_utc_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]
        emitted_id = emitted_id = str(uuid.uuid4())
        return emitted_at,emitted_id
    except Exception as e:
        print("Error in genegrate_emitted_info: ", str(e))
        return None
    

def drop_duplicates_by_id(list_of_dicts):
    try:
        unique_ids = set()
        unique_list_of_dicts = []
        
        for item in list_of_dicts:
            id_value = item.get('id')
            
            if id_value not in unique_ids:
                unique_ids.add(id_value)
                unique_list_of_dicts.append(item)
        
        return unique_list_of_dicts
    except Exception as e:
        print("Error in drop_duplicates_by_id: ", str(e))
        return None

def check_null_in_source(source_data, primary_key):
    try:
        null_count = 0
        for row in source_data:
            if row.get(primary_key) is None:
                print("Found null value in primary key column in the source data.")
                null_count += 1
        if null_count == 0:
            print("No null values found in primary key column in the source data.")
        else:
            print(f"Total {null_count} null values found in primary key column in the source data.")
    except Exception as e:
        print("Error in check_null_in_source: ", str(e))
        return None
    
def zip_emitted_info(emitted_at,emitted_id,hubspot_object_list):
    try:
        for d in hubspot_object_list:
                d['emitted_at'] = emitted_at
                d['emitted_id'] = emitted_id
                d['archivedAt'] = None
                if "associations" not in d:
                    d["associations"] = {}
        return hubspot_object_list
    except Exception as e:
        print("Error in zip_emitted_info: ", str(e))
        return None
    
    
def zip_emitted_info_mautic(emitted_at,emitted_id,hubspot_object_list):
    try:
        for d in hubspot_object_list:
                d['emitted_at'] = emitted_at
                d['emitted_id'] = emitted_id
        return hubspot_object_list
    except Exception as e:
        print("Error in zip_emitted_info: ", str(e))
        return None
    
def concat_df_column_into_string(df, column_name):

    unique_values = df.drop_duplicates(subset=column_name, keep='first')[column_name]
    result = "', '".join(unique_values.astype(str))
    result = "'" + result + "'"
    
    return result
