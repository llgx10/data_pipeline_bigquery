from datetime import datetime, timezone
import pytz
import json
import time
import pandas as pd
from pandas import json_normalize
import os
import sys
import numpy as np
import hashlib

sys.path.append("./")
from node.bigquery.bigquery import BigQuery
from node.redash.Redash import RedashClient
from node.mautic.mautic import Mautic
import constants
from node.chats.googlechat import chats


def genegrate_emitted_info():
    try:
        gmt7 = pytz.timezone('Asia/Bangkok') 
        current_utc_date = datetime.now(timezone.utc)
        current_gmt7_date = current_utc_date.astimezone(gmt7)
        emitted_at = current_gmt7_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]
        emitted_id = hashlib.md5(emitted_at.encode('utf-8')).hexdigest()
        return emitted_at,emitted_id
    except Exception as e:
        print("Error in genegrate_emitted_info: ", str(e))
        return None
    
def transform_mail_log(mail_log_df):
    mail_log_df['hits'] = mail_log_df['hits'].astype(int)
    mail_log_df = mail_log_df.drop(columns=["name", "e_description", "source", "source_id", "viewed_in_browser"])
    bool_cols = ["is_read", "unsubscribed"]
    mail_log_df[bool_cols] = mail_log_df[bool_cols].replace({"0": False, "1": True})
    prefix_cols = ["firstname", "lastname", "email", "phone", "mobile"]
    mail_log_df = mail_log_df.rename(columns={col: "contact_" + col for col in prefix_cols})
    mail_log_df = mail_log_df.rename(columns={"e_id": "email_id","subject1":"subject"})

    return mail_log_df
    
def create_event_id(df):
    df['event_id'] = df['contactId'].astype(str) + '_' + df['email_id'].astype(str)
    return df


def get_mautic_read_mail_log_cursors(redash_credentials_path):
    try:
        #get cursor date
        redash_url = constants.redash_url
        rd = RedashClient(redash_url, redash_credentials_path)
        email_log_cursor_query_id="1472"
        data = rd.get_fresh_query_result(query_id=email_log_cursor_query_id, params=None)
        cursor_date = data[0].get("max_date")
        return cursor_date
    except Exception as e:
        print("Error in get_cursor: ", str(e))
        return None
    


def get_mautic_read_mail_log(mautic_credentials_path,redash_credentials_path):
    try:
        cursor_date = get_mautic_read_mail_log_cursors(redash_credentials_path)
        with open (mautic_credentials_path, 'r', encoding='utf-8') as file:
            mautic_creds = json.load(file)
        username = mautic_creds.get("user_name")
        password = mautic_creds.get("password")
        mautic = Mautic(username, password)
        report_data = mautic.get_report(report_id=19,date_from=None,date_to=None)
        report_data=pd.DataFrame(report_data)
        report_data['date_read'] = pd.to_datetime(report_data['date_read'])
        filtered_df = report_data[report_data['date_read'] > cursor_date]
        filtered_df=filtered_df.astype(str)
        return filtered_df
    except Exception as e:
        print("Error in get_cursor: ", str(e))
        return None
    


def zip_emitted_info(emitted_at,emitted_id,recently_read_mail_log):
    try:
        for d in recently_read_mail_log:
                d['emitted_at'] = emitted_at
                d['emitted_id'] = emitted_id
        return recently_read_mail_log
    except Exception as e:
        print("Error in zip_emitted_info: ", str(e))
        return None
    
    
def workflow(google_credentials_path,mautic_credentials_path,redash_credentials_path):
    emitted_at,emitted_id=genegrate_emitted_info()
    
    
    email_read_log_df=get_mautic_read_mail_log(mautic_credentials_path,redash_credentials_path)
    email_read_log_df=transform_mail_log(email_read_log_df)
    email_read_log_df=create_event_id(email_read_log_df)
    
    email_read_log_dict=email_read_log_df.to_dict(orient="records")
    email_read_log_dict=zip_emitted_info(emitted_at,emitted_id,email_read_log_dict)
   
    
    #upsert to bigquery 
    email_log_bq_config=constants.hubspot_email_read_log_bigquery_config
    email_log_schema=constants.hubspot_email_read_log_schema
    pk=constants.hubspot_email_read_log_upsert_key.get("pk")
    cursor=constants.hubspot_email_read_log_upsert_key.get("cursor")
    bq=BigQuery(google_credentials_path)
    bq.json_upsert_to_bigquery(email_read_log_dict,email_log_bq_config,pk,cursor,email_log_schema)
    
    
    #send message to google chat
    chat=chats(constants.workflow_monitoring_chat_space)
    workflow_name="Mautic email reading log"
    started_at=datetime.strptime(emitted_at, '%Y-%m-%dT%H:%M:%S.%f')
    started_at_short=started_at.strftime('%Y-%m-%d %H:%M:%S')
    ended_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    duration=round(((datetime.now()-started_at).total_seconds()),2)
    row_processed=email_read_log_df.shape[0]
    print(started_at_short,ended_at,duration)
    chat.workflow_end_message(workflow_name,started_at_short,ended_at,duration,row_processed)
    
    
    
# google_credentials_path = 'gitlab_ci/google/bizzi-home-creds.json'
# redash_credentials_path = 'gitlab_ci/redash/redash_credential_ci.json'
# mautic_credential_path='gitlab_ci\mautic\mautic_credntial_ci.json'
# workflow(google_credentials_path,mautic_credential_path,redash_credentials_path)