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
from config.bigquery.bigquery import BigQuery
from config.redash.Redash import Redash
from config.mautic.mautic import Mautic
from pipeline.functions.functions import genegrate_emitted_info,zip_emitted_info_mautic



def genegrate_emitted_info():
    try:
        current_utc_date = datetime.now(timezone.utc)
        emitted_at = current_utc_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]
        emitted_id = hashlib.md5(emitted_at.encode('utf-8')).hexdigest()
        return emitted_at,emitted_id
    except Exception as e:
        print("Error in genegrate_emitted_info: ", str(e))
        return None
    
def transform_mail_log(mail_log_df):
    print(mail_log_df.columns)
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


def get_mautic_read_mail_log_cursor():
    try:
        rd = Redash()
        print(rd.get_query_id('mautic_read_mail_log_cursor'))
        data = rd.get_fresh_query_result(query_id=rd.get_query_id('mautic_read_mail_log_cursor'), params=None)
        cursor_date = data[0].get("max_date")
        return cursor_date
    except Exception as e:
        print("Error in get_cursor: ", str(e))
        return None
    


def get_mautic_read_mail_log(cursor_date):
    mautic = Mautic()
    report_data = mautic.get_report(report_id=19,date_from='2024-01-01',date_to=cursor_date)
    # user_list_data = Mautic.fetch_user_list(limit)
    return report_data

def mautic_email_log_to_bigquery():
    emitted_at,emitted_id=genegrate_emitted_info()
    cursor_date=get_mautic_read_mail_log_cursor()
    
    email_read_log=get_mautic_read_mail_log(cursor_date)
    email_read_log_df=pd.DataFrame(email_read_log)
    email_read_log_df=transform_mail_log(email_read_log_df)
    email_read_log_df=create_event_id(email_read_log_df)
    email_read_log_df.sort_values(by="email_id",ascending=False,inplace=True)
    print (email_read_log_df)
    # return True
    
    email_read_log_dict=email_read_log_df.to_dict(orient="records")
    email_read_log_dict=zip_emitted_info_mautic(emitted_at,emitted_id,email_read_log_dict)
    
    bq=BigQuery()
    email_log_bq_config=bq.get_table_config('hubspot', 'email_read_log')
    email_log_schema=bq.get_table_schema('hubspot', 'email_read_log')
    pk='event_id'
    cursor='date_read'
   
    bq.json_upsert_to_bigquery(email_read_log_dict,email_log_bq_config,pk,cursor,email_log_schema)
    
if __name__ == "__main__":
    mautic_email_log_to_bigquery()
