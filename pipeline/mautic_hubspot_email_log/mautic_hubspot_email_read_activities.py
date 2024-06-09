import time
import pandas as pd
import sys
import json
from datetime import datetime

import requests
sys.path.append("./")
from config.hubspot.hubspotoop import HubspotAPI
from config.redash.Redash import Redash
from config.mautic.mautic import Mautic

from pipeline.hubspot_2_bigquery_migration.contacts_pipeline import Hubspot_contacts_to_BigQuery as contacts_pipeline
from pipeline.functions.functions import workflow_monitoring



def get_utc_milliseconds():
    current_time_utc = datetime.utcnow()
    milliseconds = int(current_time_utc.timestamp() * 1000)
    return milliseconds


def create_hubspot_contact_dict(df):
    try:
        list_of_dicts = []
        for index, row in df.iterrows():
            entry = {
                "properties": {
                    "firstname": row['contact_name'],
                    "phone": row['contact_phone'],
                    "email": row['contact_email'],
                    "workflow": "[n8n Workflow] Sync contact từ mautic",
                    "hubspot_owner_id":"182140703",
                    "mautic_contact_id":row['contactId'],
                    "hs_lead_status":"OPEN_DEAL",
                    "lifecyclestage":"lead",
                    "source":"Marketing",
                    "hubspot_owner_id":"673455525"
                    
                }
            }
            list_of_dicts.append(entry)
        return list_of_dicts
    except Exception as e:
        print(f"Error in create_hubspot_contact_dict: {e}")


def get_matic_read_email_log():
    try:
        rd=Redash()
        data=rd.get_fresh_query_result(rd.get_query_id('matic_read_email_log'),params={"max_age":0})
        matic_read_email_log_df=pd.DataFrame(data)
        matic_read_email_log_df['contact_phone'] = matic_read_email_log_df['contact_phone'].apply(lambda x: None if x == 'null' else x)
        matic_read_email_log_df['email_id']=matic_read_email_log_df['email_id'].astype(str)
        return matic_read_email_log_df
    except Exception as e:
        print(f"Error in get_matic_read_email_log: {e}")



def create_contact_on_hubspot(read_mail_log):
    try:
        contact_not_created = read_mail_log[read_mail_log['hubspot_id'] == 'contact not created']
        if contact_not_created is None:
            print("All contacts were created on Hubspot")
            return None
        contact_not_created.drop_duplicates(subset=['contact_email'], keep='first', inplace=True)
        contact_creation_dict=create_hubspot_contact_dict(contact_not_created)
        for contact in contact_creation_dict:
            hubspot = HubspotAPI()
            hubspot.create_contact(contact)
    except Exception as e:
        print(f"Error in create_contact_on_hubspot: {e}")
    return None

def distinct_element(df, column_name):
    distinct_values = df[column_name].unique().tolist()
    return distinct_values


def get_email_html(email_ids):
    try:
        email_html_list = []
        for email_id in email_ids:
            print(f"Getting html for email ID: {email_id}, total list: {len(email_ids)}")
            email_html_response = requests.get(f"https://bi-n8n.bizzi.vn/webhook/view-email?id={email_id}")
            time.sleep(2)
            if email_html_response.status_code == 200:
                email_html = email_html_response.text
                email_html_list.append((email_id, email_html))
            else:
                print(f"Failed to fetch email HTML for ID: {email_id}")
        email_list_html=pd.DataFrame(email_html_list, columns=['email_id', 'email_html'])
        email_list_html['email_id']=email_list_html['email_id'].astype(str)
        return email_list_html
    except Exception as e:
        print(f"Error in get_email_html: {e}")
        
def check_if_email_logged(row):
    email_id = row['email_id']
    list_campaigns = row['list_campaigns']

    if list_campaigns is None:
        return False
    
    list_campaigns = list_campaigns.split(',')
    
    return email_id in list_campaigns      


def remove_duplicates(campaign_ids):
    id_list = campaign_ids.split(',')
    unique_ids = sorted(set(id_list), key=id_list.index)
    return ','.join(unique_ids)


def concatenate_campaigns(row):
    email_id = row['email_id']
    list_campaigns = row['list_campaigns']
    if list_campaigns is None:
        return email_id
    
    return email_id + ',' + list_campaigns

def log_email_to_hubspot(email_read_log_df):
    email_read_log_df.reset_index(inplace=True)
    for index,row in email_read_log_df.iterrows():
        print(f"{index} /{len(email_read_log_df)}")
        hubspot_contact_id=row['hubspot_id']
        contact_properties = json.dumps({
        "properties": {
            "mautic_contact_id": f"{row['contactId']}",
            "campaign_name": f"{row['subject']}",
            "mautic_last_updated_at": f"{row['date_read'][:10]}",
            "list_campaigns": f"{row['new_list_campaign']}"
        }
    })
        email_properties = json.dumps({
                                        "properties": {
                                            "hs_email_html": f"{row['email_html']}",
                                            "hs_email_subject": f"{row['subject']}",
                                            "hs_timestamp": f"{row['date_read_ms']}",
                                            "hs_email_direction": "EMAIL",
                                            "hs_email_status": "SENT",
                                            "hubspot_owner_id":"182140703"
                                        },
                                        "associations": [
                                            {
                                            "to": {
                                                "id": hubspot_contact_id
                                            },
                                            "types": [
                                                {
                                                "associationCategory": "HUBSPOT_DEFINED",
                                                "associationTypeId": 198
                                                }
                                            ]
                                            }
                                        ]
                                        })
        hb=HubspotAPI()
        hb.create_email_engagement_on_contacts(email_properties)
        hb.patch_update_object("contact",hubspot_contact_id,contact_properties)
    
    




@workflow_monitoring
def Log_Mautic_email_read_to_Hubspot():

    read_mail_log=get_matic_read_email_log()
    # create contact on hubspot
    create_contact_on_hubspot(read_mail_log)
    contacts_pipeline()
    read_mail_log=get_matic_read_email_log()
    unique_email_id_list=distinct_element(read_mail_log,'email_id')
    email_html_df=get_email_html(unique_email_id_list)
    print(email_html_df)
    
    read_mail_log=pd.merge(read_mail_log,email_html_df,on='email_id',how='left')
    
    read_mail_log['is_logged']=read_mail_log.apply(check_if_email_logged,axis=1)
    email_not_logged = read_mail_log[~read_mail_log['is_logged']]
    print(email_not_logged)

    email_not_logged['new_list_campaign'] = email_not_logged.apply(concatenate_campaigns, axis=1)
    
    
    email_not_logged['new_list_campaign'] = email_not_logged.groupby('contactId')['new_list_campaign'].transform(lambda x: ','.join(x))
    email_not_logged['new_list_campaign'] = email_not_logged['new_list_campaign'].apply(remove_duplicates)
    
    
    email_not_logged.sort_values(by='contactId',inplace=True)
    
    email_not_logged.head(5).to_csv('email_not_logged.csv',index=False)
    
    log_email_to_hubspot(email_not_logged)
    
    return (len(email_not_logged))
    

if __name__ == "__main__":
    Log_Mautic_email_read_to_Hubspot()

