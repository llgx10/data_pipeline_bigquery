# import build in modules

import json
import pandas as pd
from pandas import json_normalize
import os
import sys
import numpy as np
import hashlib
sys.path.append("./")
# import custom module
from node.hubspot.hubspotoop import HubspotAPI
from node.bigquery.bigquery import BigQuery
from datetime import datetime, timezone
import workflows.hubspot_2_bigquery_migration.properties as prop
current_utc_date = datetime.now(timezone.utc)
emitted_at = current_utc_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

emitted_id = hashlib.md5(emitted_at.encode('utf-8')).hexdigest()

with open('gitlab_ci\hubspot\hubspotcrentials_ci.json', 'r') as file:
    hubspot_key = json.load(file)
bizzi_main_token = hubspot_key.get("app_token2")
bizzi_partner_token = hubspot_key.get("app_token3")


hb_main=HubspotAPI(bizzi_main_token)
hb_sub=HubspotAPI(bizzi_partner_token)
def get_all_association_of_contacts(contactId):
    
    payload = {
        "limit":100,
        "associations":"deals,call,note,emails,calls,meetings,tasks",
        "properties": [prop.extract_properties(bizzi_main_token,objects="contacts")

        ]
    }
    contact=hb_main._get_single_records("contacts",contactId,payload)
    return contact.get('associations')

data=get_all_association_of_contacts(1039751)


contact_to_email_ids = [result['id'] for result in data['emails']['results']]
contact_to_meeting_event_ids = [result['id'] for result in data['meetings']['results']]

def get_email_info(contact_to_email_ids,new_id):
    payload_get = {"properties": hb_main.object_properties_v2("emails"), "associations": "companies,contacts,deals","limit": 100}
    
    emails=[]
    for email_id in contact_to_email_ids:
        email=hb_main._get_single_records("emails",email_id,payload_get)
        emails.append(email)
    emails_df= pd.DataFrame(emails)
    emails_df['timestamp'] = emails_df['properties'].apply(lambda x: x.get('hs_timestamp'))
    emails_df['hs_email_html'] = emails_df['properties'].apply(lambda x: x.get('hs_email_html'))
    emails_df['hs_email_subject'] = emails_df['properties'].apply(lambda x: x.get('hs_email_subject'))
    emails_df['hs_email_from_email'] = emails_df['properties'].apply(lambda x: x.get('hs_email_from_email'))
    emails_df['hs_email_from_firstname'] = emails_df['properties'].apply(lambda x: x.get('hs_email_from_firstname'))
    emails_df['hs_email_from_lastname'] = emails_df['properties'].apply(lambda x: x.get('hs_email_from_lastname'))
    emails_df['hs_email_to_email'] = emails_df['properties'].apply(lambda x: x.get('hs_email_to_email'))
    emails_df['hs_email_to_firstname'] = emails_df['properties'].apply(lambda x: x.get('hs_email_to_firstname'))
    emails_df['hs_email_to_lastname'] = emails_df['properties'].apply(lambda x: x.get('hs_email_to_lastname'))
    
    for index, row in emails.iterrows():
        timestamp=row['timestamp'] 
        email_html=row['hs_email_html'] 
        hs_email_subject=row['hs_email_subject'] 
        hs_email_from_email=row['hs_email_from_email'] 
        hs_email_from_firstname=row['hs_email_from_firstname'] 
        hs_email_from_lastname=row['hs_email_from_lastname'] 
        hs_email_to_email=row['hs_email_to_email'] 
        hs_email_to_firstname=row['hs_email_to_firstname']
        hs_email_to_lastname=row['hs_email_to_lastname'] 
        
        payload={
            "properties": {
                "timestamp": f"{timestamp}",
                "hs_email_html": f"{email_html}",
                "hs_email_subject": f"{hs_email_subject}",
                "hs_email_from_email": f"{hs_email_from_email}",
                "hs_email_from_firstname": f"{hs_email_from_firstname}",
                "hs_email_from_lastname": f"{hs_email_from_lastname}",
                "hs_email_to_email": f"{hs_email_to_email}",
                "hs_email_to_firstname": f"{hs_email_to_firstname}",
                "hs_email_to_lastname": f"{hs_email_to_lastname}"
            },
            "associations": [
            {
            "to": {
                "id": 601
            },
            "types": [
                {
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": 210
                }
            ]
            }
        ]
        }
    
emails=get_email_info(contact_to_email_ids)
print(emails)

