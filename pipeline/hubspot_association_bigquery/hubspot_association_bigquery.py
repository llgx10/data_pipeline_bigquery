import json
import pandas as pd
import sys
import hashlib
from datetime import datetime, timezone
sys.path.append("./")
from config.hubspot.hubspotoop import HubspotAPI
from config.bigquery.bigquery import BigQuery
from config.redash.Redash import Redash
from pipeline.functions.functions import genegrate_emitted_info,concat_df_column_into_string,workflow_monitoring



def extract_id_and_type(data):
    return pd.Series([data['id'], data['type']]) if isinstance(data, dict) else pd.Series([None, None])


def get_recently_updated_companies():
    rd=Redash()
    query_id=rd.get_query_id('recently_updated_companies')
    id_list= rd.get_fresh_query_result(query_id=query_id,params={"max_age":0})
    return id_list

def get_recently_updated_deals():
    rd=Redash()
    query_id=rd.get_query_id('recently_updated_deals')
    id_list= rd.get_fresh_query_result(query_id=query_id,params={"max_age":0})
    return id_list


def get_recently_updated_contacts():
    rd=Redash()
    query_id=rd.get_query_id('recently_updated_contacts')
    id_list= rd.get_fresh_query_result(query_id=query_id,params={"max_age":0})
    return id_list

def get_asociated_objects_to_contacts(id_list,object_type):
    hb=HubspotAPI()
    association_data=hb.get_association_in_batch("contacts",object_type,id_list)
    return association_data

def get_associated_objects_to_companies(id_list,object_type):
    hb=HubspotAPI()
    association_data=hb.get_association_in_batch("companies",object_type,id_list)
    return association_data

def get_associated_objects_to_deals(id_list,object_type):
    hb=HubspotAPI()
    association_data=hb.get_association_in_batch("deals",object_type,id_list)
    return association_data


def check_if_association_created(association_df):
    association_id_list=concat_df_column_into_string(association_df,'association_id')
    rd=Redash()
    result=rd.get_fresh_query_result(query_id="1637",params={"max_age":0,"parameters":{"id_array":association_id_list}})
    association_not_exist=pd.DataFrame(result)
    return association_not_exist

def transform_association_data(association_data):
    if len(association_data)==0:
        return None
    try:
        
        emitted_at,emitted_id=genegrate_emitted_info()
        association_df=pd.DataFrame(association_data)        
        association_df = association_df.explode('to')
        association_df['from']=association_df["from"].apply(lambda x: x["id"] if isinstance(x, dict) else None)
        association_df[['to', 'type']] = association_df['to'].apply(extract_id_and_type)
        print(f'Transforming association data: {association_df["type"].iloc[0]}')
        
        association_df['association_id'] = association_df['from'] + association_df['type'] + association_df['to']
        association_df['association_id'] = association_df['association_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        
        association_df=association_df[['association_id','from','to','type']]
        association_df.reset_index(drop=True,inplace=True)
        
        association_df['emitted_at']=emitted_at
        association_df['emitted_id']=emitted_id
        
        association_not_exist_ids=check_if_association_created(association_df)
        if len(association_not_exist_ids)==0:
            return None
        
        association_not_exist_df = association_df[association_df['association_id'].isin(association_not_exist_ids['association_id'])]
        association_not_exist_dict=association_not_exist_df.to_dict(orient="records")
        print(association_not_exist_df)
        
        return association_not_exist_dict
    except Exception as e:
        print(f"Error transforming association data: {e}")
        return []


# @workflow_monitoring
def Hubspot_Association_to_BigQuery():
    #init BQ
    bq=BigQuery()
    association_table_confg=bq.get_table_config('hubspot','associations')
    association_table_schema=bq.get_table_schema('hubspot','associations')
    deal_id_list=get_recently_updated_deals()
    company_id_list=get_recently_updated_companies()
    contact_id_list=get_recently_updated_contacts()
    
    #company to contact 
    companies_to_contacts=get_associated_objects_to_companies(company_id_list,"contacts")
    companies_to_contacts_dict=transform_association_data(companies_to_contacts)
    bq.json_append_to_bigquery(companies_to_contacts_dict,association_table_confg,association_table_schema)
    
    
    #company to deal
    companies_to_deals=get_associated_objects_to_companies(company_id_list,"deals")
    companies_to_deals_dict=transform_association_data(companies_to_deals)
    bq.json_append_to_bigquery(companies_to_deals_dict,association_table_confg,association_table_schema)
    
    
    #deal to contact 
    deals_to_contacts=get_associated_objects_to_deals(deal_id_list,"contacts")
    deals_to_contacts_dict=transform_association_data(deals_to_contacts)
    bq.json_append_to_bigquery(deals_to_contacts_dict,association_table_confg,association_table_schema)
    
    #contact to emails

    contacts_to_emails=get_asociated_objects_to_contacts(contact_id_list,"emails")
    contacts_to_emails_dict=transform_association_data(contacts_to_emails)
    bq.json_append_to_bigquery(contacts_to_emails_dict,association_table_confg,association_table_schema)
    
    
    #contacts to calls
    contacts_to_calls=get_asociated_objects_to_contacts(contact_id_list,"calls")
    contacts_to_calls_dict=transform_association_data(contacts_to_calls)
    bq.json_append_to_bigquery(contacts_to_calls_dict,association_table_confg,association_table_schema)
    
    #contacts to notes
    contacts_to_notes=get_asociated_objects_to_contacts(contact_id_list,"notes")
    contacts_to_notes_dict=transform_association_data(contacts_to_notes)
    bq.json_append_to_bigquery(contacts_to_notes_dict,association_table_confg,association_table_schema)
    
    #contacts to meetings
    contacts_to_meetings=get_asociated_objects_to_contacts(contact_id_list,"meetings")
    contacts_to_meetings_dict=transform_association_data(contacts_to_meetings)
    bq.json_append_to_bigquery(contacts_to_meetings_dict,association_table_confg,association_table_schema)
    
    #contacts to tasks
    contacts_to_tasks=get_asociated_objects_to_contacts(contact_id_list,"tasks")
    contacts_to_tasks_dict=transform_association_data(contacts_to_tasks)
    bq.json_append_to_bigquery(contacts_to_tasks_dict,association_table_confg,association_table_schema)
    
    #contacts to communications
    contacts_to_communications=get_asociated_objects_to_contacts(contact_id_list,"communications")
    contacts_to_communications_dict=transform_association_data(contacts_to_communications)
    bq.json_append_to_bigquery(contacts_to_communications_dict,association_table_confg,association_table_schema)
    
    
    #tasks to deals
    deals_to_tasks=get_associated_objects_to_deals(deal_id_list,"tasks")
    deals_to_tasks_dict=transform_association_data(deals_to_tasks)
    bq.json_append_to_bigquery(deals_to_tasks_dict,association_table_confg,association_table_schema)
    
    
    total_row_processed = (
    len(companies_to_contacts_dict or {}) +
    len(companies_to_deals_dict or {}) +
    len(deals_to_contacts_dict or {}) +
    len(contacts_to_emails_dict or {}) +
    len(contacts_to_calls_dict or {}) +
    len(contacts_to_notes_dict or {}) +
    len(contacts_to_meetings_dict or {}) +
    len(contacts_to_tasks_dict or {}) +
    len(contacts_to_communications_dict or {})+
    len(deals_to_tasks_dict or {})
    )
    print(f'total row processed: {total_row_processed}')
    return total_row_processed

if __name__ == "__main__":
    Hubspot_Association_to_BigQuery()