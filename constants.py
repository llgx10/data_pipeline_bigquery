#url
redash_url='https://stats.bizzi.vn/'
hubspot_url='https://api.hubapi.com/'

#bigquery_config
cursor_bigquery_config = {'project_id': 'bizzi-home','dataset_id': 'hubspot','table_id': 'hubspot_object_cursor'}
companies_bigquery_config = {'project_id': 'bizzi-home','dataset_id': 'hubspot','table_id': 'hubspot_companies'}




#bigquery upsert method
hubspot_upsert_key={'pk':'id','cursor':'updatedAt'}
hubspot_email_read_log_upsert_key={'pk':'event_id','cursor':'date_read'}



#bigquery_schema
cursor_schema_dict={
    'emitted_id': 'STRING',
    'emitted_at': 'TIMESTAMP',
    'cursor_date': 'TIMESTAMP',
    'object': 'STRING'
    
} 

companies_schema_dict={
        'emitted_id': 'STRING',
        'emitted_at': 'TIMESTAMP',
        'createdAt': 'TIMESTAMP',
        'id': 'STRING',
        'updatedAt': 'TIMESTAMP',
        'associations': 'JSON',
        'properties': 'JSON', 
        'archived': 'BOOLEAN',
        'archivedAt': 'TIMESTAMP'
        
    }  

hubspot_email_read_log_schema={ 
    'emitted_id': 'STRING',
    'emitted_at': 'TIMESTAMP',
    'event_id': 'STRING',
    'contact_email': 'STRING',
    'contact_firstname': 'STRING',
    'contact_lastname': 'STRING',
    'contact_phone': 'STRING',
    'contact_mobile': 'STRING',
    'e_created_by_user': 'STRING',
    'is_read': 'BOOL',
    'read_delay': 'STRING',
    'hits': 'INT64',
    'contactId': 'STRING',
    'date_read': 'TIMESTAMP',
    'date_sent': 'TIMESTAMP',
    'unsubscribed': 'BOOL',
    'subject': 'STRING',
    'email_id': 'STRING'
  }


#google_space_webhook
workflow_monitoring_chat_space='https://chat.googleapis.com/v1/spaces/AAAAiJpQEBY/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=MgEc9zH2zgq7qmuOnDC-PpYhJv9jyLOABBF8YrkvNaQ'