import time
import pandas as pd
import sys
import json
from datetime import datetime

import requests
sys.path.append("./")
from node.hubspot.hubspotoop import HubspotAPI
from node.redash.Redash import RedashClient
from node.mautic.mautic import Mautic
import constants
from workflows.hubspot_2_bigquery_migration.contacts_pipeline import (
    workflow as contacts_pipeline
)


hubspot_path='gitlab_ci\hubspot\hubspotcrentials_ci.json'
with open (hubspot_path, 'r') as file:
    hubspot_key = json.load(file)
app_token = hubspot_key.get("app_token2")
hb=HubspotAPI(app_token)
url = f"https://api.hubapi.com/crm/v3/objects/companies/search"
payload_search = json.dumps({
    "filters": [
                        {
                            "propertyName": "bizzi_group_id",
                            "operator": "HAS_PROPERTY"
                        },
                        {
                            "propertyName": "description",
                            "operator": "NOT_HAS_PROPERTY"
                        },

                    ],
    "limit":100,
    "properties":["taxcode"],
                }
)
results = hb._search_records("companies", payload_search)

# with open ("companies.json", 'w') as file:
#     json.dump(results, file, ensure_ascii=False, indent=4)


hubspot_companies=pd.DataFrame(results)
hubspot_companies['taxcode']=hubspot_companies["properties"].apply(lambda x: x['taxcode'])

print(hubspot_companies)
for index,row in hubspot_companies.iterrows():
    hubspot_id=row['id']
    workflow_name='[n8n workflow] Đồng bộ dữ liệu từ Bizzi sang Hubspot'
    update_param=json.dumps({"properties":{"workflow":f"{workflow_name}"}})
    hb.patch_update_object("companies",hubspot_id,update_param)


# def concat_string(df, column_name):

#     unique_values = df.drop_duplicates(subset=column_name, keep='first')[column_name]
#     result = "', '".join(unique_values.astype(str))
#     result = "'" + result + "'"
    
#     return result


# tax_code_string=concat_string(hubspot_companies, 'taxcode')

# redash_path='gitlab_ci/redash/redash_credential_ci.json'

# query_id='1530'
# rd=RedashClient(constants.redash_url,redash_path)
# data=rd.get_fresh_query_result(query_id,params={"max_age":0,"parameters":{"tax_code": f"{tax_code_string}"}})
# print(data)
# company_profile=pd.DataFrame(data)
# print(company_profile)
# merge_df=pd.merge(hubspot_companies,company_profile, on='taxcode', how='left')
# print(merge_df)


#     else:
#         update_param=json.dumps({"properties":{"address":f"{address}","description":f"Legal name: {legal_name}"}})
#         hb.patch_update_object("companies",hubspot_id,update_param)
    