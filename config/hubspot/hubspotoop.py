import time
import traceback
import requests
import pandas as pd
import sys
import json
import os
from datetime import datetime, timedelta

from tqdm import tqdm
import yaml

sys.path.append("./")

from dotenv import load_dotenv

load_dotenv()


class HubspotAPI:
    def __init__(self):
        hubspot_acount_info= json.loads(os.getenv("HUBSPOT_APPLICATION_CREDENTIALS"))
        self.app_token = hubspot_acount_info.get('app_token')
        self.url=self._get_default_url()
        self.session = self._create_session()
    
    def _create_session(self):
        session = requests.Session()
        session.headers.update({ "Content-Type": "application/json","Authorization": f"Bearer {self.app_token}"})
        return session
    def _get_default_url(self):
        try:
            with open('config/hubspot/hubspot_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
                url = data.get('default').get('url')
                return url
        except Exception as e:
            print(f"Error in load hubspot-config-yml file: {e}")
        return None
        
    def create_contact(self, data):
        try:
            url = f"{self.url}crm/v3/objects/contacts"
            response = self.session.post(url, json=data)
            return response    
        except Exception as e:
            print(e)
        

    def _get_records(self, object_type, payload=None):
        url=f"{self.url}crm/v3/objects/{object_type}"
        try:
            if payload is None:
                payload = {} 
            results = []
            i = 0
            while True:
                response = self.session.get(
                    url, params=payload
                )
                data = response.json()
                i += 1
                print(i)
                time.sleep(2)
                
                
                if isinstance(data, list):
                    results.extend(data) 
                elif isinstance(data, dict):
                    records = data.get("results", [])
                    paging_info = data.get("paging", {})
                    next_page = paging_info.get("next", {})
                    after_value = next_page.get("after")
                    payload["after"] = after_value
                    print(after_value)
                    results.append(records)
                    if not after_value:
                        break

            return results
        except Exception as e:
            print(f"Error occured in: {e}")
            return []
    def _get_records_batch(self, object_type, payload=None):
        url=f"{self.url}crm/v3/objects/{object_type}"
        try:
            if payload is None:
                payload = {}
            results = []
            i = 0
            batch_size = 100

            while True:
                response = self.session.get(url, params=payload)
                data = response.json()
                i += 1
                print(i)
                time.sleep(2)

                if isinstance(data, list):
                    results.extend(data)
                elif isinstance(data, dict):
                    records = data.get("results", [])
                    paging_info = data.get("paging", {})
                    next_page = paging_info.get("next", {})
                    after_value = next_page.get("after")
                    payload["after"] = after_value
                    results.extend(records)
                    if not after_value:
                        break
                if i % batch_size == 0:
                    self._save_results_to_json(results,object_type, i // batch_size)
                    results = []  # Clear results list

            return results
        except Exception as e:
            print(f"Error occurred: {e}")
            return []
    def _save_results_to_json(self, results,object_type, batch_number):
        filename = f"{object_type}_{batch_number}.json"
        filepath = os.path.join("workflows\\hubspot_engagement\\temp_json", filename) 
        with open(filepath, "w") as f:
            json.dump(results,f,ensure_ascii=False)

    def _get_single_records(self, object_type,object_id, payload=None):
        url=f"{self.url}crm/v3/objects/{object_type}/{object_id}"
        try:
            if payload is None:
                payload = {}
            response = self.session.get(
                url, params=payload
            ) 
            data = response.json()
                    
            return data
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def _search_records(self,object_type, payload):
        try:
            url=f"{self.url}crm/v3/objects/{object_type}/search"
            results = []
            while True:
                response = self.session.post(url, data=payload)
                data = response.json()
                records = data.get("results", [])
                paging_info = data.get("paging", {})
                next_page = paging_info.get("next", {})
                after_value = next_page.get("after")
                results+=records
                print(after_value)
                if not after_value:
                    break
                payload_dict = json.loads(payload)
                payload_dict["after"] = after_value
                payload = json.dumps(payload_dict)
            return results

        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred : {str(e)}")
            return []

    def get_lead_info(self, email):
        payload = json.dumps(
            {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "email",
                                "operator": "EQ",
                                "value": f"{email}",
                            }
                        ]
                    }
                ],
                "properties": [
                    "email",
                    "phone",
                    "hs_lead_status",
                    "lifecyclestage",
                    "hubspot_owner_id",
                ],
            }
        )
        object_type="contacts"
        return self._search_records(object_type, payload)

    def get_company_info_by_tax_code(self, tax_code):
        url = f"{self.url}crm/v3/objects/companies/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "taxcode",
                            "operator": "EQ",
                            "value": f"{tax_code}",
                        }
                    ]
                }
            ],
            "properties": [
                "name",
                "domain",
                "taxcode",
                "hs_lead_status",
                "lifecyclestage",
                "hubspot_owner_id",
            ],
        }
        return self._get_records(url, payload)

    def get_company_info_by_domain(self, domain):
        url = f"{self.url}crm/v3/objects/companies/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "domain",
                            "operator": "EQ",
                            "value": f"{domain}",
                        }
                    ]
                }
            ],
            "properties": [
                "name",
                "domain",
                "taxcode",
                "hs_lead_status",
                "lifecyclestage",
                "hubspot_owner_id",
            ],
        }
        return self._get_records(url, payload)

    def bulk_get_company_by_tax_code(self, df):
        results = []
        for tax_code in df["tax_code"]:
            company_info = self.get_company_info_by_tax_code(tax_code)
            if company_info:
                results.extend(company_info)
        return results

    def bulk_get_company_by_domain(self, df):
        results = []
        for domain in df["domain"]:
            company_info = self.get_company_info_by_domain(domain)
            if company_info:
                results.extend(company_info)
        return results

    def data_profiling(self, objects):
        url = f"{self.url}properties/v1/{objects}/properties"
        return self._get_records(url, {})
    
    def object_properties(self, objects):
        url = f"{self.url}properties/v1/{objects}/properties"
        response=self.session.get(url=url)
        data=response.json()
        return (data)
    def object_properties_v2(self, objects):
        url = f"{self.url}properties/v2/{objects}/properties"
        response=self.session.get(url=url)
        data=response.json()
        names_array = [item["name"] for item in data]
        return (names_array)
    
    def object_properties_v2_full(self, objects):
        url = f"{self.url}properties/v2/{objects}/properties"
        response=self.session.get(url=url)
        data=response.json()
        df=pd.DataFrame
        return df

    def get_time_last_week_in_milliseconds(self):
        current_datetime = datetime.now()
        time_last_week = current_datetime - timedelta(weeks=1)
        time_last_week_in_milliseconds = int(time_last_week.timestamp() * 1000)
        return time_last_week_in_milliseconds

    def check_field_usage(self, properties, objects=None):
        url = f"{self.url}crm/v3/objects/{objects}/search"
        thistime_lasweek = self.get_time_last_week_in_milliseconds()

        for item in properties:
            name = item.get("name")
            print(name)
            if name:
                payload = {
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": name,
                                    "operator": "HAS_PROPERTY",
                                },
                                {
                                    "propertyName": "createdate",
                                    "operator": "GTE",
                                    "value": thistime_lasweek,
                                },
                            ]
                        }
                    ],
                    "limit": 1,
                }
                response = self.session.post(url, json=payload)
                data = response.json()
                total = data["total"]
                print(response.status_code)
                print(total)

        return total

    def get_total_record_count(self, objects):
        if objects == "contacts":
            url = f"{self.url}{objects}/v1/lists/99"
        if objects == "companies":
            url = f"{self.url}{objects}/v1/lists/100"
        try:
            response = self.session.get(url)
            data = response.json()
            return data["metaData"]["size"]
        except Exception as e:
            print(e)

    def get_node_records(self, payload=None):
        url = f"{self.url}crm/v3/objects/notes"
        try:
            return self._get_records(url, payload)
        except Exception as e:
            print(f"Error fetching records: {e}")
            return []

    def delete_objects(self, objects, object_id):
        url = f"{self.url}crm/v3/objects/{objects}/{object_id}"
        try:
            response = self.session.delete(url)
            print(response)
        except Exception as e:
            print(f"Error deleting objects: {e}")
            return []


    def get_deal_v1(self):
        url = f"{self.url}deals/v1/deal/recent/modified?includePropertyVersions=true"
        has_more = True
        parameter_dict = ""
        deal_list = []
        while has_more:
            try:
                response = self.session.get(url + f"&{parameter_dict}")
                data = response.json()
                has_more = data["hasMore"]
                parameter_dict = f"offset={data['offset']}"
                deal_list.append(data["results"])
                print(data["offset"])
                # print(deal_list)
            except Exception as e:
                print(e)
        return deal_list

    def get_full_deal(self):
        url = f"{self.url}deals/v1/deal/paged??includeAssociations=true&propertiesWithHistory=dealstage&properties=dealname&properties=product_name&properties=product_name&properties=amount&properties=createdate&properties=hubspot_owner_id&properties=closedate"
        has_more = True
        parameter_dict = ""
        deal_list = []
        while has_more:
            try:
                response = self.session.get(url + f"&{parameter_dict}")
                data = response.json()
                has_more = data["hasMore"]
                parameter_dict = f"offset={data['offset']}"
                deal_list.append(data["deals"])
                print(data["offset"])
            except Exception as e:
                print(e)
        return deal_list

    def get_archived_objects(self,object,payload):
        url=f"{self.url}crm/v3/objects/{object}?archived=true"
        try:
            if payload is None:
                payload = {}  # Initialize an empty dictionary if payload is None
            results = []
            i = 0
            while True:
                response = self.session.get(
                    url, params=payload
                )
                data = response.json()
                i += 1
                print(i)
                if isinstance(data, list):
                    results.extend(data) 
                elif isinstance(data, dict):
                    records = data.get("results", [])
                    paging_info = data.get("paging", {})
                    next_page = paging_info.get("next", {})
                    after_value = next_page.get("after")
                    payload["after"] = after_value
                    results.extend(records)
                    if not after_value:  # Break the loop if there are no more records
                        break

            return results
        except Exception as e:
            print(f"Error: {e}")
            return []
        
        

    def get_owner_list(self):
        # Fetch active owners
        active_owner_url = f"{self.url}/crm/v3/owners"
        active_owner_dict = self.session.get(active_owner_url).json()
        active_owner_dict = active_owner_dict.get("results", [])
        active_owner_df = pd.DataFrame(active_owner_dict)
        
        # Fetch inactive owners
        unactive_owner_url = f"{self.url}crm/v3/owners?archived=True"
        unactive_owner_dict = self.session.get(unactive_owner_url).json()
        unactive_owner_dict = unactive_owner_dict.get("results", [])
        unactive_owner_df = pd.DataFrame(unactive_owner_dict)
        

        combined_df = pd.concat([active_owner_df, unactive_owner_df], ignore_index=True)
        
        return combined_df
    
    def create_email_engagement_on_contacts(self,email_properties):
        url = f"{self.url}crm/v3/objects/emails"
        try:
            response = self.session.post(url,data=email_properties)
            print(response.status_code)
        except Exception as e:
            print(f"Error creating email engagement: {e}")
            
    def extract_properties(self,objects):
        data=self.object_properties(objects=objects)
        names_array = [obj["name"] for obj in data]
        return (names_array)
    
    def get_object_in_batch(self,object_type,object_id_list):
        url=f"{self.url}crm/v3/objects/{object_type}/batch/read"
        all_responses = []
        try:
            batch_size = 100
            num_batches = len(object_id_list) // batch_size + (len(object_id_list) % batch_size > 0)  # Calculate total number of batches
            for i in tqdm(range(0, len(object_id_list), batch_size), total=num_batches, desc='Processing batches'):  # Use tqdm for progress bar
                batch_ids = object_id_list[i:i+batch_size]
                payload = {
                    "associations": "contacts,companies,deals,calls,notes,meetings,tasks,conversations,emails",
                    "properties": self.object_properties_v2(object_type), 
                    "inputs": batch_ids,
                }

                response = self.session.post(url, json=payload)  
                data = response.json()
                all_responses.append(data["results"])
                time.sleep(4)
        except Exception as e:
                print(f"Error: {e}")
                traceback.print_exc()
        flattened_list = [item for sublist in all_responses for item in sublist]
        return flattened_list
    
    def patch_update_object(self,object_type,object_id,properties):
        url=f"{self.url}crm/v3/objects/{object_type}/{object_id}"
        try:
            response = self.session.patch(url,data=properties)
            print(object_id,response.status_code,response.text)
        except Exception as e:
            print(f"Error updating object: {e}")
            
    def get_association_in_batch(self, from_object, to_object, id_list):
        association_list=[]
        url = f"{self.url}crm/v3/associations/{from_object}/{to_object}/batch/read"
        try:
            # Split id_list into batches of 100 items
            for i in tqdm(range(0, len(id_list), 100), desc="Processing batches", leave=False):
                batch_ids = id_list[i:i+100]
                param = {"inputs": batch_ids}
                response = self.session.post(url, json=param)
                data = response.json()
                association = data.get("results")
                association_list += association
            return association_list
        except Exception as e:
            print(f"Error occurred while getting associations: {e}")