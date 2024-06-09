import requests
import time
import json
import datetime
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
import yaml
sys.path.append("./")
from transformation.transformation import traverse_dict
from dotenv import load_dotenv

load_dotenv()

class Mautic:
    def __init__(self):
        mautic_account_info= json.loads(os.getenv("MAUTIC_APPLICATION_CREDENTIALS"))
        self.username = mautic_account_info.get('username')
        self.password = mautic_account_info.get('password')
        self.session = self._create_session()
        
        
    def _create_session(self):
        session = requests.Session()
        session.auth = (self.username, self.password)
        return session

    def get_date(self):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        today = today.strftime("%Y-%m-%d")
        return today
    
    
    def get_api_endpoint(self,endpoint_name):
        try:
            with open('config/mautic/mautic_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
            endpoints= data.get('api_endpoint')
            for endpoint in endpoints:
                if endpoint['name'] == endpoint_name:
                    return endpoint['url']    
        except Exception as e:
            print(f"Error in load mautic-config-yml file: {e}")

    def get_report(self, report_id,date_from,date_to):
        page = 1
        full_report = []
        url = f"{self.get_api_endpoint('get_report')}/{report_id}?limit=100"
        if date_from is not None and date_to is not None:
            url = f"{self.get_api_endpoint('get_report')}/{report_id}?limit=100&dateFrom={date_from}&dateTo={date_to}"
        while True:
            params = {"page": page}
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                if not data["data"]:
                    break

                full_report.extend(data["data"])
                page += 1
                print(page)
                time.sleep(3)
            else:
                print(f"Request failed with status code: {response.status_code}")
                break
        return full_report




    def get_user_list(self, limit):
        page = 1
        record_count = 0
        data_list = []
        yesterday = self.get_date()
        url = f"{self.get_api_endpoint('get_report')}/14"

        while True:
            params = {
                "page": page,
                "dateFrom": f"{yesterday} 00:00:00",
                "dateTo": f"{yesterday} 23:59:59",
                "limit": limit,
            }
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                record_count += len(data["data"])
                data_list.extend(data["data"])
                if data["totalResults"] == record_count:
                    break

                page += 1
                time.sleep(3)
            else:
                print(f"Request failed with status code: {response.status_code}")
                break

        contact_ids, email, phone, firstname, lastname, created_at = (
            [],
            [],
            [],
            [],
            [],
            [],
        )

        for item in data_list:
            email.append(item["email"])
            contact_ids.append(item["id"])
            phone.append(item["phone"])
            firstname.append(item["firstname"])
            lastname.append(item["lastname"])
            created_at.append(item["date_identified"])

        df = pd.DataFrame(
            {
                "contact_id": contact_ids,
                "email": email,
                "firstname": firstname,
                "lastname": lastname,
                "phone": phone,
                "created_at": created_at,
            }
        )

        return df

    def get_mautic_contact(self, contact_id):
        url = f"{self.get_api_endpoint('get_contacts')}/{contact_id}"
        response = self.session.get(url)

        if response.status_code == 200:
            contact_data = response.json()
            if not contact_data["contact"]:
                print("Contact not found")
                return None
            return contact_data

        else:
            print(f"Request failed with status code: {response.status_code}-{response.text}")
            return None

    def get_all_activities(self):
        base_url = self.get_api_endpoint("get_activities")
        auth = (self.username, self.password)
        order_col = "date_added"
        order_dir = "asc"
        limit = 100
        start = 400

        activities_data = []

        while True:
            url = f"{base_url}?order[0][col]={order_col}&order[0][dir]={order_dir}&page={start}&limit={limit}"
            print(url)
            response = self.session.get(url, auth=auth)

            if response.status_code != 200:
                print(
                    f"Error: Unable to get data. Status Code: {response.status_code}"
                )
                break

            data = response.json()

            if not data.get("events"):
                break

            activities_data = data["events"]
            with open(
                f"data/raw/mautic/activities/activities_{start}.json",
                "w",
                encoding="utf-8",
            ) as json_file:
                json.dump(activities_data, json_file, ensure_ascii=False, indent=4)

            start += 1
            print(start)
        return activities_data
    
    
    
    def get_sepcific_activities(self,activities):
        base_url = self.get_api_endpoint("get_activities")
        order_col = "date_added"
        order_dir = "asc"
        limit = 5000
        start = 1        
        while True:
            url = f"{base_url}?filters[includeEvents][]={activities}&order[0][col]={order_col}&order[0][dir]={order_dir}&page={start}&limit={limit}"
            print(url)
            response = self.session.get(url)
            if response.status_code != 200:
                print(
                    f"Error: Unable to get data. Status Code: {response.status_code}"
                )
                break

            data = response.json()

            if not data.get("events"):
                break

            # Process the data as needed
            activities_data = data["events"]
            with open(
                f"data/raw/mautic/activities/{activities.replace('.','_')}/activities_{start}.json",
                "w",
                encoding="utf-8",
            ) as json_file:
                json.dump(activities_data, json_file, ensure_ascii=False, indent=4)

            start += 1
            print(start)
        return activities_data

    def get_all_contacts(self):
        base_url = self.get_api_endpoint("get_contacts")
        order_col = "date_added"
        order_dir = "asc"
        limit = 1000
        start = 0
        while True:
            url = f"{base_url}?order[0][col]={order_col}&order[0][dir]={order_dir}&start={start}&limit={limit}"
            print(url)

            response = self.session.get(url)

            if response.status_code != 200:
                print(
                    f"Error: Unable to get data. Status Code: {response.status_code}"
                )
                break

            data = response.json()

            if not data.get("contacts"):
                break

            contact_data = data["contacts"]
            with open(
                f"workflows/Mautic_migration/contacts/contacts{start}.json",
                "w",
                encoding="utf-8",
            ) as json_file:
                json.dump(contact_data, json_file, ensure_ascii=False, indent=4)

            start += limit
            print(start)

        return contact_data

    def get_recently_modified_contacts(self, since) -> pd.DataFrame:
        base_url = self.get_api_endpoint('get_contacts')
        limit = 100
        start = 0

        full_contact_data = []

        while True:
            url = f"{base_url}?start={start}&limit={limit}&where[0][expr]=orX&where[0][val][0][expr]=gte&where[0][val][0][val]={since}&where[0][val][1][col]=date_modified&where[0][val][1][expr]=gte&where[0][val][0][col]=date_added&where[0][val][1][val]={since}"
            response = self.session.get(url)
            if response.status_code != 200:
                print(
                    f"Error: Unable to get data. Status Code: {response.status_code}"
                )
                break
            data = response.json()

            if not data.get("contacts"):
                break

            contacts_data = traverse_dict(data["contacts"])
            full_contact_data.extend(contacts_data)
            start += limit
            print(start)
        
        return full_contact_data
        

        

       
    def get_recently_updated_activities(self, mindate):
        try:
            base_url = self.get_api_endpoint("get_activities")
            order_col = "date_added"
            order_dir = "desc"
            date_from = mindate
            activities="filters%5BincludeEvents%5D%5B%5D=page.hit&filters%5BincludeEvents%5D%5B%5D=email.read&filters%5BincludeEvents%5D%5B%5D=email.sent&filters%5BincludeEvents%5D%5B%5D=asset.download&filters%5BincludeEvents%5D%5B%5D=campaign.event&filters%5BincludeEvents%5D%5B%5D=campaign.event.scheduled&filters%5BincludeEvents%5D%5B%5D=%20campaign_membership&filters%5BincludeEvents%5D%5B%5D=lead.source.created&filters%5BincludeEvents%5D%5B%5D=lead.source.identified&filters%5BincludeEvents%5D%5B%5D=lead.donotcontact&filters%5BincludeEvents%5D%5B%5D=dynamic.content.sent&filters%5BincludeEvents%5D%5B%5D=email.failed&filters%5BincludeEvents%5D%5B%5D=email.replied&filters%5BincludeEvents%5D%5B%5D=sms.failed&filters%5BincludeEvents%5D%5B%5D=form.submitted&filters%5BincludeEvents%5D%5B%5D=lead.imported&filters%5BincludeEvents%5D%5B%5D=integration_sync_issues&filters%5BincludeEvents%5D%5B%5D=message.queue&filters%5BincludeEvents%5D%5B%5D=point.gained&filters%5BincludeEvents%5D%5B%5D=segment_membership&filters%5BincludeEvents%5D%5B%5D=sms.sent&filters%5BincludeEvents%5D%5B%5D=stage.changed&filters%5BincludeEvents%5D%5B%5D=sms_reply&filters%5BincludeEvents%5D%5B%5D=lead.utmtagsadded&filters%5BincludeEvents%5D%5B%5D=page.videohit"

            today = self.get_date()
            date_to = today
            limit = 2000
            page = 1

            all_data = []
            url = f"{base_url}?order[0][col]={order_col}&order[0][dir]={order_dir}&page={page}&limit={limit}&filters[dateFrom]={date_from} &filters[dateTo]={date_to} 23:59:59&{activities}"
            print(url)
            while True:

                url = f"{base_url}?order[0][col]={order_col}&order[0][dir]={order_dir}&page={page}&limit={limit}&filters[dateFrom]={date_from} &filters[dateTo]={date_to} 23:59:59&{activities}"
                response = self.session.get(url)

                if response.status_code != 200:
                    print(
                        f"Error: Unable to get data. Status Code: {response.status_code}:{response.text}"
                    )
                    break

                data = response.json()

                if not data.get("events"):
                    break


                events_data = data["events"]
                all_data.extend(events_data)

                page += 1
                print(page)
        except Exception as e:
            print(f"Error: {e}")
        with open ('data\\raw\\mautic\\activities\\recently_updated_activities.json', 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        return all_data

    def get_emails(self):
        base_url = self.get_api_endpoint("get_emails")
        limit = 1000
        start = 0
        all_email_data = []
        while True:
            url = f"{base_url}?start={start}&limit={limit}"
            response = self.session.get(url)

            if response.status_code != 200:
                print(
                    f"Error: Unable to get data. Status Code: {response.status_code}"
                )
                break

            data = response.json()
            if not data.get("emails"):
                break
            email_data = traverse_dict(data["emails"])
            all_email_data.extend(email_data)
            start += limit
            print(start)
        return(email_data)
            
