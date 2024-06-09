import requests
import json
import time
import sys
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())


class Redash:
    def __init__(self):
        self.redash_url = self._get_url()
        redash_acount_info= json.loads(os.getenv("REDASH_APPLICATION_CREDENTIALS"))
        self.api_key = redash_acount_info.get('api_key')
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        session.headers.update({"Authorization": f"Key {self.api_key}"})
        return session
    
    def _get_url(self):
        try:
            with open('config/redash/redash_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
            bizzi_redash= data.get('bizzi-redash')
            return bizzi_redash['url']    
        except Exception as e:
            print(f"Error in load redash-config-yml file: {e}")
            
    def get_query_id(self,query_name):
        try:
            with open('config/redash/redash_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
            bizzi_redash= data.get('bizzi-redash')
            queries=bizzi_redash['queries']    
            for query in queries:
                if query['name'] == query_name:
                    return query['id']
        except Exception as e:
            print(f"Error in load redash-config-yml file: {e}")

    def _poll_job(self, job):
        while job["status"] not in (3, 4):
            response = self.session.get(f"{self.redash_url}/api/jobs/{job['id']}")
            job = response.json()["job"]
            time.sleep(1)
        print(job)
        if job["status"] == 3:
            return job["query_result_id"]
        return None

    def get_fresh_query_result(self, query_id, params):
        if params == None:
            payload = dict(max_age=0)
        else:
            payload = params
        response = self.session.post(
            f"{self.redash_url}/api/queries/{query_id}/results",
            data=json.dumps(payload),
        )
        if response.status_code != 200:
            print(response.text)
            raise Exception("Refresh failed.")
        result_id = self._poll_job(response.json()["job"])

        if result_id:
            response = self.session.get(
                f"{self.redash_url}/api/queries/{query_id}/results/{result_id}.json"
            )
            if response.status_code != 200:
                raise Exception("Failed getting results.")
        else:
            raise Exception("Query execution failed.")
        return response.json()["query_result"]["data"]["rows"]

