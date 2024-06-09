import requests
import json
import time

class RedashClient:
    def __init__(self, redash_url, api_key_path):
        self.redash_url = redash_url
        self.api_key = self._read_api_key(api_key_path)
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        session.headers.update({"Authorization": f"Key {self.api_key}"})
        return session

    def _read_api_key(self, api_key_path):
        with open(api_key_path, 'r') as file:
            api_key_data = json.load(file)
            return api_key_data['api_key']

    def _poll_job(self, job):
        while job["status"] not in (3, 4):
            response = self.session.get(f"{self.redash_url}/api/jobs/{job['id']}")
            job = response.json()["job"]
            time.sleep(1)
        print(job)
        if job["status"] == 3:
            return job["query_result_id"]
        return None

    def _get_fresh_query_result(self, query_id, params):
        payload = dict(max_age=0, parameters=params)
        response = self.session.post(
            f"{self.redash_url}/api/queries/{query_id}/results",
            data=json.dumps(payload),
        )
        if response.status_code != 200:
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

    def get_query_result(self, query_id, params):
        try:
            return self._get_fresh_query_result(query_id, params)
        except Exception as e:
            print(f"Error: {e}")
            return None

# # Example usage:
# redash_url = "your_redash_url"
# api_key_path = "path/to/your/api_key.json"
# query_id = 123  # Replace with the actual query ID
# params = {"param1": "value1", "param2": "value2"}  # Replace with actual parameters

# redash_client = RedashClient(redash_url, api_key_path)
# result = redash_client.get_query_result(query_id, params)

# if result:
#     print(result)
# else:
#     print("Query execution failed.")
