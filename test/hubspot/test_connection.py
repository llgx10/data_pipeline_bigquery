import sys
import json
sys.path.append("./")
from config.hubspot.hubspotoop import HubspotAPI
from pipeline.functions.functions import get_engagement_cursor_date


def get_recently_modified_companies(cursor_date):
    try:
        hb=HubspotAPI()
        payload = json.dumps(
            {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "hs_lastmodifieddate",
                                "operator": "GT",
                                "value": f"{cursor_date}"
                            }
                        ]
                    }
                ],
                "associations": "companies,companies,deals,call,note",
                "limit":100
            }
        )
        recently_modified_companies=hb._search_records('companies',payload)
        return recently_modified_companies
    except Exception as e:
        print("Error in get_recently_modified_companies: ", str(e))
        return None
    
data=get_recently_modified_companies(0)
print(data)