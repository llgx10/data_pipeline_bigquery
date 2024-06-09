import pandas as pd
import os
import json
import sys
import uuid
import pytz
import warnings
warnings.filterwarnings("ignore")
sys.path.append("./")

from config.bigquery.bigquery import BigQuery
from config.redash.Redash import Redash
from datetime import datetime, timedelta
from dateutil import parser
import ast
from config.hubspot.hubspotoop import HubspotAPI
from pipeline.functions.functions import workflow_monitoring

uuid_value = str(uuid.uuid4())
        

def create_dict(row):
    return {
        "dealId": row["dealId"],
        "field": row["field"],
        "version": row["version"],
        "made_by": row["made_by"],
        "new_value": row["new_value"],
        "source": row["source"],
        "updated_by_user_id": row["updatedByUserId"],
        "request_id": row["requestId"],
        "updated_at": row["updated_at"],
        "updated_at_date": row["updated_at_date"],
    }


def add_7_hours(timestamp_ms):
    timestamp_sec = timestamp_ms / 1000
    dt_object = datetime.utcfromtimestamp(timestamp_sec)
    dt_object_plus_7_hours = dt_object + timedelta(hours=7)
    return int(dt_object_plus_7_hours.timestamp() * 1000)


def extract_deal_stage(data):
    flattened_list_of_dicts = [item for sublist in data for item in sublist]

    df = pd.DataFrame(flattened_list_of_dicts)
    df["deal_stage"] = df["properties"].apply(
        lambda x: x.get("dealstage").get("versions")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    df_exploded = df.explode("deal_stage")
    df_exploded = df_exploded.reset_index(drop=True)

    df_exploded["field"] = "dealstage"
    df_exploded["new_value"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("value") if isinstance(x, dict) and x and len(x) > 0 else None
    )
    df_exploded["made_by"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("sourceId")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    df_exploded["source"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("source") if isinstance(x, dict) and x and len(x) > 0 else None
    )
    df_exploded["updated_at"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("timestamp")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    df_exploded["updatedByUserId"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("updatedByUserId")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    
    df_exploded["updatedByUserId"] = df_exploded["updatedByUserId"].astype('Int64')
    


    df_exploded["requestId"] = df_exploded["deal_stage"].apply(
        lambda x: x.get("requestId")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    df_exploded.sort_values(by=["dealId", "updated_at"], inplace=True)
    df_exploded["version"] = df_exploded.groupby("dealId").cumcount() + 1
    df_exploded["updated_at_date"] = pd.to_datetime(
        df_exploded["updated_at"], unit="ms"
    )
    df_exploded["raw"] = df_exploded.apply(lambda row: create_dict(row), axis=1)
    df_exploded["raw"] = df_exploded["raw"].apply(
        lambda x: json.dumps(x, default=str) if pd.notnull(x) else None
    )
    df = df_exploded[
        [
            "dealId",
            "raw",
            "field",
            "version",
            "made_by",
            "new_value",
            "updated_at",
            "updated_at_date",
        ]
    ]

    df.sort_values(by=["dealId", "updated_at"], inplace=True, ascending=False)
    # df = df.drop_duplicates(subset="dealId", keep="first")
    df.rename(
        columns={"dealId": "object_id", "updated_at": "updated_at_timestamp"},
        inplace=True,
    )
    df["object_type"] = "deal"
    df["emitted_at"] = datetime.now()
    df["emitted_at"] = df["emitted_at"].apply(lambda x: x.replace(microsecond=0))
    df["updated_at_date"] = df["updated_at_date"].apply(
        lambda x: x.replace(microsecond=0)
    )
    df["emitted_id"] = [uuid_value] * len(df)
    df['updated_at_date'] = df['updated_at_date'].astype(str)
    df['emitted_at'] = df['emitted_at'].astype(str) 
    
    df.rename(
            columns={
                "dealId": "object_id",
                "updated_at": "updated_at_timestamp",
                "made_by": "updated_by",
                "new_value": "updated_value",
            },
            inplace=True,
        )

    return df


def create_cursor():
    df = pd.DataFrame()
    df["field"] = ["dealstage"]
    df["cursor_date_timestamp"] = [int(datetime.timestamp(datetime.now()) * 1000)]
    df["cursor_date"] = [datetime.now()]
    df["object_type"] = ["deal"]
    df["emitted_at"] = [datetime.now().replace(microsecond=0) - timedelta(hours=7)]
    df["emitted_id"] = [uuid_value] * len(df)
    df = df.astype(str)
    bq = BigQuery()
    bigquery_config = bq.get_table_config('hubspot', 'hubspot_change_log_cursor')
    bigquery_schema=bq.get_table_schema('hubspot', 'hubspot_change_log_cursor')
    bq.dataframe_append_to_bigquery(df, bigquery_config,bigquery_schema)

@workflow_monitoring
def Hubspot_deal_log_to_BigQuery( ):
    rd = Redash()
    data = rd.get_fresh_query_result(query_id="1420", params=None)
    mindate = data[0].get("DATE")
    hubspot = HubspotAPI()
    data = hubspot.get_deal_v1()
    df = extract_deal_stage(data)
    df = df[df["updated_at_date"] > mindate]
    bq = BigQuery()
    bigquery_config = bq.get_table_config('hubspot', 'hubspot_change_log')
    if len(df) > 0:
        df_dict=df.to_dict(orient='records')
        create_cursor()
        schema=bq.get_table_schema('hubspot', 'hubspot_change_log')
        bq.json_append_to_bigquery(df_dict, bigquery_config,schema)
    return len(df)
        
        
    
if __name__ == "__main__":
    Hubspot_deal_log_to_BigQuery()