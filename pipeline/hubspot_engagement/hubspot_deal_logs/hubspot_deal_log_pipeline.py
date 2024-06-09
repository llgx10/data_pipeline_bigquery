import pandas as pd
import os
import json
import sys
import uuid
import pytz

sys.path.append("./")
from node.bigquery.bigquery import BigQuery
from node.redash.Redash import RedashClient
from datetime import datetime, timedelta
from dateutil import parser
import ast
from node.chats.googlechat import chats
from node.hubspot.hubspotoop import HubspotAPI


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
    # df["properties"] = df["properties"].apply(json.loads)

    df["deal_stage"] = df["properties"].apply(
        lambda x: x.get("dealstage").get("versions")
        if isinstance(x, dict) and x and len(x) > 0
        else None
    )
    # df.to_csv("test_unnest.csv")
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
    # df["updated_at_date"] += timedelta(hours=7)
    # df["updated_at_timestamp"] = df["updated_at_timestamp"].apply(add_7_hours)
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


def create_cursor(credentials_path):
    df = pd.DataFrame()
    df["field"] = ["dealstage"]
    df["cursor_date_timestamp"] = [int(datetime.timestamp(datetime.now()) * 1000)]
    df["cursor_date"] = [datetime.now()]
    df["object_type"] = ["deal"]
    df["emitted_at"] = [datetime.now().replace(microsecond=0) - timedelta(hours=7)]
    df["emitted_id"] = [uuid_value] * len(df)
    df = df.astype(str)
    print(df)
    bq = BigQuery(credentials_path)
    bigquery_config = {
        "project_id": "bizzi-home",
        "dataset_id": "hubspot",
        "table_id": "hubspot_change_log_cursor",
        "write_disposition": "WRITE_APPEND",
    }
    bq.dataframe_append_to_bigquery(df, bigquery_config)


def workflow(credentials_path, hubspot_crential_path, redash_credential_path):
    gmt7 = pytz.timezone("Asia/Bangkok")  # Replace 'Asia/Bangkok' with the appropriate timezone for GMT+7
    started = datetime.now(gmt7)
    with open(hubspot_crential_path, "r") as file:
        hubspot_key = json.load(file)
    app_token = hubspot_key.get("app_token")
    # bq = BigQuery(credentials_path)

    today = datetime.today().date()
    redash_url = "https://stats.bizzi.vn"
    rd = RedashClient(redash_url, redash_credential_path)
    data = rd.get_fresh_query_result(query_id="1410", params=None)
    mindate = data[0].get("DATE")
    mindate="2024-02-28 00:00:00.993809"
    hubspot = HubspotAPI(app_token)
    data = hubspot.get_deal_v1()
    df = extract_deal_stage(data)
    print(df)
    df = df[df["updated_at_date"] > mindate]
    print(df)
    bq = BigQuery(credentials_path)
    bigquery_config = {
        "project_id": "bizzi-home",
        "dataset_id": "hubspot",
        "table_id": "hubspot_change_log",
        "write_disposition": "WRITE_APPEND",
    }
    if len(df) > 0:
        df_dict=df.to_dict(orient='records')
        schema={
                "object_id": "STRING",
                "raw": "STRING",
                "field": "STRING",
                "version": "INT64",
                "updated_by": "STRING",
                "updated_value": "STRING",
                "updated_at_timestamp": "INT64",
                "updated_at_date": "TIMESTAMP",
                "object_type": "STRING",
                "emitted_at": "TIMESTAMP",
                "emitted_id": "STRING"
                }
        bq.json_append_to_bigquery(df_dict, bigquery_config,schema)
        create_cursor(credentials_path)
        
        
    ended = datetime.now(gmt7)
    started_formatted = started.strftime("%Y-%m-%d %H:%M:%S")
    ended_formatted = ended.strftime("%Y-%m-%d %H:%M:%S")
    elapsed_time = ended - started
    elapsed_time_seconds = round(elapsed_time.total_seconds(), 2)
    # return True
    payload = json.dumps(
        {
            "cardsV2": [
                {
                    "cardId": "unique-card-id",
                    "card": {
                        "header": {
                            "title": "Hubspot deal logs to BigQuery",
                            "subtitle": "Workflow Finished Running",
                            "imageUrl": "https://static-00.iconduck.com/assets.00/gitlab-icon-512x471-wfbmkpzi.png",
                            "imageType": "SQUARE",
                        },
                        "sections": [
                            {
                                "header": "Informations: ",
                                "collapsible": False,
                                "widgets": [
                                    {
                                        "textParagraph": {
                                            "text": f"<b> Started At: </b>{started_formatted} "
                                        }
                                    },
                                    {
                                        "textParagraph": {
                                            "text": f"<b> Ended At: </b> {ended_formatted}"
                                        }
                                    },
                                    {
                                        "textParagraph": {
                                            "text": f"<b> Time run:</b> {elapsed_time_seconds} seconds"
                                        }
                                    },
                                    {
                                        "textParagraph": {
                                            "text": f"<b> Row processed: </b>{len(df)}"
                                        }
                                    },
                                    {"divider": {}},
                                    {
                                        "buttonList": {
                                            "buttons": [
                                                {
                                                    "text": "Source check",
                                                    "onClick": {
                                                        "openLink": {
                                                            "url": "https://app.hubspot.com/contacts/6319203/objects/0-3/views/18351922/list"
                                                        }
                                                    },
                                                },
                                                {
                                                    "text": "Destination Check",
                                                    "color": {
                                                        "red": 0,
                                                        "green": 0.5,
                                                        "blue": 1,
                                                        "alpha": 1,
                                                    },
                                                    "onClick": {
                                                        "openLink": {
                                                            "url": "https://stats.bizzi.vn/queries/1407/source"
                                                        }
                                                    },
                                                },
                                            ]
                                        }
                                    },
                                ],
                            }
                        ],
                    },
                }
            ]
        }
    )
    chat = chats(
        "https://chat.googleapis.com/v1/spaces/AAAAiJpQEBY/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=MgEc9zH2zgq7qmuOnDC-PpYhJv9jyLOABBF8YrkvNaQ"
    )
    chat.post_message(payload)
    print("acitivities message send to google chat")


