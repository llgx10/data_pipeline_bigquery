import json
from google.oauth2 import service_account
import sys
from google.cloud.bigquery import SchemaField
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
from urllib.parse import urlparse
import os
import yaml
sys.path.append("./")
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# path to your .env file
load_dotenv(find_dotenv())


class BigQuery:
    def __init__(self):
        service_account_info=(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        service_account_info = json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        self.client = bigquery.Client(project=service_account_info["project_id"], credentials=credentials)
        
        
    def execute_query(self, query):
        job_config = bigquery.QueryJobConfig(dry_run=False, use_query_cache=False)
        query_job = self.client.query(
            query, job_config=job_config, project=self.client.project
        )

        query_job.result()
        results = list(query_job)
        return results
    def get_table_schema(self,dataset_name, table_name):
        try:
            with open('config/bigquery/big_query_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
                datasets = [data.get('project').get('datasets')]
                for dataset in datasets:
                    if dataset['name'] == dataset_name:
                        tables = dataset.get('tables', [])
                        for table in tables:
                            if isinstance(table, dict) and table.get('name') == table_name:
                                columns = table.get('columns', [])
                                schema_dict = {column['name']: column['data-type'] for column in columns}
                                return schema_dict
                        print(f"Table '{table_name}' not found in the dataset '{dataset_name}'.")
                        return None
                print(f"Dataset '{dataset_name}' not found in the schema.")
                return None
        except FileNotFoundError:
            print(f"Error: File 'config/big_query/big_query_config.yaml' not found.")
            return None
        except yaml.YAMLError as exc:
            print(f"Error while parsing YAML: {exc}")
            return None
    
    def generate_config(self,project_id, dataset_id, table_id):
        return {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id
        }

    def get_table_config(self,dataset_id, table_id):
        with open('config/bigquery/big_query_config.yaml', 'r') as stream:
            try:
                data = yaml.safe_load(stream)
                datasets = [data.get('project').get('datasets')]
                for dataset in datasets:
                    if dataset['name'] == dataset_id:
                        tables = dataset.get('tables', [])
                        for table in tables:
                            if isinstance(table, dict) and table.get('name') == table_id:
                                config=table.get('config')
                                config = {key.replace("-", "_"): value for key, value in config.items()}
                                return config
                    else:
                        print(f"Dataset '{dataset_id}' not found in the schema.")                  
                
            except yaml.YAMLError as exc:
                print("Error while parsing YAML:", exc)
                return None
            
    
    
    def convert_results_to_dataframe(self, results):
        if not results:
            return pd.DataFrame()
        schema = results[0].keys()
        column_names = list(schema)
        data = [list(row.values()) for row in results]
        results_df = pd.DataFrame(data, columns=column_names)
        return results_df
    
    
    def dataframe_append_to_bigquery(self, dataframe, bigquery_config):

        rows_to_insert = dataframe.to_dict(orient="records")

        dataset_ref = self.client.dataset(bigquery_config["dataset_id"])
        table_ref = dataset_ref.table(bigquery_config["table_id"])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery_config.get(
            "write_disposition", "WRITE_APPEND"
        )
        job_config.schema = [
            bigquery.SchemaField(name, "STRING") for name in dataframe.columns
        ]

        load_job = self.client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )

        load_job.result()

        print(
            f'Loaded {len(rows_to_insert)} rows into {bigquery_config["project_id"]}:{bigquery_config["dataset_id"]}.{bigquery_config["table_id"]}'
        )



    def dataframe_append_to_bigquery(self, dataframe, bigquery_config,schema_dict):
        def generate_schema_from_mapping(input_mapping):
            schema = []
            
            for field, data_type in input_mapping.items():
                schema.append(bigquery.SchemaField(field, data_type))

            return schema
    
        rows_to_insert = dataframe.to_dict(orient="records")

        dataset_ref = self.client.dataset(bigquery_config["dataset_id"])
        table_ref = dataset_ref.table(bigquery_config["table_id"])

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery_config.get(
            "write_disposition", "WRITE_APPEND"
        )
        job_config.schema=generate_schema_from_mapping(schema_dict)

        load_job = self.client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        
        load_job.result()

        print(
            f'Loaded {len(rows_to_insert)} rows into {bigquery_config["project_id"]}:{bigquery_config["dataset_id"]}.{bigquery_config["table_id"]}'
        )

    def dataframe_upsert_to_bigquery(
        self, dataframe, bigquery_config, primary_key, cursor_column
    ):

        
        dataframe = dataframe.astype(str)

        dataset_ref = self.client.dataset(bigquery_config["dataset_id"])
        table_ref = dataset_ref.table(bigquery_config["table_id"])

        rows_to_upsert = dataframe.to_dict(orient="records")

        schema = [SchemaField(col, "STRING") for col in dataframe.columns]

        temp_table_id = f'{bigquery_config["table_id"]}_temp'
        temp_table_ref = dataset_ref.table(temp_table_id)

        self.client.load_table_from_dataframe(
            dataframe,
            temp_table_ref,
            job_config=bigquery.LoadJobConfig(schema=schema),
        ).result()

        merge_query = f"""
            MERGE INTO `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` AS target
        USING `{temp_table_ref.project}.{temp_table_ref.dataset_id}.{temp_table_ref.table_id}` AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED AND target.{cursor_column} != source.{cursor_column} THEN
            UPDATE SET 
                {', '.join([f"target.{col} = {'CAST(' + f'source.{col}' + ' AS STRING)' if col in ['created_at', 'updated_at'] else f'source.{col}'}" for col in dataframe.columns])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(dataframe.columns)})
            VALUES ({', '.join([f"source.{col}" for col in dataframe.columns])});

        """     
        
        
        query_job = self.client.query(merge_query)
        query_job.result()
        
        print(
            f'Upserted {len(rows_to_upsert)} rows into {bigquery_config["project_id"]}:{bigquery_config["dataset_id"]}.{bigquery_config["table_id"]}'
        )
        
    
    
    def json_upsert_to_bigquery(
        self, json_data, bigquery_config, primary_key, cursor_column,schema_dict
    ):
        def generate_schema_from_mapping(input_mapping):
            schema = []
            
            for field, data_type in input_mapping.items():
                schema.append(bigquery.SchemaField(field, data_type))

            return schema

        dataset_ref = self.client.dataset(bigquery_config["dataset_id"])
        table_ref = dataset_ref.table(bigquery_config["table_id"])

        columns = list(json_data[0].keys())

        schema=generate_schema_from_mapping(schema_dict)


        temp_table_id = f'{bigquery_config["table_id"]}_temp'
        temp_table_ref = dataset_ref.table(temp_table_id)
        primary_key_values = [item[primary_key] for item in json_data]
        has_duplicates = len(primary_key_values) != len(set(primary_key_values))
        print(f"Has Duplicates in Primary Key: {has_duplicates}")
        try:
            load_job = self.client.load_table_from_json(
                json_data,
                temp_table_ref,
                job_config=bigquery.LoadJobConfig(schema=schema),
            )
            load_job.result()
        except Exception as e:
            print(f"Error loading data into BigQuery: {str(e)}")
            if hasattr(load_job, 'errors'):
                for error in load_job.errors:
                    print(error)


        # Perform upsert using MERGE statement
        merge_query = f"""
            MERGE INTO `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` AS target
        USING `{temp_table_ref.project}.{temp_table_ref.dataset_id}.{temp_table_ref.table_id}` AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED AND target.{cursor_column} != source.{cursor_column} THEN
            UPDATE SET 
                {', '.join([f"target.{col} = {'CAST(' + f'source.{col}' + ' AS STRING)' if col in ['created_at', 'updated_at'] else f'source.{col}'}" for col in columns])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(columns)})
            VALUES ({', '.join([f"source.{col}" for col in columns])});

        """
        
        
        query_job = self.client.query(merge_query)

     
        query_job.result()
        
        
        clear_temp_query=f"truncate table {dataset_ref}.{temp_table_id }"
        query_job2 = self.client.query(clear_temp_query)
        query_job2.result()

        print(
            f'Upserted {len(json_data)} rows into {bigquery_config["project_id"]}:{bigquery_config["dataset_id"]}.{bigquery_config["table_id"]}'
        )
        
    def json_append_to_bigquery(self, json_data, bigquery_config, schema_dict):
        if json_data is None or len(json_data) == 0:
            print("No data to append to BigQuery")
            return None
        
        print("start appending to bigquery")
        def generate_schema_from_mapping(input_mapping):
            schema = []
            for field, data_type in input_mapping.items():
                schema.append(bigquery.SchemaField(field, data_type, mode='REQUIRED'))
            return schema


        try:
            dataset_ref = self.client.dataset(bigquery_config["dataset_id"])
            table_ref = dataset_ref.table(bigquery_config["table_id"])

            schema = generate_schema_from_mapping(schema_dict)

            # Load data into BigQuery
            load_job = self.client.load_table_from_json(
                json_data,
                table_ref,
                job_config=bigquery.LoadJobConfig(schema=schema),
            )
            job_id = load_job.job_id
            print("Job ID:", job_id)
            load_job.result()
            
            print("Successfully appended data to BigQuery")

        except Exception as e:
            print(f"Error appending data to BigQuery: {str(e)}")
            if hasattr(load_job, 'errors'):
                for error in load_job.errors:
                    print("Job ID:", job_id)
                    print(error)