from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
import time
import os
from io import BytesIO
import datetime

class GCSHandler:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.client = storage.Client.from_service_account_json(credentials_path)
    def get_parquet_file_size(self, bucket_name, file_path):
        # Get the blob (file) from the bucket
        blob = self.client.bucket(bucket_name).get_blob(file_path)

        # Get the size of the Parquet file in bytes
        file_size_bytes = blob.size

        # Convert the size from bytes to kilobytes
        file_size_kb = file_size_bytes / 1024

        return file_size_kb
    
    def create_folder(self, bucket_name, folder_name):
        """Creates a folder (directory) in the specified GCS bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(folder_name + '/')
        blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
    
    def list_parquet_files(self, bucket_name, prefix):
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

        parquet_files = [blob.name for blob in blobs if blob.name.lower().endswith('.parquet')]

        # Use pagination to handle large result sets
        next_page_token = blobs.next_page_token
        while next_page_token:
            blobs = bucket.list_blobs(prefix=prefix, delimiter='/', page_token=next_page_token)
            parquet_files.extend([blob.name for blob in blobs if blob.name.lower().endswith('.parquet')])
            next_page_token = blobs.next_page_token

        return parquet_files
    
    def read_all_parquet_files(self, bucket_name, directory):
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=directory)

        parquet_files = [blob.name for blob in blobs if blob.name.lower().endswith('.parquet')]

        if not parquet_files:
            print("No Parquet files found in the directory.")
            return None

        data_frames = []
        for file in parquet_files:
            blob = bucket.blob(file)
            try:
                # Download Parquet file from GCS as bytes
                content = blob.download_as_bytes()
                table = pq.read_table(BytesIO(content))
                data_frames.append(table.to_pandas())
            except Exception as e:
                print(f"Error reading file {file} from GCS: {e}")

        if data_frames:
            # Concatenate all DataFrames into a single DataFrame
            result_df = pd.concat(data_frames, ignore_index=True)
            return result_df
        else:
            return None
                                                                                                                              

    def upload_file(self, bucket_name, file_name, content):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)
        print(f"File {file_name} created in bucket {bucket_name}.")

    def list_files(self, bucket_name):
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        file_list = [blob.name for blob in blobs]
        print(f"Files in bucket {bucket_name}: {file_list}")
        return file_list
    
    def list_buckets(self):
        buckets = list(self.client.list_buckets())
        return [bucket.name for bucket in buckets]

    def create_bucket(self, new_bucket_name):
        new_bucket = self.client.bucket(new_bucket_name)
        new_bucket.create()
        print(f"Bucket {new_bucket_name} created.")
        
    def list_files(self, bucket_name, prefix=''):
        """
        List files in a GCS bucket with an optional prefix.

        Args:
            bucket_name (str): The GCS bucket name.
            prefix (str, optional): The prefix to filter files. Defaults to an empty string.

        Returns:
            list: A list of file names in the GCS bucket.
        """
        try:
            bucket = self.client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            file_list = [blob.name for blob in blobs]
            return file_list
        except Exception as e:
            print(e)
            return None

    def read_files_in_folder(self, bucket_name, folder_path):
        try:
            file_list = self.list_files(bucket_name, prefix=folder_path)
            combined_df = pd.DataFrame()

            for file_name in file_list:
                # Download the Parquet file from GCS
                bucket = self.client.get_bucket(bucket_name)
                blob = bucket.blob(file_name)
                file_content = blob.download_as_text()

                # Read Parquet file into a DataFrame
                parquet_table = pq.read_table(file_content)
                parquet_df = parquet_table.to_pandas()

                # Combine DataFrames
                combined_df = pd.concat([combined_df, parquet_df], ignore_index=True)

            return combined_df
        except Exception as e:
            print(e)
            return None
        
    def list_files_in_years(self, bucket_name, base_folder):
        """
        List Parquet files in year-based subdirectories within the specified range.

        Args:
            bucket_name (str): The GCS bucket name.
            base_folder (str): The base folder (e.g., "A").

        Returns:
            list: A list of file names in the GCS bucket and its subdirectories.
        """
        current_year = datetime.datetime.now().year
        years_range = range(2020, current_year + 1)

        file_list = []

        for year in years_range:
            year_folder = f"{base_folder}/{year}"
            try:
                files_in_year = self.list_files(bucket_name, prefix=year_folder)
                file_list.extend(files_in_year)
            except Exception as e:
                print(f"Error listing files in {year_folder}: {e}")

        return file_list

    def read_files_in_years(self, bucket_name,object, taxcode):
        base_folder=f"dash-application/{object}/{taxcode}"
        file_list = self.list_files_in_years(bucket_name, base_folder)
        combined_df = pd.DataFrame()

        for file_name in file_list:
            # Download the Parquet file from GCS
            bucket = self.client.get_bucket(bucket_name)
            blob = bucket.blob(file_name)
            file_content = blob.download_as_bytes()

            # Read Parquet file into a DataFrame
            parquet_table = pq.read_table(source=BytesIO(file_content))
            parquet_df = parquet_table.to_pandas()

            # Combine DataFrames
            combined_df = pd.concat([combined_df, parquet_df], ignore_index=True)

        return combined_df
    
    def read_parquet_file(self, bucket_name, file_path)->pd.DataFrame:
        try:
            bucket = self.client.get_bucket(bucket_name)
            blob = bucket.blob(file_path)
            content = blob.download_as_bytes()

            # Use pyarrow to read the Parquet file from bytes
            table = pq.read_table(io.BytesIO(content))
            df = table.to_pandas()
            return df

        except Exception as e:
            print(f"Error reading Parquet file {file_path} from GCS: {e}")
            return None

    def create_parquet_file(self, bucket_name, file_path, dataframe, compression='NONE', max_retries=5, retry_delay=5):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)

        # Convert Pandas DataFrame to PyArrow Table
        arrow_table = pa.Table.from_pandas(dataframe)

        for attempt in range(max_retries):
            try:
                # Write PyArrow Table to Parquet file in memory
                with io.BytesIO() as buf:
                    pq.write_table(arrow_table, buf, compression=compression)
                    buf.seek(0)

                    # Upload the Parquet file to GCS
                    blob.upload_from_file(buf, content_type='application/octet-stream')

                print(f"Upload parquet successful on attempt {attempt + 1}")
                break  # If upload is successful, exit the loop
            except Exception as e:
                print(f"Attempt {attempt + 1} failed. Retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)

                if attempt == max_retries - 1:
                    print(f"Max retries reached. Upload failed with error: {str(e)}")

            
    def list_folders(self, bucket_name, prefix):
        bucket = self.client.get_bucket(bucket_name)

        # List blobs with the given prefix
        blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

        # Extract unique folder names from the blobs
        folders = set()
        for blob in blobs:
            folder_name = blob.name.rstrip('/')
            if '/' in folder_name:
                folder_name = folder_name.rsplit('/', 1)[0]
            folders.add(folder_name)

        return list(folders)
    
    def list_all_folders(self, bucket_name):
        bucket = self.client.get_bucket(bucket_name)

        # List all blobs in the bucket
        blobs = bucket.list_blobs()

        # Extract unique folder names from the blobs
        folders = set()
        for blob in blobs:
            folder_name = blob.name.rstrip('/')
            if '/' in folder_name:
                folder_name = folder_name.rsplit('/', 1)[0]
            folders.add(folder_name)

        return list(folders)

    
    def create_folder(self, bucket_name, folder_path):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_path}/')

        blob.upload_from_string('')
    
    def write_string_to_text_file(self, bucket_name, file_path, text, max_retries=5, retry_delay=5):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)

        # Convert string to bytes
        content = text.encode('utf-8')

        for attempt in range(max_retries):
            try:
                # Upload the content to the GCS file
                blob.upload_from_string(content, content_type='text/plain',timeout=120)
                print(f"Upload successful on attempt {attempt + 1}")
                break  # If upload is successful, exit the loop
            except Exception as e:
                print(f"Attempt {attempt + 1} failed. Retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)

                if attempt == max_retries - 1:
                    print(f"Max retries reached. Upload failed with error: {str(e)}")
        
    def read_text_file_from_gcs(self, bucket_name, file_path):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)

        try:
            # Download the content of the text file
            content = blob.download_as_text()
            return content
        except Exception as e:
            print(f"An error occurred while reading the text file: {str(e)}")
            return None
    
    def delete_files_in_path(self, bucket_name, path_prefix):
        try:
            bucket = self.client.get_bucket(bucket_name)

            # List all blobs in the specified path
            blobs = bucket.list_blobs(prefix=path_prefix)

            # Delete each blob (object) in the path
            for blob in blobs:
                blob.delete()
        except Exception as e:
                print(f"An error occured in deleting {path_prefix}: {e}")
        
        
class HmacGCSHandler:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.client = self.create_hmac_client()
        
    def create_hmac_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return storage.Client(credentials=credentials)