from google.cloud import storage
import os

key_path = './key.json'
bucket_name = 'news_latest'
folder_path = './Downloads/'

def upload(key_path, bucket_name, file_path):
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.get_bucket(bucket_name)

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_path.lower().endswith('.html'):
                relative_path = os.path.relpath(file_path, folder_path)
                blob = bucket.blob(relative_path)
                blob.upload_from_filename(file_path)
                print(file_name)

upload(key_path, bucket_name, folder_path)
