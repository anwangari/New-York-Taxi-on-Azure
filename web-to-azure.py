from dotenv import load_dotenv
import os
import requests
from azure.storage.blob import BlobServiceClient
import requests

# read environment varibles from memory
load_dotenv()

STORAGE_ACCOUNT_URL = os.getenv("STORAGE_ACCOUNT_URL")
STORAGE_ACCOUNT_KEY = os.getenv("CREDENTIAL")
CONTAINER = os.getenv("CONTAINER_NAME")

def upload_web_data_to_azure(storage_account_url, container_name, credential):
    # connect to data store and create container
    blob_service_client = BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY
    )
    try:
        container_client = blob_service_client.get_container_client(CONTAINER)
        if not container_client.exists():
            container_client.create_container()
            print(f"Container '{container_name}' created.")
    except Exception as e:
        print(f"Error accessing or creating container: {e}")
        return

    # iTERATE THOUGH SERVICES AND MONTHS    
    for year in ["2019", "2020"]:
        for service in ['yellow', 'green']:
          for i in range(1,13):
              month = f"0{i}"
              month = month[-2:]
              base_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
              f_name = f"{service}_tripdata_{year}-{month}.parquet"
              file_url = f"{base_url}{f_name}"
    
              try:
                # Download the file from the web
                response = requests.get(file_url, stream=True)
                response.raise_for_status()  # Raise an error for bad status codes
                    
                # Upload the file to Azure
                blob_client = container_client.get_blob_client(f_name)
                blob_client.upload_blob(response.content, overwrite=True)
                
                print(f"Uploaded: {f_name}")
              except Exception as e:
                print(f"Failed to upload {f_name}: {e}")

    return "Upload complete"

