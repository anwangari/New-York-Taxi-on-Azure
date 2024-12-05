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

def upload_web_data_to_azure(storage_account_url, container_name, credential, directory_name):
    # Connect to data store 
    service_client = DataLakeServiceClient(
        STORAGE_ACCOUNT_URL, credential=credential
    )
    
    try:
        # Get or create file system
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Create file system if it doesn't exist
        if not file_system_client.exists():
            file_system_client.create_file_system()
            print(f"Container '{container_name}' created.")
        
        # Create or get directory
        directory_client = file_system_client.get_directory_client(directory_name)
        if not directory_client.exists():
            directory_client.create_directory()
            print(f"Directory '{directory_name}' created.")
        
    except Exception as e:
        print(f"Error accessing or creating container/directory: {e}")
        return

    # Iterate through services and months
    for year in range(2019, 2021):
        for service in ['yellow', 'green']:
            for i in range(1, 13):
                month = f"0{i}"
                month = month[-2:]
                base_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f_name = f"{service}_tripdata_{year}-{month}.parquet"
                file_url = f"{base_url}{f_name}"

                try:
                    # Download the file from the web
                    response = requests.get(file_url)
                    response.raise_for_status()  # Raise an error for bad status codes

                    # Upload the file to Azure
                    file_client = directory_client.get_file_client(f_name)
                    file_client.upload_data(response.content, overwrite=True)

                    print(f"Uploaded: {f_name}")
                except Exception as e:
                    print(f"Failed to upload {f_name}: {e}")

    return "Upload complete"
