from pyspark.sql import SparkSession, DataFrame
from functools import reduce
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import os
import requests

spark = SparkSession.builder \
        .appName('Taxi Trip Data') \
        .getOrCreate()

load_dotenv()

STORAGE_ACCOUNT_URL = os.getenv("STORAGE_ACCOUNT_URL")
STORAGE_ACCOUNT_KEY = os.getenv("CREDENTIAL")
CONTAINER = os.getenv("CONTAINER_NAME")
DIRECTORY_NAME = os.getenv("DIRECTORY_NAME")

def web_to_azure_data_lake(service, years, storage_account_url, container_name, credential, directory_name):
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
        service_directory_name = directory_name + "/" + service
        directory_client = file_system_client.get_directory_client(service_directory_name)
        if not directory_client.exists():
            directory_client.create_directory()
            print(f"Directory '{service_directory_name}' created.")

    except Exception as e:
        print(f"Error accessing or creating container/directory: {e}")
        return
    
    #Iterate through the source files
    dataframes = []
    for year in years:
        for x in range(1,13):
            month = f"0{x}" if x < 10 else f"{x}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet"
            spark.sparkContext.addFile(url)
            df = spark.read.parquet(SparkFiles.get(f"{service}_tripdata_{year}-{month}.parquet"))
            dataframes.append(df)
            del df

    # Combine all DataFrames into a single DataFrame
    combined_df = reduce(DataFrame.union, dataframes)

    # Generate the output file name
    years_str = "-".join(map(str, years))
    output_dir = f"{service}_tripdata_{years_str}_output"
    file_name = f"{service}_tripdata_{years_str}.parquet"

    # Save the combined DataFrame to a single output file
    combined_df.coalesce(1).write.parquet(output_dir, mode="overwrite")
    print(f"Data combined and saved to: {output_dir}")

    parquet_file = None
    for f_name in os.listdir(output_dir):
        if f_name.endswith(".parquet"):
            parquet_file = os.path.join(output_dir, f_name)
            break

    if not parquet_file:
        print("Error: No '.parquet' file found in the output directory.")
        return
    
    # Upload to Azure Data Lake
    try:
        with open(parquet_file, "rb") as file_data:
            file_client = directory_client.get_file_client(file_name)
            file_client.upload_data(file_data, overwrite=True)
        return "Upload complete"
    except Exception as e:
        return f"Error uploading file to Azure Data Lake: {e}"
