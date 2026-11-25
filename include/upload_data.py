import os 
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path


location = Path(__file__).parent

project_root= location.parent

load_dotenv()

ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
ACCOUNT_NAME = 'cityservicestore'
CONTAINER_NAME = 'bronze1'

LOCAL_FILE_PATH = project_root / "data" / "servicedata.csv"



BLOB_NAME = "servicedata.csv"



def upload_data():
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=ACCOUNT_KEY
        )

        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

        print(f"Starting file upload from {LOCAL_FILE_PATH} to blob storage...")

        with open(LOCAL_FILE_PATH,'rb') as data:
            blob_client.upload_blob(
            data,
            overwrite=True, 
            max_concurrency=4)

        print(f"File upload completed successfully to {CONTAINER_NAME}")

    except Exception as e:
        print(e)

    return None

#upload_data()



