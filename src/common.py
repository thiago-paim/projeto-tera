from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
import pandas as pd
from src.config import (
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONTAINER,
    AZURE_STORAGE_ACCOUNT_URL,
    PROCESSED_DATASETS_PATH,
    RAW_DATASETS_PATH,
    PROJECT_PATH,
    TEMP_FILES_PATH,
)


def download_blob(file_name: str) -> str:
    blob_service_client_instance = BlobServiceClient(
        account_url=AZURE_STORAGE_ACCOUNT_URL, credential=AZURE_STORAGE_ACCOUNT_KEY
    )
    blob_client_instance = blob_service_client_instance.get_blob_client(
        AZURE_STORAGE_CONTAINER, file_name, snapshot=None
    )

    temp_file_path = f"{TEMP_FILES_PATH}/{file_name}"
    with open(temp_file_path, "wb") as blob_file:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(blob_file)

    return temp_file_path


def upload_blob(df, file_name: str, sep=";", add_time_signature=True):
    temp_file_path = f"{TEMP_FILES_PATH}/{file_name}"
    if add_time_signature:
        signature = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        name, extension = file_name.split(".")
        file_name = f"{name}-{signature}.{extension}"

    df.to_csv(f"{temp_file_path}", sep=sep, encoding="utf-8", index=False)

    service_client = BlobServiceClient(
        account_url=AZURE_STORAGE_ACCOUNT_URL, credential=AZURE_STORAGE_ACCOUNT_KEY
    )
    container_client = service_client.get_container_client(AZURE_STORAGE_CONTAINER)

    with open(file=temp_file_path, mode="rb") as data:
        container_client.upload_blob(name=file_name, data=data, overwrite=True)


def load_dataset(file_path: str, sep: str = ";") -> pd.DataFrame:
    df = pd.read_csv(f"{PROJECT_PATH}/{file_path}", sep=sep)
    return df


def load_raw_dataset(file_name: str, sep: str = ";") -> pd.DataFrame:
    df = pd.read_csv(f"{RAW_DATASETS_PATH}/{file_name}", sep=sep)
    return df


def load_processed_dataset(file_name: str, sep: str = ";") -> pd.DataFrame:
    df = pd.read_csv(f"{PROCESSED_DATASETS_PATH}/{file_name}", sep=sep)
    return df


def save_dataset(df: pd.DataFrame, file_name: str, sep: str = ";") -> None:
    df.to_csv(f"{PROCESSED_DATASETS_PATH}/{file_name}", sep=sep, index=False)
