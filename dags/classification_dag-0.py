from airflow.decorators import dag, task
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from decouple import config
import json
from pathlib import Path
import pandas as pd
import pendulum


AZURE_STORAGE_ACCOUNT_URL = "https://projetotera.blob.core.windows.net"
AZURE_STORAGE_ACCOUNT_KEY = config("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER = "datasets"

PROJECT_PATH = Path.cwd().resolve().parent
RAW_DATASETS_PATH = PROJECT_PATH.joinpath("data/raw")
TEMP_DATASETS_PATH = PROJECT_PATH.joinpath("data/temp")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["classification"],
)
def tweet_classification_dag():
    @task()
    def extract(file_name, container_name=AZURE_STORAGE_CONTAINER, sep=";"):
        temp_file_name = "temp_dataset_0.csv"
        temp_file_path = TEMP_DATASETS_PATH.joinpath(temp_file_name)

        blob_service_client_instance = BlobServiceClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL, credential=AZURE_STORAGE_ACCOUNT_KEY
        )
        blob_client_instance = blob_service_client_instance.get_blob_client(
            container_name, file_name, snapshot=None
        )

        with open(temp_file_path, "wb") as my_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(my_blob)

        df = pd.read_csv(temp_file_path, sep=sep)

        # To Do: Apagar arquivo temporário para limpar espaço

        return df

    @task()
    def transform(df: pd.DataFrame):
        X = df.info()
        return X

    @task()
    def load(X):
        print(f"--- Output X={X}")

    file_name = "rede_social_candidato_2022_ES.csv"
    df = extract(file_name=file_name)
    X = transform(df)
    load(X)


tweet_classification_dag()
