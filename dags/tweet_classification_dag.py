from airflow.decorators import dag, task
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from decouple import config
from transformers import pipeline
from pathlib import Path
import pandas as pd
import pendulum


AZURE_STORAGE_ACCOUNT_URL = "https://projetotera.blob.core.windows.net"
AZURE_STORAGE_ACCOUNT_KEY = config("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER = "datasets"

PROJECT_PATH = Path.cwd().resolve().parent
TEMP_DATASETS_PATH = PROJECT_PATH.joinpath("data/temp")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["classification"],
)
def tweet_classification_dag():
    @task()
    def load_data(file_name, container_name=AZURE_STORAGE_CONTAINER, sep=";"):
        temp_file_path = TEMP_DATASETS_PATH.joinpath(file_name)

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
        return df

    @task()
    def classify(
        df: pd.DataFrame,
        col="rawContent",
        model_name="ruanchaves/bert-large-portuguese-cased-hatebr",
    ):
        classifier = pipeline("sentiment-analysis", model=model_name)
        results = df[col].apply(classifier)

        df["class_label"] = results.apply(lambda x: x[0]["label"])
        df["class_score"] = results.apply(lambda x: x[0]["score"])

        return df

    @task()
    def export(df, file_name, container_name=AZURE_STORAGE_CONTAINER, sep=";"):
        output_file_name = "output-" + file_name
        temp_file_path = TEMP_DATASETS_PATH.joinpath(output_file_name)

        df.to_csv(f"{temp_file_path}", sep=sep, encoding="utf-8", index=False)

        service_client = BlobServiceClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL, credential=AZURE_STORAGE_ACCOUNT_KEY
        )
        container_client = service_client.get_container_client(container_name)

        with open(file=temp_file_path, mode="rb") as data:
            container_client.upload_blob(name=output_file_name, data=data)

    @task()
    def cleanup(file_name):
        # To Do: Apagar arquivo temporário para limpar espaço
        # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli#delete-a-container
        pass

    file_name = "erika-short.csv"
    df = load_data(file_name=file_name)
    df = classify(df)
    export(df, file_name=file_name)
    cleanup(file_name=file_name)


tweet_classification_dag()
