from datetime import datetime
import sys
from time import time
import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
import pytz
from src.common import (
    download_blob,
    upload_blob,
    load_processed_dataset,
    save_local_dataset,
)
from transformers import pipeline, BertTokenizer, BertForSequenceClassification
import torch
from tqdm import tqdm


# Incomplete

OFFENSE_MODELS = {
    "ru_bert_base": "ruanchaves/bert-base-portuguese-cased-hatebr",
    "ru_bert_large": "ruanchaves/bert-large-portuguese-cased-hatebr",
    "ru_mdeberta_base": "ruanchaves/mdeberta-v3-base-hatebr",
    "ci_distilbert_base": "citizenlab/distilbert-base-multilingual-cased-toxicity",
}


@task
def filter_tweets(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    local_tz = pytz.timezone("America/Sao_Paulo")
    since = datetime(year=2022, month=9, day=1, tzinfo=local_tz)
    until = datetime(year=2022, month=11, day=1, tzinfo=local_tz)

    df[date_col] = pd.to_datetime(df[date_col])

    df = df[(df[date_col] >= since) & (df[date_col] <= until)]

    # df = df.iloc[:100]
    return df


@task
def classify(df: pd.DataFrame, col_name: str, batch_size: int = 32) -> pd.DataFrame:
    logger = get_run_logger()
    tqdm.pandas()

    model_path = "../../models/fine_tuned_model"
    tokenizer = BertTokenizer.from_pretrained(model_path)
    model = BertForSequenceClassification.from_pretrained(model_path)
    model_name = "FineTunedBertBase"

    # Escolhendo o dispositivo adequado (CPU or GPU) caso haja mais que um disponível
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    # Criando pipeline customizado de analise de sentimento
    # OBS: possível instanciar o modelo sem o tokenizer e device
    classifier = pipeline(
        task="sentiment-analysis",
        model=model,
        tokenizer=tokenizer,
        device=torch.cuda.current_device(),
    )

    t1 = time()
    logger.info(
        f"\nStarting classification: {model_name=}, {df.shape=}, {batch_size=}, {device=}"
    )
    results = df.apply(lambda x: classifier(x))
    t2 = time()
    logger.info(
        f"Finished classification in {t2-t1:.3f} seconds; {df.shape=}, {batch_size=}, {device=}"
    )

    # Separando rotulo e score
    df["label"] = [result[0]["label"] for result in results]
    df["score"] = [result[0]["score"] for result in results]

    return df


@task
def clean_outputs(df: pd.DataFrame) -> pd.DataFrame:
    df["label"] = df["label"].apply(lambda x: False if x == 0 else True)

    return df


@task()
def cleanup(file_name):
    # To Do: Apagar arquivo temporário para limpar espaço
    # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli#delete-a-container
    pass


@flow(name="Classify Tweets")
def classify_tweets(
    file_name: str = "sp_elected_stdep_tweets.csv",
    source: str = "azure",
    col_name: str = "content",
):
    flow_name = prefect.context.get_run_context().flow.name
    flow_params = prefect.context.get_run_context().flow_run.dict()["parameters"]
    logger = get_run_logger()
    logger.info(f"Starting {flow_name} flow with params: {flow_params}")

    if source == "azure":
        file_path = download_blob(file_name)
        df = pd.read_csv(file_path, sep=";")

    if source == "local":
        df = load_processed_dataset(file_name, sep=";")

    df = filter_tweets(df)
    df = classify(df, col_name=col_name)
    df = clean_outputs(df)

    if source == "azure":
        upload_blob(df, file_name)
    else:
        save_local_dataset(df, file_name)
    # cleanup(file_name)

    return df.shape


if __name__ == "__main__":
    classify_tweets(*sys.argv[1:])
