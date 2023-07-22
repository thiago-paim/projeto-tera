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
from transformers import pipeline
import torch
from tqdm import tqdm


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
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    num_batches = len(df) // batch_size + 1
    results = {}
    for model_key, model_name in OFFENSE_MODELS.items():
        if device.type == "cuda":
            classifier = pipeline("sentiment-analysis", model=model_name, device=0)
        else:
            classifier = pipeline("sentiment-analysis", model=model_name)

        results[model_key] = []
        t1 = time()
        logger.info(
            f"\nStarting classification: {model_name=}, {df.shape=}, {batch_size=}, {num_batches=}, {device=}"
        )
        for i in tqdm(range(num_batches)):
            batch_start = i * batch_size
            batch_end = min((i + 1) * batch_size, len(df))
            batch_texts = df[col_name][batch_start:batch_end].tolist()
            batch_results = classifier(batch_texts)
            results[model_key] += batch_results

        t2 = time()
        logger.info(
            f"Finished classification in {t2-t1:.3f} seconds; {df.shape=}, {batch_size=}, {num_batches=}, {device=}"
        )

    for key in OFFENSE_MODELS.keys():
        print(key)
        df[f"{key}_label"] = [result["label"] for result in results[key]]
        df[f"{key}_score"] = [result["score"] for result in results[key]]

    return df


@task
def clean_outputs(df: pd.DataFrame) -> pd.DataFrame:
    def get_distilbert_label(label):
        if label == "toxic":
            return True
        return False

    if "ci_distilbert_base_label" in df.columns:
        df["ci_distilbert_base_label"] = df["ci_distilbert_base_label"].apply(
            get_distilbert_label
        )

    return df


@task
def get_label_sum(df: pd.DataFrame) -> pd.DataFrame:
    def get_offense_label_sum(row):
        count = 0
        if row["ru_bert_base_label"] == True:
            count += 1
        if row["ru_bert_large_label"] == True:
            count += 1
        if row["ru_mdeberta_base_label"] == True:
            count += 1
        if row["ci_distilbert_base_label"] == True:
            count += 1
        return count

    df["label_sum"] = df.apply(get_offense_label_sum, axis=1)
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
    df = get_label_sum(df)

    if source == "azure":
        upload_blob(df, file_name)
    else:
        save_local_dataset(df, file_name)
    # cleanup(file_name)

    return df.shape


if __name__ == "__main__":
    classify_tweets(*sys.argv[1:])


"""To Do:
- Criar um dicionario de códigos por modelo, para facilitar a visualização
- Adicionar todos os modelos no `classify`, em colunas novas
- Otimizar execução em GPU
"""
