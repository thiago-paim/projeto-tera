import sys
from time import time
import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
from src.common import download_blob, upload_blob
from transformers import pipeline
import torch
from tqdm import tqdm


@task
def classify(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    logger = get_run_logger()
    model_name = "ruanchaves/bert-large-portuguese-cased-hatebr"
    col = "rawContent"

    tqdm.pandas()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    if device.type == "cuda":
        classifier = pipeline("sentiment-analysis", model=model_name, device=0)
    else:
        classifier = pipeline("sentiment-analysis", model=model_name)

    t1 = time()
    num_batches = len(df) // batch_size + 1
    logger.info(
        f"\nStarting classification: {df.shape=}, {batch_size=}, {num_batches=}, {device=}"
    )

    results = []
    for i in tqdm(range(num_batches)):
        batch_start = i * batch_size
        batch_end = min((i + 1) * batch_size, len(df))
        batch_texts = df[col][batch_start:batch_end].tolist()
        batch_results = classifier(batch_texts)
        results += batch_results

    t2 = time()
    logger.info(
        f"Finished classification in {t2-t1:.3f} seconds; {df.shape=}, {batch_size=}, {num_batches=}, {device=}"
    )

    df["class_label"] = [result["label"] for result in results]
    df["class_score"] = [result["score"] for result in results]

    return df


@task()
def cleanup(file_name):
    # To Do: Apagar arquivo temporário para limpar espaço
    # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli#delete-a-container
    pass


@flow(name="Classify Tweets")
def classify_tweets(file_name: str = "erika-short.csv"):
    flow_name = prefect.context.get_run_context().flow.name
    flow_params = prefect.context.get_run_context().flow_run.dict()["parameters"]
    logger = get_run_logger()
    logger.info(f"Starting {flow_name} flow with params: {flow_params}")

    file_path = download_blob(file_name)
    df = pd.read_csv(file_path, sep=";")
    df = classify(df)
    upload_blob(df, file_name)
    # cleanup(file_name)

    return df.shape


if __name__ == "__main__":
    if len(sys.argv) > 1:
        classify_tweets(file_name=sys.argv[1])
    else:
        classify_tweets()
