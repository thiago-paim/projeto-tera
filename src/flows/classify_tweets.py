import sys
from time import time
import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
from src.common import download_blob, upload_blob, load_processed_dataset, save_dataset
from transformers import pipeline
import torch
from tqdm import tqdm


@task
def classify(
    df: pd.DataFrame, col_name: str = "content", batch_size: int = 50
) -> pd.DataFrame:
    logger = get_run_logger()
    model_name = "ruanchaves/bert-large-portuguese-cased-hatebr"

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
        batch_texts = df[col_name][batch_start:batch_end].tolist()
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
def classify_tweets(
    file_name: str = "erika-short.csv", source: str = "azure", col_name: str = "content"
):
    flow_name = prefect.context.get_run_context().flow.name
    flow_params = prefect.context.get_run_context().flow_run.dict()["parameters"]
    logger = get_run_logger()
    logger.info(f"Starting {flow_name} flow with params: {flow_params}")

    if source == "azure":
        file_path = download_blob(file_name)
        df = pd.read_csv(file_path, sep=";")

    if source == "local":
        df = load_processed_dataset(file_name, sep=",")

    df = classify(df, col_name=col_name)

    if source == "azure":
        upload_blob(df, file_name)
    else:
        save_dataset(df, file_name)
    # cleanup(file_name)

    return df.shape


if __name__ == "__main__":
    classify_tweets(*sys.argv[1:])
