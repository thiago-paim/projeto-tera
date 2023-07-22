import sys
from time import time
import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
from src.common import load_local_dataset, save_local_dataset
from transformers import pipeline
import torch
from tqdm import tqdm

# Incomplete


@flow(name="Classify Tweets")
def get_offense_metrics(
    file_name: str = "erika-short.csv", source: str = "local", col_name: str = "content"
):
    flow_name = prefect.context.get_run_context().flow.name
    flow_params = prefect.context.get_run_context().flow_run.dict()["parameters"]
    logger = get_run_logger()
    logger.info(f"Starting {flow_name} flow with params: {flow_params}")

    df = load_local_dataset(file_name, source)

    return df.shape


if __name__ == "__main__":
    get_offense_metrics(*sys.argv[1:])
