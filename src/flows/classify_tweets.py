import sys
import pandas as pd
import prefect
from prefect import flow, task, get_run_logger
from src.common import save_dataset, load_dataset, download_blob, upload_blob
from transformers import pipeline


@task
def classify(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    model_name = "ruanchaves/bert-large-portuguese-cased-hatebr"
    col = "rawContent"

    # To Do: Rodar em batches
    classifier = pipeline("sentiment-analysis", model=model_name)
    results = df[col].apply(classifier)

    df["class_label"] = results.apply(lambda x: x[0]["label"])
    df["class_score"] = results.apply(lambda x: x[0]["score"])

    return df


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

    return df.shape


if __name__ == "__main__":
    classify_tweets(file_name=sys.argv[1])
