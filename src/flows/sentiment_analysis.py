import sys
from mlflow import log_metric, log_param, log_artifacts
import pandas as pd
from prefect import flow, task, get_run_logger
from src.common import load_dataset
from src.preprocess import filter_letters, remove_stopwords, lemmatize
from src.models import leia_sentiment_analysis


@task
def preprocess(tweets: pd.Series) -> pd.Series:
    def pre_process(text: str):
        text = filter_letters(text)
        text = remove_stopwords(text)
        return lemmatize(text)

    processed_tweets = tweets.apply(pre_process)
    return processed_tweets


@flow(name="Sentiment Analysis")
def sentiment_analysis(file_path: str):
    logger = get_run_logger()
    logger.info(f"Starting Sentiment Analysis flow with file_path: {file_path}")

    df = load_dataset(file_path)

    df["cleanContent"] = preprocess(df["rawContent"])

    df["leia_sentiment"] = leia_sentiment_analysis(df["cleanContent"])

    print(df.head())

    # Incompleto


if __name__ == "__main__":
    sentiment_analysis(file_path=sys.argv[1])
