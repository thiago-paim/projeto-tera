from mlflow import log_metric, log_param, log_artifacts
import pandas as pd
from prefect import flow, task, get_run_logger
from src.common import load_raw_dataset
from src.feature_extraction import bag_of_words
from src.filters import (
    drop_duplicated_rows,
    drop_unused_tweet_columns,
    get_tweets_by_username,
)
from src.preprocess import filter_letters, remove_stopwords, lemmatize
from src.models import topic_modelling_sentiment_analysis


@task
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # Removendo colunas pouco informativas
    df = drop_unused_tweet_columns(df)

    # Remove linhas com tweets duplicados
    df = drop_duplicated_rows(df)

    # Filtra somente tweets do usuario
    username = "https://twitter.com/ErikakHilton"
    df = get_tweets_by_username(df, username)
    return df


@task
def preprocess(tweets: pd.DataFrame) -> pd.DataFrame:
    def pre_process(text: str):
        text = filter_letters(text)
        text = remove_stopwords(text)
        return lemmatize(text)

    processed_tweets = tweets.rawContent.apply(pre_process)
    return processed_tweets


@task
def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    X = bag_of_words(df)
    return X


@task
def run_model(df: pd.DataFrame) -> pd.DataFrame:
    X = topic_modelling_sentiment_analysis(df)
    return X


@flow(name="Process Erikas Tweets")
def process_erikas_tweets():
    logger = get_run_logger()
    logger.info("Starting Process Erikas Tweets flow")

    file_name = "ErikakHilton-tweets.csv"
    log_param("file_name", file_name)
    df = load_raw_dataset(file_name)

    tweets = apply_filters(df)
    tweets = preprocess(tweets)
    X = extract_features(tweets)
    results = run_model(X)

    pos_class = results[results["classif_tmba"] == "positive"].shape[0]
    neg_class = results[results["classif_tmba"] == "negative"].shape[0]
    log_metric("pos_class", pos_class)
    log_metric("neg_class", neg_class)

    return results


if __name__ == "__main__":
    process_erikas_tweets()
