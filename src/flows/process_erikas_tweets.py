from mlflow import log_metric, log_param, log_artifacts
import pandas as pd
from prefect import flow, task, get_run_logger
from src.common import load_raw_dataset, drop_duplicated_rows
from src.feature_extraction import get_bag_of_words
from src.filters import drop_unused_tweet_columns, get_tweets_by_username
from src.preprocess import filter_letters, remove_stopwords, lemmatize


@task
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # Removendo colunas pouco informativas
    df = drop_unused_tweet_columns(df)
    
    # Remove linhas com tweets duplicados
    df = drop_duplicated_rows(df)
    
    # Filtra somente tweets do usuario
    username = 'https://twitter.com/ErikakHilton'
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
    bow = get_bag_of_words(df)
    return bow


@flow(name="Process Erikas Tweets")
def process_erikas_tweets():
    logger = get_run_logger()
    logger.info("Starting Process Erikas Tweets flow")
    
    file_name = 'ErikakHilton-2022-10-01-2022-11-01-2023-02-16T19:31:01.224464.csv'
    log_param("file_name", file_name)
    df = load_raw_dataset(file_name)
    
    tweets = apply_filters(df)
    tweets = preprocess(tweets)
    bow = get_bag_of_words(tweets)
    
    log_metric("tweets_count", len(tweets))
    return None


if __name__ == "__main__":
    process_erikas_tweets()