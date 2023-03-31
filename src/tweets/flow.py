import pandas as pd
from prefect import flow, task, get_run_logger
from ..common import load_raw_dataset, save_dataset, drop_duplicated_rows
from .utils import drop_unused_tweet_columns, filter_letters, remove_stopwords, lemmatize, tokenize


@task
def load_tweets_dataset(file_name: str) -> pd.DataFrame:
    # Carregando os datasets do TSE
    tweets_dataset_name = file_name
    df = load_raw_dataset(tweets_dataset_name)
    
    # Removendo colunas pouco informativas
    df = drop_unused_tweet_columns(df)
    
    # Remove linhas com tweets duplicados
    df = drop_duplicated_rows(df)
    
    return df


@task
def get_tweets_by_username(df: pd.DataFrame, username: str) -> pd.Series:
    tweets = df[df.user == username].rawContent
    return tweets

 
@task
def pre_process_tweets(tweets: pd.Series) -> pd.Series:
    pass


@flow(name="Process Erikas Tweets")
def process_erikas_tweets():
    logger = get_run_logger()
    logger.info("Starting Process Erikas Tweets flow")
    
    file_name = 'ErikakHilton-2022-10-01-2022-11-01-2023-02-16T19:31:01.224464.csv'
    username = 'https://twitter.com/ErikakHilton'
    
    df = load_tweets_dataset(file_name)
    tweets = get_tweets_by_username(df, username)
    
    return len(tweets)


if __name__ == "__main__":
    process_erikas_tweets()