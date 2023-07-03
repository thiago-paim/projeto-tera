from prefect import flow, get_run_logger
from src.common import load_local_dataset, save_local_dataset
from src.feature_extraction import get_tweets_count
import sys


@flow(name="Update Tweets count")
def update_tweets_count(file_path: str):
    logger = get_run_logger()
    logger.info(f"Starting Update Tweets count flow for file_path={file_path}")

    df = load_local_dataset(file_path)
    usernames = list(df["TW_USER"])  # Filtrar somente os que não tem os campos
    logger.info(f"{len(usernames)} Tweet counts to be updated")

    user_tweets = get_tweets_count(usernames)

    # Quantidade de tweets postados no período
    df["TW_electionTweets"] = df.TW_USER.apply(
        lambda x: user_tweets[x]["count"] if x else None
    )

    # Salva dataset processado
    file_name = file_path.split("/")[-1]
    logger.info(f"Saving results on {file_name}")
    save_local_dataset(df, file_name)


if __name__ == "__main__":
    update_tweets_count(file_path=sys.argv[1])
