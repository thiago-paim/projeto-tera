import sys
import pandas as pd
from prefect import flow, task, get_run_logger
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import snscrape.modules.twitter as sntwitter
from src.common import save_dataset, load_dataset
from src.filters import drop_nans, drop_duplicate_rows
from src.feature_extraction import get_twitter_usernames, get_twitter_username_by_url


@flow(name="Process Candidates data")
def sample_dataset(file_path: str, sample_size: int = 100):
    logger = get_run_logger()
    logger.info("Starting Sample Dataset flow")

    df = load_dataset(file_path)

    df = df[:sample_size]

    file_name = file_path.split("/")[-1]
    file_name = f"sample-{sample_size}-{file_name}"
    save_dataset(df, file_name)

    return df.shape


if __name__ == "__main__":
    print(sys.argv)
    sample_dataset(file_path=sys.argv[1])
