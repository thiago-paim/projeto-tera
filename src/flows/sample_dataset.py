import sys
import pandas as pd
from prefect import flow, task, get_run_logger
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import snscrape.modules.twitter as sntwitter
from src.common import save_dataset, load_dataset
from src.filters import drop_nans, drop_duplicate_rows
from src.feature_extraction import get_twitter_usernames, get_twitter_username_by_url


@task
def get_sample(df: pd.DataFrame, sample_size: int = 100) -> pd.DataFrame:
    sample_df = df[:sample_size]
    return sample_df


@flow(name="Process Candidates data")
def sample_dataset(file_path: str):
    SAMPLE_SIZE = 100
    logger = get_run_logger()
    logger.info(
        f"Starting Sample Dataset flow with file_path: {file_path}, sample_size: {SAMPLE_SIZE}"
    )

    df = load_dataset(file_path)
    df = get_sample(df, sample_size=SAMPLE_SIZE)

    file_name = file_path.split("/")[-1]
    file_name = f"sample-{SAMPLE_SIZE}-{file_name}"
    save_dataset(df, file_name)

    return df.shape


if __name__ == "__main__":
    sample_dataset(file_path=sys.argv[1])
