import pandas as pd
from src.config import PROCESSED_DATASETS_PATH, RAW_DATASETS_PATH


def load_raw_dataset(file_name: str, sep: str=';') -> pd.DataFrame:
    df = pd.read_csv(f"{RAW_DATASETS_PATH}/{file_name}", sep=sep)
    return df


def load_processed_dataset(file_name: str, sep: str=';') -> pd.DataFrame:
    df = pd.read_csv(f"{PROCESSED_DATASETS_PATH}/{file_name}", sep=sep)
    return df


def save_dataset(df: pd.DataFrame, file_name: str, sep: str=';') -> None:
    df.to_csv(f"{PROCESSED_DATASETS_PATH}/{file_name}", sep=sep, index=False)


def drop_duplicated_rows(df: pd.DataFrame) -> pd.DataFrame:
    duplicated_indexes = df[df.duplicated()].index
    df = df.drop(duplicated_indexes)
    return df