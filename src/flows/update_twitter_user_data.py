from prefect import flow, get_run_logger
from src.common import load_local_dataset, save_local_dataset
from src.feature_extraction import get_twitter_user_data
import sys


@flow(name="Update Twitter User data")
def update_twitter_user_data(file_path: str):
    logger = get_run_logger()
    logger.info(f"Starting Update Twitter User data flow for file_path={file_path}")

    df = load_local_dataset(file_path)
    usernames = list(df["TW_USER"])  # Filtrar somente os que não tem os campos
    logger.info(f"{len(usernames)} Twitter users to be updated")

    user_data = get_twitter_user_data(usernames)

    # Quantidade de seguidores da conta
    df["TW_followersCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["followersCount"] if x else None
    )

    # Quantidade de usuarios que a conta segue
    df["TW_friendsCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["friendsCount"] if x else None
    )

    # Quantidade de tweets postados pela conta
    df["TW_statusesCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["statusesCount"] if x else None
    )

    # Quantidade de tweets curtidos pela conta
    df["TW_favouritesCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["favouritesCount"] if x else None
    )

    # Quantidade de listas em que a conta está
    df["TW_listedCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["listedCount"] if x else None
    )

    # Quantidade de mídias postadas pela conta
    df["TW_mediaCount"] = df.TW_USER.apply(
        lambda x: user_data[x]["mediaCount"] if x else None
    )

    # Salva dataset processado
    file_name = file_path.split("/")[-1]
    logger.info(f"Saving results on {file_name}")
    save_local_dataset(df, file_name)

    return df.shape


if __name__ == "__main__":
    update_twitter_user_data(file_path=sys.argv[1])
