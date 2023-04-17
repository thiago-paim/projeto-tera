import pandas as pd
from prefect import flow, task, get_run_logger
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import snscrape.modules.twitter as sntwitter
from src.common import load_raw_dataset, save_dataset
from src.filters import drop_nans, drop_duplicate_rows
from src.feature_extraction import get_twitter_usernames, get_twitter_username_by_url


@task
def merge_candidates_datasets() -> pd.DataFrame:
    # Carregando os datasets do TSE
    raw_candidates_dataset_name = "consulta_cand_2022_BRASIL.csv"
    raw_socnet_es_dataset_name = "rede_social_candidato_2022_ES.csv"
    raw_socnet_mg_dataset_name = "rede_social_candidato_2022_MG.csv"
    raw_socnet_rj_dataset_name = "rede_social_candidato_2022_RJ.csv"
    raw_socnet_sp_dataset_name = "rede_social_candidato_2022_SP.csv"
    cand_df = load_raw_dataset(raw_candidates_dataset_name)
    socnet_es_df = load_raw_dataset(raw_socnet_es_dataset_name)
    socnet_mg_df = load_raw_dataset(raw_socnet_mg_dataset_name)
    socnet_rj_df = load_raw_dataset(raw_socnet_rj_dataset_name)
    socnet_sp_df = load_raw_dataset(raw_socnet_sp_dataset_name)

    # Filtrando somente linhas cuja rede social seja o Twitter
    socnet_es_df = socnet_es_df[socnet_es_df.DS_URL.str.contains("twitter", case=False)]
    socnet_mg_df = socnet_mg_df[socnet_mg_df.DS_URL.str.contains("twitter", case=False)]
    socnet_rj_df = socnet_rj_df[socnet_rj_df.DS_URL.str.contains("twitter", case=False)]
    socnet_sp_df = socnet_sp_df[socnet_sp_df.DS_URL.str.contains("twitter", case=False)]

    # Mesclando os datasets em um só
    socnet_df = pd.concat([socnet_es_df, socnet_mg_df, socnet_rj_df, socnet_sp_df])
    df = pd.merge(
        cand_df,
        socnet_df[["SQ_CANDIDATO", "DS_URL"]],
        on="SQ_CANDIDATO",
        how="left",
    )

    # Salva dataset processado
    file_name = "se_candidates_output_1.csv"
    save_dataset(df, file_name)

    return df


@task
def apply_pipeline(df: pd.DataFrame, pipeline: Pipeline) -> pd.DataFrame:
    df = pipeline.fit_transform(df)

    # Salva dataset processado
    file_name = "se_candidates_output_2.csv"
    save_dataset(df, file_name)

    return df


@task
def scrape_twitter_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o scraping dos dados das contas Twitter dos candidatos, e adiciona no DataFrame.
    """
    logger = get_run_logger()
    usernames = list(df["TW_USER"])
    logger.info(f"Scrapping Twitter data for {len(usernames)} users")

    user_data = {}
    for i, username in enumerate(usernames):
        if not username:
            continue
        try:
            last_tweet = next(sntwitter.TwitterProfileScraper(username).get_items())
            user_data[username] = {
                "followersCount": last_tweet.user.followersCount,
                "friendsCount": last_tweet.user.friendsCount,
                "statusesCount": last_tweet.user.statusesCount,
                "favouritesCount": last_tweet.user.favouritesCount,
                "listedCount": last_tweet.user.listedCount,
                "mediaCount": last_tweet.user.mediaCount,
            }
            print(f"{i+1}/{len(usernames)} {username}: {user_data[username]}")
        except Exception as e:
            print(f"{i+1}/{len(usernames)} {username}: Erro {e}")
            user_data[username] = {
                "followersCount": 0,
                "friendsCount": 0,
                "statusesCount": 0,
                "favouritesCount": 0,
                "listedCount": 0,
                "mediaCount": 0,
            }

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
    file_name = "se_candidates_output_3.csv"
    save_dataset(df, file_name)

    return df


@task
def scrape_tweets_count(
    df: pd.DataFrame, since: str = "2022-09-01", until: str = "2022-11-01"
) -> pd.DataFrame:
    """
    Realiza o scraping dos tweets dos candidatos no período solicitado, e adiciona no DataFrame.
    """
    logger = get_run_logger()
    usernames = list(df["TW_USER"])
    logger.info(f"Scrapping tweets count for {len(usernames)} users")

    user_tweets = {}
    for i, username in enumerate(usernames):
        if not username:
            continue
        try:
            query = f"from:{username} since:{since} until:{until}"
            user_scrapping_results = sntwitter.TwitterSearchScraper(query).get_items()
            tweets = []
            for tweet in user_scrapping_results:
                tweets.append(tweet)

            user_tweets[username] = {
                "posts": tweets,
                "count": len(tweets),
            }
            print(f"{i+1}/{len(usernames)} {username}: {len(tweets)} tweets")
        except Exception as e:
            print(f"{i+1}/{len(usernames)} {username}: Erro {e}")
            user_tweets[username] = {
                "posts": [],
                "count": 0,
            }

    # Quantidade de tweets postados no período
    df["TW_electionTweets"] = df.TW_USER.apply(
        lambda x: user_tweets[x]["count"] if x else None
    )

    # Salva dataset processado
    file_name = "se_candidates_output_4.csv"
    save_dataset(df, file_name)

    return df


@flow(name="Process Candidates data")
def process_southeast_candidates_data():
    """Pipeline de processamento dos dados dos candidatos
    - Carrega os arquivos do TSE com dados dos candidatos e suas redes sociais, e junta ambos em um só,
    - Filtra os candidatos a deputado estadual que possuem conta no Twitter
    - Realiza um scrapping no Twitter para obter informações sobre as contas dos deputados, e também a quantidade de tweets postados no período da eleição

    Uma nova versão do dataset é salvo a cada etapa, com nome "candidates_output_N".
    """
    logger = get_run_logger()
    logger.info("Starting Process Southeast Candidates data flow")

    df = merge_candidates_datasets()

    pipeline = Pipeline(
        [
            (
                "get_twitter_usernames",
                FunctionTransformer(get_twitter_usernames),
            ),
            (
                "drop_nans",
                FunctionTransformer(drop_nans, kw_args={"subset": "DS_URL"}),
            ),
            (
                "drop_duplicate_rows",
                FunctionTransformer(drop_duplicate_rows, kw_args={"subset": "DS_URL"}),
            ),
        ]
    )

    df = apply_pipeline(df, pipeline)

    df = scrape_twitter_data(df)
    df = scrape_tweets_count(df)
    return df.shape


if __name__ == "__main__":
    process_southeast_candidates_data()
