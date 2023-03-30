import pandas as pd
from prefect import task, get_run_logger
import snscrape.modules.twitter as sntwitter
from utils import load_raw_dataset, save_dataset
from config import PROCESSED_DATASETS_PATH, RAW_DATASETS_PATH


def drop_duplicated_candidates(df: pd.DataFrame) -> pd.DataFrame:
    # Lista obtida a partir da checagem manual das contas no Twitter
    drop_list = [
        180, 181, 579, 886, 2344, 2688, 2784, 3442, 3443        
    ]
    # Removendo linhas para que cada candidato do dataset possua somente uma conta no Twitter
    df = df.drop(drop_list)
    return df


def get_twitter_username(link):
    """Extrai o username do Twitter a partir da URL"""
    if link and isinstance(link, str):
        link = link.rstrip('/')
        username = link.split('/')[-1]
        username = username.split('?')[0]
        return username.lower()
    return None


@task
def process_candidates_dataset() -> pd.DataFrame:
    """
    Carrega os dados dos candidatos e suas redes sociais (obtidos no TSE), 
    mescla em um único dataset, filtra somente candidatos a deputado estadual e
    remove linhas de candidatos repetidos.
    O resultado final é salvo em um arquivo CSV.
    """
    # Carregando os datasets do TSE
    raw_candidates_dataset_name = 'consulta_cand_2022_SP.csv'
    raw_social_networks_dataset_name = 'rede_social_candidato_2022_SP.csv'
    cand_df = load_raw_dataset(raw_candidates_dataset_name)
    rs_df = load_raw_dataset(raw_social_networks_dataset_name)
 
    # Filtrando somente linhas cuja rede social seja o Twitter
    rs_twitter_df = rs_df[
        rs_df.DS_URL.str.contains('twitter', case=False)
    ]
    
    # Mesclando os datasets em um só
    df = pd.merge(
        cand_df, rs_twitter_df[['SQ_CANDIDATO', 'DS_URL']], 
        on="SQ_CANDIDATO", how="left"
    )
    
    # Filtrando somente candidatos a deputado estadual
    candidates_df = df.loc[
        (df['DS_CARGO'] == 'DEPUTADO ESTADUAL')
    ]
    
    # Remove linhas de candidatos repetidos
    candidates_df = drop_duplicated_candidates(candidates_df)
    
    # Extrai o username do Twitter a partir da URL        
    candidates_df['TW_USER'] = candidates_df.DS_URL.apply(get_twitter_username)
    
    # Salva dataset processado
    file_name = "candidates_output_1.csv"
    save_dataset(df, file_name)
    
    return candidates_df


@task
def scrape_twitter_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o scraping dos dados das contas Twitter dos candidatos, e adiciona no DataFrame.
    """
    logger = get_run_logger()
    usernames = list(df['TW_USER'])
    logger.info(f'Scrapping Twitter data for {len(usernames)} users')
    
    user_data = {}
    for i, username in enumerate(usernames):
        if not username:
            continue
        try:
            last_tweet = next(sntwitter.TwitterProfileScraper(username).get_items())
            user_data[username] = {
                'followersCount': last_tweet.user.followersCount,
                'friendsCount': last_tweet.user.friendsCount,
                'statusesCount': last_tweet.user.statusesCount,
                'favouritesCount': last_tweet.user.favouritesCount,
                'listedCount': last_tweet.user.listedCount,
                'mediaCount': last_tweet.user.mediaCount,
            }
            print(f'{i+1}/{len(usernames)} {username}: {user_data[username]}')
        except Exception as e:
            print(f'{i+1}/{len(usernames)} {username}: Erro {e}')
            user_data[username] = {
                'followersCount': 0,
                'friendsCount': 0,
                'statusesCount': 0,
                'favouritesCount': 0,
                'listedCount': 0,
                'mediaCount': 0,
            }
    
    # Quantidade de seguidores da conta
    df['TW_followersCount'] = df.TW_USER.apply(lambda x: user_data[x]['followersCount'] if x else None)

    # Quantidade de usuarios que a conta segue
    df['TW_friendsCount'] = df.TW_USER.apply(lambda x: user_data[x]['friendsCount'] if x else None)

    # Quantidade de tweets postados pela conta
    df['TW_statusesCount'] = df.TW_USER.apply(lambda x: user_data[x]['statusesCount'] if x else None)

    # Quantidade de tweets curtidos pela conta
    df['TW_favouritesCount'] = df.TW_USER.apply(lambda x: user_data[x]['favouritesCount'] if x else None)

    # Quantidade de listas em que a conta está
    df['TW_listedCount'] = df.TW_USER.apply(lambda x: user_data[x]['listedCount'] if x else None)
    
    # Quantidade de mídias postadas pela conta
    df['TW_mediaCount'] = df.TW_USER.apply(lambda x: user_data[x]['mediaCount'] if x else None)
    
    # Salva dataset processado
    file_name = "candidates_output_2.csv"
    save_dataset(df, file_name)
    
    return df


@task
def scrape_tweets_count(df: pd.DataFrame, since: str='2022-09-01', until: str='2022-11-01') -> pd.DataFrame:
    """
    Realiza o scraping dos tweets dos candidatos no período solicitado, e adiciona no DataFrame.
    """
    logger = get_run_logger()
    usernames = list(df['TW_USER'])
    logger.info(f'Scrapping tweets count for {len(usernames)} users')
    
    user_tweets = {}
    for i, username in enumerate(usernames):
        if not username:
            continue
        try:
            query = f'from:{username} since:{since} until:{until}'
            user_scrapping_results = sntwitter.TwitterSearchScraper(query).get_items()
            tweets = []
            for tweet in user_scrapping_results:
                tweets.append(tweet)

            user_tweets[username] = {
                'posts': tweets,
                'count': len(tweets),
            }
            print(f'{i+1}/{len(usernames)} {username}: {len(tweets)} tweets')
        except Exception as e:
            print(f'{i+1}/{len(usernames)} {username}: Erro {e}')
            user_tweets[username] = {
                'posts': [],
                'count': 0,
            }
    
    # Quantidade de tweets postados no período
    df['TW_electionTweets'] = df.TW_USER.apply(lambda x: user_tweets[x]['count'] if x else None)
    
    # Salva dataset processado
    file_name = "candidates_output_3.csv"
    save_dataset(df, file_name)
    
    return df