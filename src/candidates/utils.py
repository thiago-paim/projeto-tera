import pandas as pd


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