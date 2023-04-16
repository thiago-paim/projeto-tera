import pandas as pd


def drop_nans(X, y=None, subset=None):
    return X.dropna(subset=subset)


def drop_duplicate_rows(X, y=None, subset=None):
    duplicate_rows = X[X.duplicated(subset=subset)].index
    return X.drop(duplicate_rows)


def drop_duplicated_candidates(df: pd.DataFrame) -> pd.DataFrame:
    # Lista obtida a partir da checagem manual das contas no Twitter
    drop_list = [180, 181, 579, 886, 2344, 2688, 2784, 3442, 3443]
    # Removendo linhas para que cada candidato do dataset possua somente uma conta no Twitter
    df = df.drop(drop_list)
    return df


def drop_duplicated_rows(df: pd.DataFrame) -> pd.DataFrame:
    duplicated_indexes = df[df.duplicated()].index
    df = df.drop(duplicated_indexes)
    return df


def drop_unused_tweet_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_columns = [
        "renderedContent",  # Possui praticamente o mesmo que a rawContent
        "source",
        "sourceUrl",
        "sourceLabel",
        "links",
        "retweetedTweet",  # Muitos nulos
        "quotedTweet",
        "coordinates",
        "place",
        "cashtags",
        "card",  # Muitos nulos
        "viewCount",
        "vibe",
        "user_descriptionLinks",
        "user_label",  # Muitos nulos
    ]
    df = df.drop(drop_columns, axis=1)
    return df


def get_tweets_by_username(df: pd.DataFrame, username: str) -> pd.Series:
    tweets = df[df.user == username]
    return tweets
