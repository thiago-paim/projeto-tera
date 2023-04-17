import os
import ast
import re
import pandas as pd
from scipy.sparse._csr import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer


def get_twitter_username_by_url(url: str) -> str:
    """Extrai o username do Twitter a partir da URL"""
    if url and isinstance(url, str):
        url = url.rstrip("/")
        username = url.split("/")[-1]
        username = username.split("?")[0]
        username = re.findall(r"\b[0-9A-zÀ-úü]+\b", username.lower())
        return "".join(username)
    return None


def get_twitter_usernames(
    df: pd.DataFrame, url_col: str = "DS_URL", username_col: str = "TW_USER"
) -> pd.DataFrame:
    """Adiciona uma nova coluna ao DataFrame com os usernames do Twitter"""
    df[username_col] = df[url_col].apply(get_twitter_username_by_url)
    return df


def tokenize(text):
    return [word for word in text.split()]


def bag_of_words(tweets: pd.Series) -> pd.Series:
    bow_vectorizer = CountVectorizer()
    X = bow_vectorizer.fit_transform(tweets)

    # Converte de volta para DataFrame
    X = pd.DataFrame(X.todense())

    # Atribuindo nomes das colunas aos termos
    vocabulary_map = {v: k for k, v in bow_vectorizer.vocabulary_.items()}
    X.columns = X.columns.map(vocabulary_map)
    return X
