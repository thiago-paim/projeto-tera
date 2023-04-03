import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def get_twitter_username(link):
    """Extrai o username do Twitter a partir da URL"""
    if link and isinstance(link, str):
        link = link.rstrip('/')
        username = link.split('/')[-1]
        username = username.split('?')[0]
        return username.lower()
    return None


def tokenize(text):
    return [word for word in text.split()]


def get_bag_of_words(tweets: pd.Series) -> pd.Series:
    bow_vectorizer = CountVectorizer()
    bow = bow_vectorizer.fit_transform(tweets)
    bow = pd.DataFrame(bow.todense())

    # Atribuindo nomes das colunas aos termos
    vocabulary_map = {v: k for k, v in bow_vectorizer.vocabulary_.items()}
    bow.columns = bow.columns.map(vocabulary_map)
    return bow