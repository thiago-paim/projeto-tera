import pandas as pd
from scipy.sparse._csr import csr_matrix
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


def bag_of_words(tweets: pd.Series) -> pd.Series:
    bow_vectorizer = CountVectorizer()
    X = bow_vectorizer.fit_transform(tweets)
    
    # Converte de volta para DataFrame
    X = pd.DataFrame(X.todense())
    
    # Atribuindo nomes das colunas aos termos
    vocabulary_map = {v: k for k, v in bow_vectorizer.vocabulary_.items()}
    X.columns = X.columns.map(vocabulary_map)
    return X