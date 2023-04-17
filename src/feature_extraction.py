import os
import ast
import re
import openai
import pandas as pd
from scipy.sparse._csr import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer

from src.config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY


def get_chatgpt_sentiment_analysis(text: str) -> dict:
    base_msg = """
        Você irá realizar uma análise de sentimentos em um tweet que irei mandar.
        Sua resposta deve vir como um dicionario Python de três chaves: 'sentiment', 'value' e 'comment', com o seguinte significado:
        - sentiment: Positivo, Neutro ou Negativo
        - value: Um valor de -1 (Negativo) a 1 (Positivo)
        - comment: Uma frase explicando o motivo da sua classificação
        Por exemplo: {
            'sentiment': 'Positivo',
            'value': 0.9,
            'comment': 'O tweet expressa felicidade devido a uma boa notícia'
        }
        
        O tweet é:
    """
    msg = base_msg + text
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": msg}],
        temperature=0,
    )
    response_content = response["choices"][0]["message"]["content"]
    return ast.literal_eval(response_content)


# def get_chatgpt_sentiment_analysis_column(
#     df: pd.DataFrame,
#     text_column: str = "content",
#     new_columns: list = ["gpt_class", "gpt_value", "gpt_comment"],
#     limit: int = 10,
#     tweets_per_request: int = 5,
# ) -> pd.DataFrame:
#     """Adiciona uma nova coluna ao DataFrame com a análise de sentimento do ChatGPT"""

#     base_msg = """
#         Você irá realizar análise de sentimentos em uma série de tweets que mandarei.
#         Para cada tweet voce deve gerar um dicionario Python de três chaves: 'sentiment', 'value' e 'comment', com o seguinte significado:
#         - sentiment: Positivo, Neutro ou Negativo
#         - value: Um valor de -1 (Negativo) a 1 (Positivo)
#         - comment: Uma frase explicando o motivo da sua classificação
#         Por exemplo: {
#             'sentiment': 'Positivo',
#             'value': 0.9,
#             'comment': 'O tweet expressa felicidade devido a uma boa notícia'
#         }
#         A sua resposta deve vir no formato de uma lista com os dicionários gerados.
#         Seguem abaixo os tweets:

#     """

#     response = openai.ChatCompletion.create(
#         model="gpt-3.5-turbo",
#         messages=[{"role": "user", "content": base_msg}],
#         temperature=0,
#     )
#     response_content = response["choices"][0]["message"]["content"]
#     print(response_content)


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
