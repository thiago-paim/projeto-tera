import nltk
import re
import spacy

nltk.download("stopwords")
spc_pt = spacy.load("pt_core_news_sm")


def filter_letters(text: str) -> list:
    # Remove non-letter characters and convert to lower case
    letters = re.findall(r"\b[A-zÀ-úü://.]+\b", text.lower())
    return letters


def remove_stopwords(text: list) -> str:
    # Fazendo ajustes nas stopwords padrão do NLTK
    stopwords = nltk.corpus.stopwords.words("portuguese")
    stopwords.remove("não")
    extra_stopwords = [
        "'",
        "pra",
        "tá",
        "/",
        "https",
        "t.co/",
        "https://t.co/",
    ]
    stopwords += extra_stopwords

    # Removendo as stopwords do texto
    meaningful_words = [w for w in text if w not in stopwords]
    meaningful_words_text = " ".join(meaningful_words)
    return meaningful_words_text


def lemmatize(text):
    spc_letters = spc_pt(text)
    tokens = [
        token.lemma_ if token.pos_ == "VERB" else str(token) for token in spc_letters
    ]

    # Tratamento especial para o verbo "ir"
    ir = ["vou", "vais", "vai", "vamos", "ides", "vão"]
    tokens = ["ir" if token in ir else str(token) for token in tokens]
    return " ".join(tokens)
