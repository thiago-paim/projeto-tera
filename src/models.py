import pandas as pd
from LeIA import SentimentIntensityAnalyzer as LeIASentimentIntensityAnalyzer
from sklearn.decomposition import LatentDirichletAllocation


def topic_modelling_sentiment_analysis(X: pd.DataFrame) -> pd.DataFrame:
    # Treinar LDA para identificar topicos/temas
    n_topics = 2  # numero de topicos
    lda = LatentDirichletAllocation(n_components=n_topics)
    lda.fit(X)

    # Manualmente atribuir rotulos para cada topico
    topic_sentiments = ["positive", "negative"]
    topic_labels = [topic_sentiments[i] for i in lda.transform(X).argmax(axis=1)]

    # Rotular tweets baseado em topicos
    X["classif_tmba"] = topic_labels

    return X


def leia_sentiment_analysis(s: pd.Series) -> pd.Series:
    analyzer = LeIASentimentIntensityAnalyzer()
    polarity_scores = s.apply(analyzer.polarity_scores)
    return polarity_scores.apply(lambda score: score["compound"])
