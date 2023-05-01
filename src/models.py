import os
import pandas as pd
from LeIA import (
    SentiText,
    SentimentIntensityAnalyzer,
    PACKAGE_DIRECTORY,
    BOOSTER_DICT,
    NEGATE,
)
from sklearn.decomposition import LatentDirichletAllocation


ADVERSATIVE = ["mas", "entretanto", "todavia", "porem", "porém"]
MODIFIERS = list(BOOSTER_DICT.keys()) + NEGATE + ADVERSATIVE


class CustomSentiText(SentiText):
    ...


class CustomSentimentIntensityAnalyzer(SentimentIntensityAnalyzer):
    def __init__(
        self,
        lexicon_file=os.path.join(
            PACKAGE_DIRECTORY, "lexicons", "vader_lexicon_ptbr.txt"
        ),
        emoji_lexicon=os.path.join(
            PACKAGE_DIRECTORY, "lexicons", "emoji_utf8_lexicon_ptbr.txt"
        ),
    ):
        self.used_lexicon = {}
        self.unknown_words = {}
        self.used_emojis = {}
        self.used_modifiers = {}
        super().__init__(lexicon_file, emoji_lexicon)

    def verify_token(self, token):
        if token in self.lexicon:
            if token not in self.used_lexicon:
                self.used_lexicon[token] = 0
            self.used_lexicon[token] += 1

        elif token in self.emojis:
            if token not in self.used_emojis:
                self.used_emojis[token] = 0
            self.used_emojis[token] += 1

        elif token in MODIFIERS:
            if token not in self.used_modifiers:
                self.used_modifiers[token] = 0
            self.used_modifiers[token] += 1

        else:
            if token not in self.unknown_words:
                self.unknown_words[token] = 0
            self.unknown_words[token] += 1

    def sentiment_valence(self, valence, sentitext, item, i, sentiments):
        self.verify_token(item.lower())
        return super().sentiment_valence(valence, sentitext, item, i, sentiments)

    def used_lexicon_count(self):
        return sum(self.used_lexicon.values())

    def used_emojis_count(self):
        return sum(self.used_emojis.values())

    def used_modifiers_count(self):
        return sum(self.used_modifiers.values())

    def unknown_words_count(self):
        return sum(self.unknown_words.values())

    def known_words_proportion(self):
        lex_count = self.used_lexicon_count()
        emo_count = self.used_emojis_count()
        mod_count = self.used_modifiers_count()
        unk_count = self.unknown_words_count()

        return (lex_count + emo_count + mod_count) / (
            lex_count + emo_count + mod_count + unk_count
        )


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
    analyzer = SentimentIntensityAnalyzer()
    polarity_scores = s.apply(analyzer.polarity_scores)
    return polarity_scores.apply(lambda score: score["compound"])
