import streamlit as st
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from snscrape.modules.twitter import TwitterProfileScraper

import seaborn as sns


pd.options.display.max_colwidth = 50
sns.set_style("whitegrid")


candidates_file_path = "data/processed/se_candidates_output_3.csv"
raw_cand_df = pd.read_csv(candidates_file_path, sep=";", encoding="utf-8")


st.title("Análise de Dados")

st.text_input("escreva o @", key="username")

if st.session_state.username:
    username = st.session_state.username

    tweet_scrapper = TwitterProfileScraper(username).get_items()
    print(tweet_scrapper)

    for i, tweet in enumerate(tweet_scrapper):
        print(i, tweet)
        if i >= 10:
            break
