# Métricas proporcionais de engajamento

Arquivo de referência rápida paras as métricas calculadas por candidato

```python
"""Média de tweets do usuario por threads do usuário. Indica o quanto o usuario interage com outros tweets dentro de suas threads"""
cand_df['TW_avg_thread_user_tts'] = cand_df['TW_tweets'] / cand_df['TW_threads']

"""Média de tweets do usuario pelo total de tweets em suas threads. Indica o quanto do engajamento das threads vem do próprio usuário"""
cand_df['TW_avg_thread_part'] = cand_df['TW_tweets'] / cand_df['TW_thread_tts']

"""Média de replies que o usuário recebe para cada tweet postado. Indica se o usuário recebe um alto engajamento direto"""
cand_df['TW_avg_replies'] = cand_df['TW_replies'] / cand_df['TW_tweets']

"""Média do total de tweets por thread do usuário. Indica o engajamento geral nas threads do usuário"""
cand_df['TW_avg_thread_tts'] = cand_df['TW_thread_tts'] / cand_df['TW_threads']
```

# Métricas proporcionais de ofensa

```python
"""Taxa de tweets ofensivos por thread do usuário"""
cand_df['TW_offs_per_thread'] = cand_df['TW_thread_offs'] / cand_df['TW_threads']

"""Taxa de tweets ofensivos pelo total de tweets postados pelo usuário"""
cand_df['TW_user_offs_rate'] = cand_df['TW_user_offs'] / cand_df['TW_tweets']

"""Taxa de replies ofensivas pelo total de replies recebidas pelo usuário"""
cand_df['TW_reply_offs_rate'] = cand_df['TW_reply_offs'] / cand_df['TW_replies']

"""Taxa de tweets ofensivos pelo total de tweets em threads do usuário"""
cand_df['TW_thread_offs_rate'] = cand_df['TW_thread_offs'] / cand_df['TW_thread_tts']
```

cand_df['TW_avg_thread_part'] = cand_df['TW_tweets'] / cand_df['TW_thread_tts']

cand_df['TW_offs_per_thread'] = cand_df['TW_thread_offs'] / cand_df['TW_threads']
cand_df['TW_thread_offs_rate'] = cand_df['TW_thread_offs'] / cand_df['TW_thread_tts']


```python
account_metric_cols = ['TW_followersCount', "TW_friendsCount", "TW_statusesCount", "TW_favouritesCount",]
tweet_length_cols = ['TW_avg_tt_length', 'TW_std_tt_length',]
abs_activity_cols = ['TW_tweets', 'TW_threads', 'TW_replies', 'TW_thread_tts',]
prop_activity_cols = ['TW_avg_thread_user_tts', 'TW_avg_thread_part', 'TW_avg_replies', 'TW_avg_thread_tts']
thread_comp_cols = ['TW_user_thread_tts', 'TW_reply_thread_tts', 'TW_non_reply_thread_tts']
offense_count_cols = ['TW_user_offs', 'TW_reply_offs', 'TW_thread_offs']
offense_prop_cols = ['TW_offs_per_thread', 'TW_user_offs_rate', 'TW_reply_offs_rate', 'TW_thread_offs_rate']


selected_cols = ['TW_threads', 'TW_avg_thread_user_tts', 'TW_avg_thread_tts', 'TW_offs_per_thread', 'TW_thread_offs_rate']


cols_to_check = ['DS_GENERO', 'DS_COR_RACA', 'DS_GRAU_INSTRUCAO', 'DS_CARGO', 'NR_IDADE_DATA_POSSE', 'ST_REELEICAO', 'TW_followersCount', 'SG_PARTIDO']
```

Apresentação

    OK- Diferença de score por classe
        - Possível viés em nosso classificador
            - Mais dúvida para classificar conteúdos ofensivos, devido a um menor numero de exemplos nos conjuntos de treinamento?

    OK- Diferença de tamanho de tweet por classe
        - Possível viés em nosso classificador, ou em nosso conjunto de teste para o ajuste fino

    OK- Histograma de threads e tamanho médio de threads 
        - Dá a dimensão da assimetria das threads entre os candidatos

    - Histogramas de tweets ofensivos (junto com numeros absolutos)
        - 

    - Números proporcionais de tweets ofensivos
        - 

    OK- Tweets ofensivos por tamanho de thread
        - Mostra como as pessoas tendem a ofender mais à medida que as threads crescem

    OK- Taxa de tweets ofensivos postados e tamanho médio de tweet
        TW_avg_tt_length por TW_user_offs_rate
            {'corr': -0.594795697709295}	

    OK- Participação nas threads inversamente proporcional ao numero de ofensas por thread
        - TW_avg_thread_part 
            TW_offs_per_thread
                {'corr': -0.6561520455446417}	
            TW_thread_offs_rate
                {'corr': -0.5852197593386925}

    OK- Boxplots de tweets ofensivos postados por gênero
        - 


Extra
    - Avaliar métricas de classe e score sem filtrar tweets
    - Candidatos à reeleição
        ST_REELEICAO, TW_reply_offs_rate
    - Analise taxa de ofensas por partidos
        TW_user_offs_rate, SG_PARTIDO


thread_offs_rate e thread_tts