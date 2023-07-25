# Métricas proporcionais de engajamento

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


```python
direct_activity_cols = ['TW_tweets', 'TW_threads', 'TW_replies', 'TW_thread_tts',]
prop_activity_cols = ['TW_avg_thread_user_tts', 'TW_avg_thread_part', 'TW_avg_replies', 'TW_avg_thread_tts']
thread_comp_cols = ['TW_user_thread_tts', 'TW_reply_thread_tts', 'TW_non_reply_thread_tts']
offense_count_cols = ['TW_user_offs', 'TW_reply_offs', 'TW_thread_offs']
offense_prop_cols = ['TW_offs_per_thread', 'TW_user_offs_rate', 'TW_reply_offs_rate', 'TW_thread_offs_rate']


selected_cols = ['TW_threads', 'TW_avg_thread_user_tts', 'TW_avg_thread_tts', 'TW_offs_per_thread', 'TW_thread_offs_rate']
```
