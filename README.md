# projeto-tera

# Quickstart

## Setup
Crie um ambiente virtual e instale as dependencias:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Faça [login no Prefect Cloud](https://docs.prefect.io/ui/cloud-quickstart/#log-into-prefect-cloud-from-a-terminal) através do terminal:
```
prefect cloud login
```

## Rodando pipelines

### Processamento de dados de candidatos
```
python -m src.flows.process_candidates_data
python -m src.flows.process_southeast_candidates_data
```

### Atualização de dados do Twitter
python -m src.flows.update_twitter_user_data data/processed/se_candidates_output_3.csv
python -m src.flows.update_tweets_count data/processed/se_candidates_output_4.csv

### Processamento de tweets
```
python -m src.flows.process_erikas_tweets
python -m src.flows.sentiment_analysis /data/processed/sample-100-ErikakHilton-tweets.csv

python -m src.flows.classify_tweets erika-short.csv local content
python -m src.flows.classify_tweets sp_elected_stdep_tweets.csv local content
python -m src.flows.classify_tweets_2_0 sp_elected_stdep_tweets.csv local content

```

### Amostragem de datasets
```
python -m src.flows.sample_dataset /data/raw/ErikakHilton-tweets.csv
```

## Visualizando no MLflow
```
mlflow ui
```

# Sobre o projeto

## Arquitetura
Principais pastas e arquivos do projeto:

```
├── data                    -> Armazena os datasets usados e gerados 
│   ├── processed               -> Datasets pós processamento
│   └── raw                     -> Datasets iniciais, sem tratamento
├── logs                    -> Armazena logs gerais
├── notebooks               -> Armazena os notebooks, usados para experimentos e provas de conceito
│   ├── fine_tuned_model        -> Pasta contendo modelo refinado nos notebooks
│   ├── 
├── src                     -> Armazena os códigos dos pipelines e outros scripts
│   └── flows                   -> Cada arquivo contém um flow e suas tasks relacionadas
│   │   └── 
│   ├── common.py               -> Funções de uso geral
│   ├── config.py               -> Configurações e variáveis gerais do projeto
│   ├── feature_extraction.py   -> Funções relacionadas a extração de features
│   ├── filters.py              -> Funções relacionadas a remoção de linhas e colunas
│   ├── models.py               -> Funções relacionadas a modelos para treinamento e classificação
│   └── preprocess.py           -> Funções relacionadas a pré-processamento de dados
│   └── 
```


## Fonte de dados
Dados sobre candidatos extraídos do [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/dataset/candidatos-2022).
  