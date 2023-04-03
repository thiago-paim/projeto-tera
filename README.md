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
```
### Processamento de tweets
```
python -m src.flows.process_erikas_tweets
```

### Visualizando no MLflow
```
mlflow ui
```

# Sobre o projeto

## Arquitetura
Principais pastas e arquivos do projeto:

- `data/`: Armazena os datasets usados e gerados
- `data/processed/`: Armazena os resultados dos nossos processamentos
- `data/raw/`: Armazena os datasets iniciais "crus" (antes de qualquer processamento)
- `notebooks/`: Armazena os notebooks, usados para experimentos e provas de conceito
- `src/`: Armazena os códigos dos pipelines e outros scripts
- `src/flows/`: Cada arquivo contém um flow e suas tasks relacionadas
- `src/common.py`: Funções de uso geral
- `src/config.py`: Configurações e variáveis gerais do projeto
- `src/feature_extraction.py`: Funções relacionadas a extração de features
- `src/filters.py`: Funções relacionadas a remoção de linhas e colunas
- `src/preprocess.py`: Funções relacionadas a pré-processamento de dados




## Fonte de dados
Dados sobre candidatos extraídos do [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/dataset/candidatos-2022).