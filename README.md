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


## Rodando pipeline de candidatos
```
python src/candidates.py 
```

## Rodando pipeline de processamento de tweets
```
python -m src.tweets.flow
```

# Sobre o projeto

## Arquitetura
Principais pastas e arquivos do projeto:

- `data/`: Armazena os datasets usados e gerados
- `data/processed/`: Armazena os resultados dos nossos processamentos
- `data/raw/`: Armazena os datasets iniciais "crus" (antes de qualquer processamento)
- `notebooks/`: Armazena os notebooks, usados para experimentos e provas de conceito
- `src/`: Armazena os códigos dos pipelines e outros scripts
- `src/candidates.py`: Funções, tasks e flows relacionados ao processamento dos dados de candidatos
- `src/config.py`: Configurações e variáveis gerais do projeto
- `src/main.py`: Pipeline principal
- `src/utils.py`: Funções de uso mais geral

## Fonte de dados
Dados sobre candidatos extraídos do [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/dataset/candidatos-2022).