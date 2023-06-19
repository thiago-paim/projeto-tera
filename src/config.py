import os
from decouple import config


PROJECT_PATH = os.path.join(os.getcwd())
RAW_DATASETS_PATH = os.path.join(os.getcwd(), "data", "raw")
PROCESSED_DATASETS_PATH = os.path.join(os.getcwd(), "data", "processed")
TEMP_FILES_PATH = os.path.join(os.getcwd(), "temp")

AZURE_STORAGE_ACCOUNT_URL = "https://projetotera.blob.core.windows.net"
AZURE_STORAGE_CONTAINER = "datasets"
AZURE_STORAGE_ACCOUNT_KEY = config("AZURE_STORAGE_ACCOUNT_KEY")
OPENAI_API_KEY = config("OPENAI_API_KEY")
PREFECT_API_KEY = config("PREFECT_API_KEY")
