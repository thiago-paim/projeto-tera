import os
from decouple import config


PROJECT_PATH = os.path.join(os.getcwd())
RAW_DATASETS_PATH = os.path.join(os.getcwd(), "data", "raw")
PROCESSED_DATASETS_PATH = os.path.join(os.getcwd(), "data", "processed")

OPENAI_API_KEY = config("OPENAI_API_KEY")
PREFECT_API_KEY = config("PREFECT_API_KEY")
