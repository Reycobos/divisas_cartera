import os
from dotenv import load_dotenv
load_dotenv()

DB_PATH = os.getenv("DB_PATH", "portfolio.db")
ENV = os.getenv("ENV", "dev")
