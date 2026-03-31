from dotenv import load_dotenv
import os

load_dotenv(override=True)


class Config:
    MT5_ACCOUNT_NUMBER = int(os.getenv("MT5_ACCOUNT_NUMBER"))
    MT5_ACCOUNT_PASSWORD = os.getenv("MT5_ACCOUNT_PASSWORD")
    MT5_ACCOUNT_SERVER = os.getenv("MT5_ACCOUNT_SERVER")
    PATH_MT5_EXEC = os.getenv("PATH_MT5_EXEC")
    LOG_LEVEL = os.getenv("LOG_LEVEL")
