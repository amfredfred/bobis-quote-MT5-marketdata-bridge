from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    MAX_POSITION_PERCENT = 0.15
    MT5_ACCOUNT_NUMBER = int(os.getenv("MT5_ACCOUNT_NUMBER"))
    MT5_ACCOUNT_PASSWORD = os.getenv("MT5_ACCOUNT_PASSWORD")
    MT5_ACCOUNT_SERVER = os.getenv("MT5_ACCOUNT_SERVER")
    PATH_TO_MT5_EXE = os.getenv("PATH_TO_MT5_EXE")
    TIMEZONE = os.getenv('TIMEZONE')
