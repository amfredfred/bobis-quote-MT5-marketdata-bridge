from dotenv import load_dotenv
import os
import logging

load_dotenv()


class Config:
    MAX_POSITION_PERCENT = 0.15
    MT5_ACCOUNT_NUMBER = int(os.getenv("MT5_ACCOUNT_NUMBER"))
    MT5_ACCOUNT_PASSWORD = os.getenv("MT5_ACCOUNT_PASSWORD")
    MT5_ACCOUNT_SERVER = os.getenv("MT5_ACCOUNT_SERVER")
    PATH_TO_MT5_EXE = os.getenv("PATH_TO_MT5_EXE")
    TIMEZONE = os.getenv('TIMEZONE')


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("trading_service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
