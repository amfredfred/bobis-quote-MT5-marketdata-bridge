from dotenv import load_dotenv
import os
import logging

load_dotenv(override=True)


class Config:
    @staticmethod
    def MAX_POSITION_PERCENT():
        return 0.15

    @staticmethod
    def MT5_ACCOUNT_NUMBER():
        return int(os.getenv("MT5_ACCOUNT_NUMBER"))

    @staticmethod
    def MT5_ACCOUNT_PASSWORD():
        return os.getenv("MT5_ACCOUNT_PASSWORD")

    @staticmethod
    def MT5_ACCOUNT_SERVER():
        return os.getenv("MT5_ACCOUNT_SERVER")

    @staticmethod
    def PATH_MT5_EXEC():
        return os.getenv("PATH_MT5_EXEC")

    @staticmethod
    def TIMEZONE():
        return os.getenv("TIMEZONE")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("trading_service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
