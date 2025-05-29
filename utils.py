from typing import List
from configs import Config
from datetime import datetime
import pytz


def clean_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace("_", "")


def total_open_positions_size(open_positions: List[float]) -> float:
    """
    Sum the sizes of all currently open positions in account currency.
    """
    return sum(open_positions)


def can_open_new_position(
    account_balance: float,
    open_positions_sizes: List[float],
    new_position_size: float,
    max_percent: float = Config.MAX_POSITION_PERCENT,
) -> bool:
    """
    Check if opening a new position will keep total exposure under max_percent of account.

    Args:
        account_balance: Total account balance/equity
        open_positions_sizes: List of sizes of currently open positions (account currency)
        new_position_size: Size of the new position you want to open (account currency)
        max_percent: Max allowed exposure (default 15%)

    Returns:
        bool: True if new position can be opened, else False
    """
    current_total = total_open_positions_size(open_positions_sizes)
    allowed_total = account_balance * max_percent
    return (current_total + new_position_size) <= allowed_total


UTC = pytz.UTC
UTC_PLUS_3 = pytz.timezone("Etc/GMT-3")

def to_utc_plus_3_from_iso(iso_str: str) -> datetime:
    """
    Convert an ISO 8601 string to UTC+3 timezone-aware datetime.

    Args:
        iso_str (str): ISO formatted datetime string with timezone offset.

    Returns:
        datetime: Datetime in UTC+3.
    """
    dt = datetime.fromisoformat(iso_str)  # Already includes tzinfo if offset is present
    return dt.astimezone(UTC_PLUS_3)


def to_utc_iso_from_utc_plus_3(iso_str: str) -> datetime:
    """
    Convert an ISO 8601 string in UTC+3 to UTC datetime.

    Args:
        iso_str (str): ISO formatted datetime string with offset.

    Returns:
        datetime: UTC datetime.
    """
    dt = datetime.fromisoformat(iso_str)
    return dt.astimezone(UTC)
