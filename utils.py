from typing import List, Optional
from configs import Config
from datetime import datetime, timedelta, timezone
import pytz
import MetaTrader5 as mt5
from configs import logger


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
    max_percent: float = Config.MAX_POSITION_PERCENT(),
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


def get_server_time(symbol: str) -> datetime:
    tick = mt5.symbol_info_tick(symbol)
    if tick and tick.time:
        return datetime.utcfromtimestamp(tick.time).replace(tzinfo=pytz.UTC)
    raise RuntimeError("Failed to get server time from tick data.")


def to_mt5_expiration_timestamp(
    iso_str: Optional[str],
    symbol: str,
    min_buffer_sec: int = 180,
    default_minutes: int = 60,
) -> int:
    server_time = get_server_time(symbol)
    server_ts = int(server_time.timestamp())

    if iso_str:
        try:
            dt_local = datetime.fromisoformat(iso_str)  # e.g., WAT with tzinfo
            dt_utc = dt_local.astimezone(pytz.UTC)
            expiration_ts = int(dt_utc.timestamp())
        except Exception as e:
            logger.warning(f"[WARN] Failed to parse iso_str '{iso_str}': {e}")
            dt_utc = None
            expiration_ts = 0
    else:
        dt_utc = None
        expiration_ts = 0

    # If expiration invalid or too soon, fallback to server_time + default_minutes
    if expiration_ts < server_ts + min_buffer_sec:
        fallback_expiration = server_time + timedelta(minutes=default_minutes)
        expiration_ts = int(fallback_expiration.timestamp())
        expiration_ts -= expiration_ts % 60  # Round down to nearest minute
        logger.info(f"[INFO] ⏱ Using fallback expiration: {fallback_expiration} (UNIX: {expiration_ts})")
    else:
        expiration_ts -= expiration_ts % 60  # Round down to nearest minute

    return expiration_ts
