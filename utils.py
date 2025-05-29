from typing import List
from configs import Config


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
