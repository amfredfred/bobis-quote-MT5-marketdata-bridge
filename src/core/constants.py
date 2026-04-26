"""Constants for MT5 market data processing."""

import MetaTrader5 as mt5

# Timeframe mapping from string to MT5 constant
_TIMEFRAME_MAP: dict[str, int] = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "6m": mt5.TIMEFRAME_M6,
    "10m": mt5.TIMEFRAME_M10,
    "15m": mt5.TIMEFRAME_M15,
    "30m": mt5.TIMEFRAME_M30,
    "1h": mt5.TIMEFRAME_H1,
    "4h": mt5.TIMEFRAME_H4,
    "d1": mt5.TIMEFRAME_D1,
    "w1": mt5.TIMEFRAME_W1,
    "mn1": mt5.TIMEFRAME_MN1,
}

# Nominal seconds per bar — used for gap detection and staleness checks.
# W1 / MN1 calendar-correct logic is handled separately.
_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "6m": 360,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14_400,
    "d1": 86_400,
    "w1": 604_800,
    "mn1": 2_592_000,
}

# Instruments where MT5 real_volume is always zero.
# These are synthetic CFDs with no centralised exchange.
_TICK_VOLUME_ONLY_PREFIXES = (
    "US",
    "UK",
    "DE",
    "FR",
    "JP",
    "AU",
    "XAU",
    "XAG",
    "XPT",
    "XPD",
    "BTC",
    "ETH",
    "LTC",
    "XRP",
)

# Instruments with a daily trading session break (~2 h nightly close).
# For these, intraday gaps up to 4 h and weekend gaps up to 72 h are normal
# and must not be flagged as data errors.
_SESSION_BREAK_PREFIXES = (
    "XAU",
    "XAG",
    "XPT",
    "XPD",
    "BTC",
    "ETH",
    "LTC",
    "XRP",
    "US",
    "UK",
    "DE",
    "FR",
    "JP",
)

# Maximum gap (seconds) that is still considered a normal session break.
# Nightly close ≈ 2 h; give 4 h of margin.
# Weekend close ≈ 48–72 h; anything up to 75 h is ignored.
_SESSION_BREAK_INTRADAY_S = 4 * 3600  # 4 h
_SESSION_BREAK_WEEKEND_S = 75 * 3600  # 75 h
