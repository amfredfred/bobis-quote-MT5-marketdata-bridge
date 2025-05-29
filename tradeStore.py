import json
from pathlib import Path
from typing import Dict, Optional
from typing import TypedDict, List, Optional


class TakeProfitLevelDict(TypedDict):
    name: str  # 'TP1' | 'TP2' | ...
    price: float
    percentage: Optional[float]
    reason: Optional[str]


class EntryDict(TypedDict):
    price: float
    type: str  # 'limit' | 'stop' | 'market'
    validUntil: Optional[str]


class StoredTradeSignalDict(TypedDict):
    symbol: str
    direction: str  # 'BUY' | 'SELL' | 'HOLD'
    entry: EntryDict
    stopLoss: float
    takeProfits: List[TakeProfitLevelDict]
    confidence: float
    reason: str
    timestamp: str
    lot_size: float  # You can add any extra fields you want
    order_id: int


STORE_FILE = Path("trade_signals_store.json")


# Load all trade signals as a dict keyed by order_id (string)
def load_trade_signals() -> Dict[str, StoredTradeSignalDict]:
    if STORE_FILE.exists():
        with open(STORE_FILE, "r") as f:
            return json.load(f)
    return {}


# Save or update a trade signal by order_id
def save_trade_signal(order_id: int, trade_signal: StoredTradeSignalDict) -> None:
    signals = load_trade_signals()
    signals[str(order_id)] = trade_signal  # use str key for JSON compatibility
    with open(STORE_FILE, "w") as f:
        json.dump(signals, f, indent=2)


# Fetch trade signal by order_id
def get_trade_signal(order_id: int) -> Optional[StoredTradeSignalDict]:
    signals = load_trade_signals()
    return signals.get(str(order_id))
