import MetaTrader5 as mt5
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
from tradeStore import StoredTradeSignalDict


class OrderType(Enum):
    """Enum for order types to improve code readability"""

    BUY_LIMIT = mt5.ORDER_TYPE_BUY_LIMIT
    SELL_LIMIT = mt5.ORDER_TYPE_SELL_LIMIT
    BUY_STOP = mt5.ORDER_TYPE_BUY_STOP
    SELL_STOP = mt5.ORDER_TYPE_SELL_STOP
    BUY_MARKET = mt5.ORDER_TYPE_BUY
    SELL_MARKET = mt5.ORDER_TYPE_SELL


class PositionAnalyzer:
    """Handles analysis and management of existing positions"""

    @staticmethod
    def get_position_age(position: mt5.TradePosition) -> timedelta:
        """Calculate how long a position has been open"""
        return datetime.now() - datetime.fromtimestamp(position.time)

    @staticmethod
    def is_profitable(position: mt5.TradePosition) -> bool:
        """Check if position is in profit"""
        return position.profit > 0

    @staticmethod
    def is_old(
        position: mt5.TradePosition, threshold: timedelta = timedelta(hours=1)
    ) -> bool:
        """Check if position is older than threshold"""
        return PositionAnalyzer.get_position_age(position) > threshold


class TradeExecutor:
    """Handles execution of trade operations"""

    MAGIC_NUMBER = 202508  # Unique identifier for EA trades
    DEFAULT_DEVIATION = 10  # Max price deviation for execution

    @staticmethod
    def execute_order(request: Dict[str, Any]) -> Dict[str, Any]:
        """Send order to MT5 and return result"""
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            raise TradeExecutionError(f"Trade failed with code {result.retcode}")
        return result._asdict()

    @staticmethod
    def create_request(
        symbol: str,
        order_type: OrderType,
        volume: float,
        price: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        is_pending: bool = False,
        expiration: Optional[datetime] = None,
        comment: str = "",
    ) -> Dict[str, Any]:
        """Create trade request dictionary"""
        return {
            "action": mt5.TRADE_ACTION_PENDING if is_pending else mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": order_type.value,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": TradeExecutor.DEFAULT_DEVIATION,
            "magic": TradeExecutor.MAGIC_NUMBER,
            "comment": comment,
            "type_time": (
                mt5.ORDER_TIME_GTC if not expiration else mt5.ORDER_TIME_SPECIFIED
            ),
            "type_filling": mt5.ORDER_FILLING_IOC,
            "expiration": int(expiration.timestamp()) if expiration else None,
        }


class PositionManager:
    """Manages open positions including modifications and closures"""

    @staticmethod
    def close_position(position_id: int) -> bool:
        """Close an open position by its ticket ID"""
        position = mt5.positions_get(ticket=position_id)
        if not position:
            raise PositionNotFoundError(f"No position found with ticket {position_id}")

        pos = position[0]
        symbol = pos.symbol
        tick = mt5.symbol_info_tick(symbol)

        if not tick:
            raise MarketDataError(f"Could not get price for symbol {symbol}")

        # Determine closing parameters
        close_type = (
            mt5.ORDER_TYPE_SELL
            if pos.type == mt5.POSITION_TYPE_BUY
            else mt5.ORDER_TYPE_BUY
        )
        price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

        request = TradeExecutor.create_request(
            symbol=symbol,
            order_type=OrderType(close_type),
            volume=pos.volume,
            price=price,
            comment=f"Closing position {position_id}",
        )
        request["position"] = position_id

        try:
            result = TradeExecutor.execute_order(request)
            return True
        except TradeExecutionError as e:
            print(f"Failed to close position: {str(e)}")
            return False

    @staticmethod
    def move_to_breakeven(position_id: int) -> bool:
        """Move stop loss to entry price (breakeven)"""
        position = mt5.positions_get(ticket=position_id)
        if not position:
            raise PositionNotFoundError(f"No position found with ticket {position_id}")

        pos = position[0]

        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": position_id,
            "symbol": pos.symbol,
            "sl": pos.price_open,  # Move SL to entry price
            "tp": pos.tp,  # Keep existing take profit
            "deviation": TradeExecutor.DEFAULT_DEVIATION,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }

        try:
            result = mt5.order_send(request)
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                return True
            raise TradeExecutionError(f"Failed to modify position: {result.retcode}")
        except Exception as e:
            print(f"Error moving to breakeven: {str(e)}")
            return False


class TradeValidator:
    """Validates trade conditions before execution"""

    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """Check if symbol exists and is marketable"""
        if not mt5.symbol_select(symbol, True):
            raise InvalidSymbolError(f"Symbol {symbol} not available")
        return True

    @staticmethod
    def check_existing_positions(symbol: str) -> bool:
        """
        Check existing positions for the symbol and apply logic:
        - If profitable and old enough: move to breakeven
        - If losing and old enough: close position
        - Returns True if new trade can be placed
        """
        positions = mt5.positions_get(symbol=symbol)
        if not positions:
            return True  # No conflicting positions

        for pos in positions:
            if PositionAnalyzer.is_profitable(pos) and PositionAnalyzer.is_old(pos):
                PositionManager.move_to_breakeven(pos.ticket)
                return True
            elif not PositionAnalyzer.is_profitable(pos) and PositionAnalyzer.is_old(
                pos
            ):
                PositionManager.close_position(pos.ticket)
                return True
            else:
                # Recent or neutral position exists - don't open new one
                return False
        return True
 

class TradingService:
    """Main trading service that orchestrates all operations"""

    @staticmethod
    def process_signal(signal: "StoredTradeSignalDict") -> Dict[str, Any]:
        """Process a trading signal"""
        if signal.direction == "HOLD":
            return {"status": "ignored", "message": "HOLD signal"}

        try:
            # Validate symbol and position conditions
            TradeValidator.validate_symbol(signal.symbol)
            if not TradeValidator.check_existing_positions(signal.symbol):
                return {
                    "status": "skipped",
                    "message": "Existing position blocks new trade",
                }

            # Prepare order parameters
            order_type = TradingService._determine_order_type(signal)
            volume = TradingService._calculate_position_size(
                signal
            )  # Implement your sizing logic

            # Create and execute order
            request = TradeExecutor.create_request(
                symbol=signal.symbol,
                order_type=order_type,
                volume=volume,
                price=signal.entry.price,
                sl=signal.stopLoss,
                tp=signal.takeProfits[0].price if signal.takeProfits else None,
                is_pending=signal.entry.type != "market",
                expiration=(
                    datetime.strptime(signal.entry.validUntil, "%Y-%m-%dT%H:%M:%S")
                    if signal.entry.validUntil
                    else None
                ),
                comment=f"Signal: {signal.reason}",
            )

            result = TradeExecutor.execute_order(request)

            return {
                "status": "placed",
                "order": result,
                "trailingStop": signal.trailingStop,
                "takeProfits": signal.takeProfits,
            }

        except TradeError as e:
            return {"status": "error", "message": str(e)}

    @staticmethod
    def _determine_order_type(signal: "StoredTradeSignalDict") -> OrderType:
        """Map signal to MT5 order type"""
        type_map = {
            ("BUY", "limit"): OrderType.BUY_LIMIT,
            ("SELL", "limit"): OrderType.SELL_LIMIT,
            ("BUY", "stop"): OrderType.BUY_STOP,
            ("SELL", "stop"): OrderType.SELL_STOP,
            ("BUY", "market"): OrderType.BUY_MARKET,
            ("SELL", "market"): OrderType.SELL_MARKET,
        }
        return type_map.get((signal.direction, signal.entry.type))


# Custom Exceptions
class TradeError(Exception):
    """Base trading exception"""

    pass


class TradeExecutionError(TradeError):
    """Trade execution failed"""

    pass


class PositionNotFoundError(TradeError):
    """Requested position not found"""

    pass


class MarketDataError(TradeError):
    """Market data not available"""

    pass


class InvalidSymbolError(TradeError):
    """Invalid trading symbol"""

    pass
