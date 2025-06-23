import MetaTrader5 as mt5
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from tradeStore import StoredTradeSignalDict
from utils import to_mt5_expiration_timestamp
from enum import Enum
import traceback
from configs import logger


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
        try:
            return datetime.now() - datetime.fromtimestamp(position.time)
        except Exception as e:
            logger.error(f"Error calculating position age: {str(e)}")
            raise PositionAnalysisError("Failed to calculate position age") from e

    @staticmethod
    def is_profitable(position: mt5.TradePosition) -> bool:
        """Check if position is in profit"""
        try:
            return position.profit > 0
        except AttributeError as e:
            logger.error(f"Invalid position object provided: {str(e)}")
            raise PositionAnalysisError("Invalid position object") from e

    @staticmethod
    def is_old(
        position: mt5.TradePosition, threshold: timedelta = timedelta(hours=2)
    ) -> bool:
        """Check if position is older than threshold"""
        try:
            return PositionAnalyzer.get_position_age(position) > threshold
        except Exception as e:
            logger.error(f"Error checking position age: {str(e)}")
            raise PositionAnalysisError("Failed to check position age") from e


class TradeExecutor:
    """Handles execution of trade operations"""

    MAGIC_NUMBER = 202508  # Unique identifier for EA trades
    DEFAULT_DEVIATION = 35  # Max price deviation for execution

    @staticmethod
    def execute_order(request: Dict[str, Any]) -> Dict[str, Any]:
        """Send order to MT5 and return result"""
        try:
            if not mt5.initialize():
                raise TradeExecutionError("MT5 initialization failed")

            result = mt5.order_send(request)

            if not result:
                error_msg = f"Error executing trade -> {mt5.last_error()}"
                logger.error(error_msg)
                print(f"result - > ${result}")
                raise TradeExecutionError(error_msg)

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                error_msg = f"Trade failed with code {result.retcode}, {mt5.last_error()} --comment: {result.comment}"
                logger.error(error_msg)
                print(f"result - > ${result}")
                raise TradeExecutionError(error_msg)

            logger.info(f"Order executed successfully: {result}")
            return result._asdict()

        except Exception as e:
            logger.error(
                f"Unexpected error in execute_order: {str(e)}\n{traceback.format_exc()}"
            )
            raise TradeExecutionError(f"Order execution failed: {str(e)}") from e

    @staticmethod
    def create_request(
        symbol: str,
        order_type: OrderType,
        volume: float,
        price: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        is_pending: bool = False,
        expiration: Optional[int] = None,
        comment: str = "Comment",
    ) -> Dict[str, Any]:
        """Create a flexible trade request dictionary compatible with different trade types."""
        try:
            request = {
                "action": (
                    mt5.TRADE_ACTION_PENDING if is_pending else mt5.TRADE_ACTION_DEAL
                ),
                "symbol": symbol,
                "volume": volume,
                "type": order_type.value,
                "price": price,
                "deviation": TradeExecutor.DEFAULT_DEVIATION,
                "magic": TradeExecutor.MAGIC_NUMBER,
                "comment": comment,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            # Optional fields - only set if not None
            if sl is not None:
                request["sl"] = sl
            if tp is not None:
                request["tp"] = tp

            # Time type logic
            if expiration:
                request["type_time"] = mt5.ORDER_TIME_SPECIFIED
                request["expiration"] = int(expiration)
            else:
                request["type_time"] = mt5.ORDER_TIME_GTC

            logger.debug(f"Created trade request: {request}")
            return request

        except Exception as e:
            logger.error(
                f"Error creating trade request: {str(e)}\n{traceback.format_exc()}"
            )
            raise TradeRequestError("Failed to create trade request") from e


class PositionManager:
    """Manages open positions including modifications and closures"""

    @staticmethod
    def close_position(position_id: int) -> bool:
        """Close an open position by its ticket ID"""
        try:
            position = mt5.positions_get(ticket=position_id)
            if not position:
                raise PositionNotFoundError(
                    f"No position found with ticket {position_id}"
                )

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

            result = TradeExecutor.execute_order(request)
            logger.info(f"Successfully closed position {position_id}")
            return True

        except TradeExecutionError as e:
            logger.error(f"Failed to close position {position_id}: {str(e)}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error closing position {position_id}: {str(e)}\n{traceback.format_exc()}"
            )
            return False

    @staticmethod
    def move_to_breakeven(position_id: int) -> bool:
        """Move stop loss to entry price (breakeven)"""
        try:
            position = mt5.positions_get(ticket=position_id)
            if not position:
                raise PositionNotFoundError(
                    f"No position found with ticket {position_id}"
                )

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

            result = mt5.order_send(request)
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"Successfully moved position {position_id} to breakeven")
                return True

            error_msg = f"Failed to modify position: {result.retcode}"
            logger.error(error_msg)
            raise TradeExecutionError(error_msg)

        except TradeExecutionError as e:
            logger.error(f"Error moving position {position_id} to breakeven: {str(e)}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error moving position {position_id} to breakeven: {str(e)}\n{traceback.format_exc()}"
            )
            return False


class TradeValidator:
    """Validates trade conditions before execution"""

    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """Check if symbol exists and is marketable"""
        try:
            if not mt5.initialize():
                raise TradeError("MT5 initialization failed")

            if not mt5.symbol_select(symbol, True):
                raise InvalidSymbolError(f"Symbol {symbol} not available")

            symbol_info = mt5.symbol_info(symbol)
            if not symbol_info:
                raise InvalidSymbolError(f"Could not get info for symbol {symbol}")

            if not symbol_info.visible:
                raise InvalidSymbolError(f"Symbol {symbol} not visible")

            if symbol_info.trade_mode != mt5.SYMBOL_TRADE_MODE_FULL:
                raise InvalidSymbolError(f"Symbol {symbol} not tradeable")

            return True

        except Exception as e:
            logger.error(f"Symbol validation failed for {symbol}: {str(e)}")
            raise

    @staticmethod
    def check_pending_orders(symbol: str) -> bool:
        """
        Check for existing pending orders on the same symbol
        Returns True if no conflicting orders exist or if conflicting orders were successfully removed
        """
        try:
            if not mt5.initialize():
                raise TradeError("MT5 initialization failed")

            # Get all pending orders for this symbol
            orders = mt5.orders_get(symbol=symbol)
            if not orders:
                return True  # No pending orders
            return False

        except Exception as e:
            logger.error(f"Error checking pending orders for {symbol}: {str(e)}")
            raise TradeValidationError(f"Pending order check failed: {str(e)}") from e

    @staticmethod
    def check_existing_positions(symbol: str) -> bool:
        """
        Enhanced version that checks both open positions AND pending orders
        """
        try:
            # First check open positions
            if not TradeValidator._check_open_positions(symbol):
                return False

            # Then check pending orders
            if not TradeValidator.check_pending_orders(symbol):
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking existing trades for {symbol}: {str(e)}")
            raise TradeValidationError(f"Trade validation failed: {str(e)}") from e

    @staticmethod
    def _check_open_positions(symbol: str) -> bool:
        """
        Internal method to handle just the open positions logic
        (This is your original check_existing_positions implementation)
        """
        try:
            if not mt5.initialize():
                raise TradeError("MT5 initialization failed")

            positions = mt5.positions_get(symbol=symbol)
            if not positions:
                return True  # No conflicting positions

            for pos in positions:
                try:
                    if PositionAnalyzer.is_profitable(pos) and PositionAnalyzer.is_old(
                        pos
                    ):
                        PositionManager.move_to_breakeven(pos.ticket)
                        return True
                    elif not PositionAnalyzer.is_profitable(
                        pos
                    ) and PositionAnalyzer.is_old(pos):
                        PositionManager.close_position(pos.ticket)
                        return True
                    else:
                        logger.info(
                            f"Active position exists for {symbol} - not opening new trade"
                        )
                        return False
                except Exception as e:
                    logger.error(f"Error processing position {pos.ticket}: {str(e)}")
                    continue

            return True

        except Exception as e:
            logger.error(f"Error checking open positions for {symbol}: {str(e)}")
            raise


class TradingService:
    """Main trading service that orchestrates all operations"""

    @staticmethod
    def process_signal(signal: "StoredTradeSignalDict") -> Dict[str, Any]:
        """Process a trading signal"""
        try:
            logger.info(f"Processing signal: {signal}")

            if signal.direction == "HOLD":
                logger.info("Ignoring HOLD signal")
                return {"status": "ignored", "message": "HOLD signal"}

            try:
                # Validate symbol and position conditions
                TradeValidator.validate_symbol(signal.symbol)
                if not TradeValidator.check_existing_positions(
                    signal.symbol
                ):  # Now checks both
                    msg = "Existing position or pending order blocks new trade"
                    logger.info(msg)
                    return {"status": "skipped", "message": msg}

                # Prepare order parameters
                order_type = TradingService._determine_order_type(signal)
                volume = PositionCalculator.calculate_position_size(
                    signal.symbol, stop_loss_pips=float(signal.stopLoss.pips)
                )

                if signal.entry.type == "market":
                    tick = mt5.symbol_info_tick(signal.symbol)
                    if not tick:
                        raise TradeError(f"Could not fetch tick for {signal.symbol}")
                    price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid
                else:
                    price = float(signal.entry.price) 

                # Create and execute order
                request = TradeExecutor.create_request(
                    symbol=signal.symbol,
                    order_type=order_type,
                    volume=volume,
                    price=price,
                    sl=signal.stopLoss.price,
                    tp=signal.takeProfits[0].price if signal.takeProfits else None,
                    is_pending=signal.entry.type != "market",
                    expiration=(
                        to_mt5_expiration_timestamp(
                            signal.entry.validUntil, signal.symbol
                        )
                        if signal.entry.validUntil
                        else None
                    ),
                    comment=f"Trade open {signal.symbol}",
                )

                result = TradeExecutor.execute_order(request)

                response = {
                    "status": "placed",
                    "order": result,
                }
                logger.info(f"Successfully processed signal: {response}")
                return response

            except TradeError as e:
                logger.error(f"Trade error processing signal: {str(e)}")
                return {"status": "error", "message": str(e)}
            except Exception as e:
                logger.error(
                    f"Unexpected error processing signal: {str(e)}\n{traceback.format_exc()}"
                )
                return {"status": "error", "message": f"Unexpected error: {str(e)}"}

        except Exception as e:
            logger.error(
                f"Fatal error in process_signal: {str(e)}\n{traceback.format_exc()}"
            )
            return {"status": "error", "message": f"Fatal processing error: {str(e)}"}

    @staticmethod
    def _determine_order_type(signal: "StoredTradeSignalDict") -> OrderType:
        """Map signal to MT5 order type"""
        try:
            type_map = {
                ("BUY", "limit"): OrderType.BUY_LIMIT,
                ("SELL", "limit"): OrderType.SELL_LIMIT,
                ("BUY", "stop"): OrderType.BUY_STOP,
                ("SELL", "stop"): OrderType.SELL_STOP,
                ("BUY", "market"): OrderType.BUY_MARKET,
                ("SELL", "market"): OrderType.SELL_MARKET,
            }
            return type_map[(signal.direction, signal.entry.type)]
        except KeyError as e:
            logger.error(
                f"Invalid order type combination: {signal.direction}, {signal.entry.type}"
            )
            raise InvalidOrderTypeError(
                f"Invalid order type combination: {signal.direction}, {signal.entry.type}"
            ) from e


class PositionCalculator:
    """Calculates position sizes with proper risk management, considering leverage."""

    RISK_PERCENT = 0.5  # Risk 0.5% of account balance per trade
    MIN_LOT_SIZE = 0.01
    DEFAULT_LOT_SIZE = 0.01

    @staticmethod
    def calculate_position_size(symbol: str, stop_loss_pips: float = None) -> float:
        """
        Calculate position size based on account balance, risk, stop loss and leverage.

        Args:
            symbol: Trading symbol
            stop_loss_pips: SL distance in pips
        Returns:
            position size (float)
        """
        try:
            if not mt5.initialize():
                raise TradeError("MT5 initialization failed")

            # Fetch account info
            account_info = mt5.account_info()
            if not account_info or account_info.balance <= 0:
                logger.warning("Invalid account info or balance")
                return PositionCalculator.DEFAULT_LOT_SIZE

            effective_balance = min(account_info.balance, account_info.equity)
            leverage = account_info.leverage or 100  # fallback if not set
            risk_amount = effective_balance * (PositionCalculator.RISK_PERCENT / 100)

            symbol_info = mt5.symbol_info(symbol)
            if not symbol_info:
                logger.warning(f"Symbol {symbol} not found")
                return PositionCalculator.DEFAULT_LOT_SIZE

            # Get current price (average of bid/ask)
            tick = mt5.symbol_info_tick(symbol)
            if not tick:
                logger.warning(f"Could not get tick data for {symbol}")
                return PositionCalculator.DEFAULT_LOT_SIZE

            price = (tick.ask + tick.bid) / 2
            contract_size = symbol_info.trade_contract_size
            pip_value = (
                (symbol_info.trade_tick_value / symbol_info.trade_tick_size)
                * symbol_info.point
                * 10
            )

            if stop_loss_pips is None or stop_loss_pips <= 0:
                logger.warning("Invalid stop_loss_pips, using default lot size")
                return PositionCalculator.DEFAULT_LOT_SIZE

            # Risk per lot
            risk_per_lot = stop_loss_pips * pip_value

            if risk_per_lot <= 0:
                logger.error("Invalid risk per lot")
                return PositionCalculator.DEFAULT_LOT_SIZE

            raw_position_size = risk_amount / risk_per_lot

            # Margin required per lot
            margin_per_lot = (contract_size * price) / leverage

            # Make sure we don't exceed margin capabilities
            max_lots_by_margin = effective_balance / margin_per_lot
            position_size = min(raw_position_size, max_lots_by_margin)

            # Clamp and round to step size
            position_size = max(
                PositionCalculator.MIN_LOT_SIZE,
                min(position_size, symbol_info.volume_max),
            )
            step = symbol_info.volume_step
            rounded_size = round(position_size / step) * step

            logger.info(
                f"✅ Position size for {symbol}: {rounded_size} lots with SL {stop_loss_pips} pips effective_balance:{effective_balance}"
            )
            return rounded_size

        except Exception as e:
            logger.error(
                f"Error calculating position size: {e}\n{traceback.format_exc()}"
            )
            return PositionCalculator.DEFAULT_LOT_SIZE


# Enhanced Custom Exceptions
class TradeError(Exception):
    """Base trading exception"""

    def __init__(self, message="Trade error occurred"):
        super().__init__(message)
        logger.error(message)


class TradeExecutionError(TradeError):
    """Trade execution failed"""

    def __init__(self, message="Trade execution failed"):
        super().__init__(message)
        logger.error(f"Trade Execution Error: {message}")


class PositionNotFoundError(TradeError):
    """Requested position not found"""

    def __init__(self, message="Position not found"):
        super().__init__(message)
        logger.error(f"Position Not Found: {message}")


class MarketDataError(TradeError):
    """Market data not available"""

    def __init__(self, message="Market data error"):
        super().__init__(message)
        logger.error(f"Market Data Error: {message}")


class InvalidSymbolError(TradeError):
    """Invalid trading symbol"""

    def __init__(self, message="Invalid symbol"):
        super().__init__(message)
        logger.error(f"Invalid Symbol: {message}")


class TradeRequestError(TradeError):
    """Invalid trade request"""

    def __init__(self, message="Invalid trade request"):
        super().__init__(message)
        logger.error(f"Trade Request Error: {message}")


class TradeValidationError(TradeError):
    """Trade validation failed"""

    def __init__(self, message="Trade validation failed"):
        super().__init__(message)
        logger.error(f"Trade Validation Error: {message}")


class PositionAnalysisError(TradeError):
    """Position analysis failed"""

    def __init__(self, message="Position analysis failed"):
        super().__init__(message)
        logger.error(f"Position Analysis Error: {message}")


class InvalidOrderTypeError(TradeError):
    """Invalid order type combination"""

    def __init__(self, message="Invalid order type"):
        super().__init__(message)
        logger.error(f"Invalid Order Type: {message}")
