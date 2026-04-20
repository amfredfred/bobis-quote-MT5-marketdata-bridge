from contextlib import asynccontextmanager
import asyncio
import logging
import sys

import MetaTrader5 as mt5
from fastapi import FastAPI, Query
from typing import Optional

from candle_service import (
    CandleRequest,
    get_multiple,
    preload_symbols,
    _detect_broker_offset_once,
)
from configs import Config

logging.basicConfig(
    level=Config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _make_loop_exception_handler(log: logging.Logger):
    """
    Suppress ConnectionResetError [WinError 10054] raised inside asyncio
    ProactorEventLoop callbacks when a client disconnects mid-request.

    On Windows, the ProactorEventLoop calls socket.shutdown(SHUT_RDWR) in
    _call_connection_lost even when the remote already closed the connection.
    The resulting ConnectionResetError is raised inside a callback — not a
    coroutine — so asyncio has no awaitable to propagate it through and logs
    it as an "Exception in callback", then stalls. Filtering it here keeps
    the server running normally; the connection is already gone, so no data
    is lost.
    """

    def handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if isinstance(exc, ConnectionResetError):
            # Client disconnected — entirely normal on Windows with Proactor.
            log.debug(
                "Client disconnected (WinError 10054 suppressed): %s",
                context.get("message", ""),
            )
            return
        # Everything else goes through the default handler unchanged.
        loop.default_exception_handler(context)

    return handler


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Install the Windows-safe exception handler as early as possible.
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_make_loop_exception_handler(logger))

    if not mt5.initialize(
        path=Config.PATH_MT5_EXEC,
        login=Config.MT5_ACCOUNT_NUMBER,
        password=Config.MT5_ACCOUNT_PASSWORD,
        server=Config.MT5_ACCOUNT_SERVER,
    ):
        code, desc = mt5.last_error()
        raise RuntimeError(f"MT5 init failed: {code} - {desc}")

    if not mt5.login(
        Config.MT5_ACCOUNT_NUMBER,
        Config.MT5_ACCOUNT_PASSWORD,
        Config.MT5_ACCOUNT_SERVER,
    ):
        code, desc = mt5.last_error()
        raise RuntimeError(f"MT5 login failed: {code} - {desc}")

    await asyncio.to_thread(_detect_broker_offset_once)
    await asyncio.to_thread(preload_symbols)

    logger.info("MT5 ready")
    yield

    mt5.shutdown()
    logger.info("MT5 shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/time-series")
async def time_series_body(request: CandleRequest):
    return await asyncio.to_thread(get_multiple, request)


@app.get("/time-series")
async def time_series_query(
    symbols: str = Query(...),
    timeframes: str = Query("1h"),
    limit: Optional[int] = Query(None, gt=0, le=1000),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    request = CandleRequest(
        symbols=[s.strip() for s in symbols.split(",")],
        timeframes=[tf.strip() for tf in timeframes.split(",")],
        limit=limit,
        from_date=from_date,
        to_date=to_date,
    )
    return await asyncio.to_thread(get_multiple, request)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
