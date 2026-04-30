"""FastAPI application entry point with lifespan management."""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from core.performance import CachedMarketDataProvider
from core.configs import Config

from . import routes

logging.basicConfig(level=Config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize provider on startup, shutdown on exit."""
    # Startup
    try:
        app.state.provider = CachedMarketDataProvider(Config)
        logger.info("CachedMarketDataProvider initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MarketDataProvider: {e}")
        raise

    yield

    # Shutdown
    if hasattr(app.state, "provider") and app.state.provider:
        app.state.provider.shutdown()
        logger.info("MarketDataProvider shut down")


# Create FastAPI app with lifespan
app = FastAPI(
    title="MT5 Candle Service",
    description="Production-grade market data API for MetaTrader 5",
    version="1.0.0",
    lifespan=lifespan,
)

# Include routes
app.include_router(routes.router)


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "MT5 Candle Service",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }


# Global exception handler for better error responses
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.exception(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
