"""Root-level entry point for the MT5 Candle Service API.

This file is the main entry point for running the server with uvicorn:

    uvicorn main:app --host 0.0.0.0 --port 8000

Or simply:

    python main.py
"""

from api.main import app


def main():
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
