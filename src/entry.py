from api.main import app


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error", access_log=False)