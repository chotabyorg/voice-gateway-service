"""Run the service without enabling a transport unless explicitly configured."""

import uvicorn

from voice_gateway.settings import Settings


def main():
    settings = Settings()
    uvicorn.run(
        "voice_gateway.app:app", host=settings.http_host, port=settings.http_port, access_log=False
    )


if __name__ == "__main__":
    main()
