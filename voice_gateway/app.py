"""HTTP liveness/readiness plus optional loopback AudioSocket listener."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from voice_gateway.application import VoiceGatewayRuntime
from voice_gateway.logging_config import configure_logging
from voice_gateway.settings import Settings


def create_app(settings=None, *, runtime=None):
    settings = settings or Settings()
    runtime = runtime or VoiceGatewayRuntime(settings)

    @asynccontextmanager
    async def lifespan(_app):
        configure_logging()
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(
        title="Voice Gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.runtime = runtime

    @app.get("/health", include_in_schema=False)
    async def health():
        return {"status": "ok"}

    @app.get("/ready", include_in_schema=False)
    async def ready():
        status = runtime.status()
        if not status.ready:
            raise HTTPException(
                status_code=503, detail={"status": "not_ready", "mode": status.mode}
            )
        return {"status": "ready", "mode": status.mode, "audiosocket_port": status.audiosocket_port}

    return app


app = create_app()
