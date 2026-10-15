"""Loopback-only authenticated PBX registration; browser identity is never accepted."""

import asyncio
import hmac
import json

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from voice_gateway.routing import Registration, RoutingUnavailable, service_token, unique_json


def install_pbx_api(app, registry, token):
    expected = service_token(token).encode()

    @app.post("/internal/v1/pbx/calls", include_in_schema=False)
    async def register(request: Request):
        tokens = request.headers.getlist("x-pbx-token")
        if len(tokens) != 1 or not hmac.compare_digest(tokens[0].encode(), expected):
            raise HTTPException(401, "Unauthorized", headers={"Cache-Control": "no-store"})
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise HTTPException(415, "Invalid request", headers={"Cache-Control": "no-store"})
        if request.headers.get("content-encoding", "identity") != "identity":
            raise HTTPException(415, "Invalid request", headers={"Cache-Control": "no-store"})
        try:
            async with asyncio.timeout(3):
                body = bytearray()
                async for chunk in request.stream():
                    if len(body) + len(chunk) > 4096:
                        raise ValueError()
                    body.extend(chunk)
                registration = Registration.model_validate(
                    json.loads(body, object_pairs_hook=unique_json)
                )
        except Exception:
            raise HTTPException(
                422, "Invalid request", headers={"Cache-Control": "no-store"}
            ) from None
        try:
            registry.register(registration)
        except RoutingUnavailable:
            raise HTTPException(
                409, "Call registration unavailable", headers={"Cache-Control": "no-store"}
            ) from None
        return JSONResponse({"status": "registered"}, headers={"Cache-Control": "no-store"})
