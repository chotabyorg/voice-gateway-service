FROM python:3.11.17-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89

ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/chotabyorg/voice-gateway-service" \
      org.opencontainers.image.revision=$VCS_REF

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 vgservice \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin vgservice

COPY voice_gateway ./voice_gateway
USER 10001:10001

EXPOSE 8000/tcp 55585/tcp
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=1)"

ENTRYPOINT ["python", "-m", "voice_gateway"]
