FROM python:3.12-slim-bookworm

ARG TALIB_VERSION=0.6.4

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/bots/haemabot \
    TZ=Asia/Kolkata

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        libgomp1 \
        tzdata \
    && curl -fsSL "https://github.com/TA-Lib/ta-lib/releases/download/v${TALIB_VERSION}/ta-lib-${TALIB_VERSION}-src.tar.gz" -o /tmp/ta-lib.tar.gz \
    && tar -xzf /tmp/ta-lib.tar.gz -C /tmp \
    && cd "/tmp/ta-lib-${TALIB_VERSION}" \
    && ./configure --prefix=/usr \
    && (make -j"$(nproc)" || make) \
    && make install \
    && ldconfig

COPY bots/haemabot/requirements.txt /tmp/haemabot-requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r /tmp/haemabot-requirements.txt \
    && apt-get purge -y --auto-remove build-essential curl \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN groupadd --system app \
    && useradd --system --gid app --home-dir /app --create-home app

COPY --chown=app:app bots/haemabot ./bots/haemabot
COPY --chown=app:app bots/haemabot/files ./files

RUN mkdir -p \
        logs \
        files/execution_results/mock \
        files/execution_results/sandbox \
        files/execution_results/prod \
    && chown -R app:app /app

USER app

ENTRYPOINT ["python", "bots/haemabot/main.py"]
CMD ["--instruments", "nifty50", "--strategy", "hm_ema_adx", "--level", "mock"]
