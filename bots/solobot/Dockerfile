FROM python:3.12-slim-bookworm

ARG TALIB_VERSION=0.6.4

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/bots/solobot \
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

COPY bots/solobot/requirements.txt /tmp/solobot-requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r /tmp/solobot-requirements.txt \
    && apt-get purge -y --auto-remove build-essential curl \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN groupadd --system app \
    && useradd --system --gid app --home-dir /app --create-home app

COPY --chown=app:app bots/solobot ./bots/solobot

RUN mkdir -p \
        logs \
        bots/solobot/files/execution_results/mock \
        bots/solobot/files/execution_results/sandbox \
        bots/solobot/files/execution_results/prod \
    && chown -R app:app /app

USER app

ENTRYPOINT ["python", "bots/solobot/main.py"]
CMD ["--instruments", "nifty50", "--strategy", "pcr_vwap_ema_orb", "--level", "mock"]
