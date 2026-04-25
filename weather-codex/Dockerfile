FROM python:3.11-slim

ARG BUILD_ARCH

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN case "${BUILD_ARCH}" in \
      amd64|aarch64) pip install --no-cache-dir "duckdb>=1.1.0" ;; \
      *) echo "Skipping duckdb install for ${BUILD_ARCH}" ;; \
    esac

ADD https://github.com/mikey9900/polymarket_weather_bot/archive/refs/heads/main.tar.gz /tmp/polymarket_weather_bot.tar.gz
RUN python -c "import tarfile; tarfile.open('/tmp/polymarket_weather_bot.tar.gz').extractall('/tmp')" \
 && cp -a /tmp/polymarket_weather_bot-main/. /app/ \
 && rm -rf /tmp/polymarket_weather_bot-main /tmp/polymarket_weather_bot.tar.gz \
 && chmod +x /app/weather-codex/run.sh

CMD ["/app/weather-codex/run.sh"]
