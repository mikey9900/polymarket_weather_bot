FROM python:3.11-slim

ARG BUILD_ARCH

WORKDIR /app

COPY . /app

RUN if [ -f /app/weather-bot/requirements.txt ]; then \
      pip install --no-cache-dir -r /app/weather-bot/requirements.txt; \
    else \
      pip install --no-cache-dir -r /app/requirements.txt; \
    fi
RUN case "${BUILD_ARCH}" in \
      amd64|aarch64) pip install --no-cache-dir "duckdb>=1.1.0" ;; \
      *) echo "Skipping duckdb install for ${BUILD_ARCH}" ;; \
    esac

RUN if [ ! -d /app/weather_bot ]; then \
      python -c "import tarfile, urllib.request; urllib.request.urlretrieve('https://github.com/mikey9900/polymarket_weather_bot/archive/refs/heads/main.tar.gz', '/tmp/polymarket_weather_bot.tar.gz'); tarfile.open('/tmp/polymarket_weather_bot.tar.gz').extractall('/tmp')" \
      && cp -a /tmp/polymarket_weather_bot-main/. /app/ \
      && rm -rf /tmp/polymarket_weather_bot-main /tmp/polymarket_weather_bot.tar.gz; \
    fi \
 && chmod +x /app/weather-bot/run.sh

EXPOSE 8099

CMD ["/app/weather-bot/run.sh"]
