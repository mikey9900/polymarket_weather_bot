FROM python:3.11-slim

ARG BUILD_ARCH

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN case "${BUILD_ARCH}" in \
      amd64|aarch64) pip install --no-cache-dir "duckdb>=1.1.0" ;; \
      *) echo "Skipping duckdb install for ${BUILD_ARCH}" ;; \
    esac

COPY . /app
RUN chmod +x /app/weather-bot/run.sh

EXPOSE 8099

CMD ["/app/weather-bot/run.sh"]
