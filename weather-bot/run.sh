#!/usr/bin/env bash
set -e

cd /app

export WEATHER_SHARED_DATA_ROOT=/data

if [ -f /config/weather_bot.env ]; then
  cp /config/weather_bot.env /app/.env
fi

python -u -m weather_bot.main
