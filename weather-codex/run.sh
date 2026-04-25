#!/usr/bin/env bash
set -e

cd /app

export WEATHER_SHARED_DATA_ROOT=/share/weather_bot

python -m weather_bot.research.runner
