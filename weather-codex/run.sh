#!/usr/bin/env bash
set -e

cd /app

export WEATHER_SHARED_DATA_ROOT=/data

python -m weather_bot.research.runner
