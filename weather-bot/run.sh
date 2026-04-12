#!/usr/bin/env bash
set -e

REPO="https://github.com/mikey9900/polymarket_weather_bot.git"
BOT_DIR="/app/bot"

if [ -d "$BOT_DIR/.git" ]; then
    echo "Pulling latest changes from GitHub..."
    git -C "$BOT_DIR" pull
else
    echo "Cloning repo for the first time..."
    git clone "$REPO" "$BOT_DIR"
fi

cd "$BOT_DIR"

echo "Installing dependencies..."
pip install --quiet requests python-dotenv

echo "Loading secrets..."
cp /config/weather_bot.env .env

echo "Starting bot..."
python -u telegram_command_listener.py 2>&1
echo "Bot exited with code $?"
