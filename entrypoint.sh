#!/bin/sh
set -e

# Defaults (can be overridden via env)
: "${TELEGRAM_BOT_TOKEN:=}"
: "${COOKIES_PATH:=/app/telegram_bot/secrets/cookies.txt}"
: "${OUTPUT_ROOT:=/data/downloads}"

mkdir -p "$(dirname "$COOKIES_PATH")" "$OUTPUT_ROOT"

if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
  echo "ERROR: TELEGRAM_BOT_TOKEN is not set"
  exit 1
fi

if [ ! -f "$COOKIES_PATH" ]; then
  echo "WARNING: COOKIES_PATH not found at $COOKIES_PATH — gamdl may fail. Mount your cookies.txt"
fi

# Prefer module entry (provided by your repo)
exec python -m gamdl.telegram_bot.bot
