# Base image
FROM python:3.12-slim

# Environment
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Asia/Yangon

WORKDIR /app

# System packages: ffmpeg (remux), gpac -> MP4Box (optional remux), curl+unzip (for mp4decrypt)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      gpac \
      ca-certificates \
      curl \
      unzip && \
    rm -rf /var/lib/apt/lists/*

# Install mp4decrypt (Bento4) for DRM (music videos, non-legacy codecs)
# Support linux/amd64 and linux/arm64
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64|amd64)  BENTO_PKG="Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip" ;; \
      aarch64|arm64) BENTO_PKG="Bento4-SDK-1-6-0-641.aarch64-unknown-linux.zip" ;; \
      *) echo "Unsupported arch: $arch" && exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/bento4.zip "https://github.com/axiomatic-systems/Bento4/releases/download/v1.6.0-641/${BENTO_PKG}" && \
    mkdir -p /opt/bento4 && \
    unzip -q /tmp/bento4.zip -d /opt && \
    mv /opt/Bento4*/* /opt/bento4/ && rmdir /opt/Bento4* || true && \
    ln -sf /opt/bento4/bin/mp4decrypt /usr/local/bin/mp4decrypt && \
    rm -f /tmp/bento4.zip

# Pre-create runtime dirs
RUN mkdir -p /data/downloads /app/telegram_bot/secrets /data/logs
ENV OUTPUT_ROOT=/data/downloads \
    RUN_LOG_DIR=/data/logs

# Copy sources first (leverage layer cache by copying manifest files first if you want)
COPY . /app

# Python deps: install local repo (this one) + bot deps
# Note: DON'T install PyPI gamdl over local package to avoid override.
RUN pip install --upgrade pip && \
    if [ -f "pyproject.toml" ]; then pip install --no-cache-dir . ; fi && \
    if [ -f "requirements.txt" ]; then pip install --no-cache-dir -r requirements.txt ; fi && \
    pip install --no-cache-dir \
      "python-telegram-bot>=20.7,<21" \
      "python-dotenv>=1.0.1" \
      "mutagen>=1.47.0" \
      "requests>=2.31.0"

# Show installed gamdl version (from local repo)
RUN gamdl --version || true

# Entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Healthcheck (lightweight)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s CMD python -c "import sys; sys.exit(0)"

ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
