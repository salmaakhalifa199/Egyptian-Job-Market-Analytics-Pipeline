# Dockerfile
# Astro Runtime image — replaces your current docker-compose Airflow setup.
# Astro Runtime is a hardened Airflow image maintained by Astronomer.

FROM quay.io/astronomer/astro-runtime:11.3.0

# ── System deps for Playwright ───────────────────────────────────────────────
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*
USER astro

# ── Install Playwright browsers ───────────────────────────────────────────────
RUN pip install --no-cache-dir playwright && \
    playwright install chromium

# ── Python path — project root is mounted via docker-compose.override.yml ────
ENV PYTHONPATH="/usr/local/airflow/project:${PYTHONPATH}"
