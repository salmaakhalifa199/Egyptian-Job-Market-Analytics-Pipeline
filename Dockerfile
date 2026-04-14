# Custom Airflow image with Playwright + project dependencies
FROM apache/airflow:2.10.4

# ── System deps for Playwright (as root) ─────────────────────────────────────
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# ── Python packages (as airflow user) ────────────────────────────────────────
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── Install Playwright + Chromium browser ─────────────────────────────────────
RUN pip install --no-cache-dir playwright && \
    playwright install chromium
