#!/usr/bin/env bash
# =============================================================================
# run_scraper.sh
# Entry point for Phase 1 — scrape Wuzzuf and save raw JSON to data/raw/
# Usage: bash run_scraper.sh [KEYWORD] [MAX_PAGES]
# Example: bash run_scraper.sh "data engineer" 5
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
RAW_DIR="$SCRIPT_DIR/data/raw"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/scraper_$TIMESTAMP.log"

KEYWORD="${1:-data engineer}"
MAX_PAGES="${2:-5}"

# ── Colors for terminal output ────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo -e "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
success() { log "${GREEN}✔ $1${NC}"; }
warn()    { log "${YELLOW}⚠ $1${NC}"; }
error()   { log "${RED}✘ $1${NC}"; exit 1; }

# ── Pre-flight checks ─────────────────────────────────────────────────────────
log "Starting Wuzzuf scraper pipeline"
log "Keyword: '$KEYWORD' | Max pages: $MAX_PAGES"

mkdir -p "$LOG_DIR" "$RAW_DIR"

command -v python3 &>/dev/null || error "python3 not found. Please install Python 3.8+"

# Check required packages
python3 -c "import requests, bs4" 2>/dev/null \
  || error "Missing packages. Run: pip install -r requirements.txt"

# ── Run scraper ───────────────────────────────────────────────────────────────
success "Dependencies OK — launching scraper"

python3 "$SCRIPT_DIR/scraper/wuzzuf_scraper.py" \
  --keyword "$KEYWORD" \
  --max-pages "$MAX_PAGES" \
  --output-dir "$RAW_DIR" \
  2>&1 | tee -a "$LOG_FILE"

# ── Summary ───────────────────────────────────────────────────────────────────
FILE_COUNT=$(find "$RAW_DIR" -name "*.json" -newer "$LOG_FILE" | wc -l)
success "Done! $FILE_COUNT new JSON file(s) saved to $RAW_DIR"
log "Full log: $LOG_FILE"
