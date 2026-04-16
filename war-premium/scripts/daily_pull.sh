#!/usr/bin/env bash
# War-premium daily automation: full ingest + RND + analysis + figures.
# Usage: ./scripts/daily_pull.sh [YYYY-MM-DD]
# Cron example (UTC after US equity close): 0 21 * * 1-5 cd /path/to/war-premium && ./scripts/daily_pull.sh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATE="${1:-$(date -u +%Y-%m-%d)}"

cd "${PROJECT_ROOT}"

# Skip GDELT unless GOOGLE_APPLICATION_CREDENTIALS is set (avoid failing cron jobs)
SKIP_GDELT=( )
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  SKIP_GDELT=( --skip-gdelt )
fi

exec python -m src.pipeline.daily --date "${DATE}" --project-root "${PROJECT_ROOT}" "${SKIP_GDELT[@]}"
