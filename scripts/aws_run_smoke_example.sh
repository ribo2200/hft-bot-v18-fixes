#!/usr/bin/env bash
set -euo pipefail

# Example for EC2/AWS Linux host
# 1) export credentials in current shell (or use AWS SSM/Secrets Manager injection)
# 2) run smoke script and keep JSON responses

# export KRAKEN_API_KEY="..."
# export KRAKEN_API_SECRET="..."   # base64 secret from Kraken Futures
# export DRY_RUN=1

./scripts/kraken_futures_smoke.sh
