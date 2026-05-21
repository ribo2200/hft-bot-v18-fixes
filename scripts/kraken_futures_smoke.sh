#!/usr/bin/env bash
set -euo pipefail

# Kraken Futures private REST smoke test (direct against API)
# Requires: curl, openssl, base64, python3
# Env:
#   KRAKEN_API_KEY
#   KRAKEN_API_SECRET   (base64)
# Optional:
#   KRAKEN_FUTURES_BASE (default: https://futures.kraken.com)
#   DRY_RUN=1           (skip private mutating endpoints)

BASE_URL="${KRAKEN_FUTURES_BASE:-https://futures.kraken.com}"
API_KEY="${KRAKEN_API_KEY:-}"
API_SECRET_B64="${KRAKEN_API_SECRET:-}"
DRY_RUN="${DRY_RUN:-0}"

if [[ -z "$API_KEY" || -z "$API_SECRET_B64" ]]; then
  echo "ERROR: set KRAKEN_API_KEY and KRAKEN_API_SECRET" >&2
  exit 1
fi

nonce_ms() { date +%s%3N; }

urlencode_value() {
  python3 - <<'PY' "$1"
import sys, urllib.parse
print(urllib.parse.quote(sys.argv[1], safe='-_.~'))
PY
}

# Encodes only values in x-www-form-urlencoded body (key=value&key2=value2)
encode_body_values() {
  local body="$1"
  local out=""
  IFS='&' read -ra parts <<< "$body"
  for kv in "${parts[@]}"; do
    local k="${kv%%=*}"
    local v="${kv#*=}"
    local ev
    ev="$(urlencode_value "$v")"
    if [[ -n "$out" ]]; then out+="&"; fi
    out+="${k}=${ev}"
  done
  printf '%s' "$out"
}

# Kraken Futures Authent = base64(hmac_sha512( sha256(postData + nonce + endpointPath), base64_decode(secret) ))
authent_header() {
  local encoded_body="$1" nonce="$2" endpoint_path="$3"
  local ep="$endpoint_path"
  ep="${ep#/derivatives}"
  local msg="${encoded_body}${nonce}${ep}"

  local sha_hex
  sha_hex="$(printf '%s' "$msg" | openssl dgst -sha256 -binary | xxd -p -c 256)"

  local key_hex
  key_hex="$(printf '%s' "$API_SECRET_B64" | base64 -d | xxd -p -c 256)"

  printf '%s' "$sha_hex" \
    | xxd -r -p \
    | openssl dgst -sha512 -mac HMAC -macopt hexkey:"$key_hex" -binary \
    | base64
}

kraken_get() {
  local path="$1"
  local nonce
  nonce="$(nonce_ms)"
  local authent
  authent="$(authent_header "" "$nonce" "$path")"

  curl -sS "$BASE_URL$path" \
    -H "APIKey: $API_KEY" \
    -H "Nonce: $nonce" \
    -H "Authent: $authent" \
    -H "User-Agent: HFT-MM/smoke" \
    -H 'Connection: keep-alive'
}

kraken_post() {
  local path="$1" body_raw="$2"
  local body_encoded
  body_encoded="$(encode_body_values "$body_raw")"
  local nonce
  nonce="$(nonce_ms)"
  local authent
  authent="$(authent_header "$body_encoded" "$nonce" "$path")"

  curl -sS "$BASE_URL$path" \
    -X POST \
    -H "APIKey: $API_KEY" \
    -H "Nonce: $nonce" \
    -H "Authent: $authent" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H "User-Agent: HFT-MM/smoke" \
    --data "$body_encoded"
}

echo "[1/5] GET /derivatives/api/v3/accounts"
kraken_get "/derivatives/api/v3/accounts" | tee /tmp/kraken_accounts.json

echo "[2/5] GET /derivatives/api/v3/leveragepreferences"
kraken_get "/derivatives/api/v3/leveragepreferences" | tee /tmp/kraken_leverage.json

echo "[3/5] POST /derivatives/api/v3/cancelallordersafter timeout=30"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "DRY_RUN=1 -> skipped"
else
  kraken_post "/derivatives/api/v3/cancelallordersafter" "timeout=30" | tee /tmp/kraken_dms.json
fi

echo "[4/5] POST /derivatives/api/v3/sendorder (post-only far price, tiny size)"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "DRY_RUN=1 -> skipped"
else
  # Warning: adjust symbol/price/size according to enabled market and min size.
  # This example aims to avoid fills by placing far from market.
  TEST_SYMBOL="${TEST_SYMBOL:-PF_SOLUSD}"
  TEST_PRICE="${TEST_PRICE:-1.00}"
  TEST_SIZE="${TEST_SIZE:-0.01}"
  TEST_OID="smoke_$(date +%s)"
  SEND_RESP="$(kraken_post "/derivatives/api/v3/sendorder" "orderType=lmt&symbol=${TEST_SYMBOL}&side=buy&size=${TEST_SIZE}&limitPrice=${TEST_PRICE}&postOnly=true&cliOrdId=${TEST_OID}")"
  echo "$SEND_RESP" | tee /tmp/kraken_sendorder.json

  echo "[5/5] POST /derivatives/api/v3/cancelorder cliOrdId=$TEST_OID"
  kraken_post "/derivatives/api/v3/cancelorder" "cliOrdId=${TEST_OID}" | tee /tmp/kraken_cancel.json
fi

echo "Done. Raw responses in /tmp/kraken_*.json"
