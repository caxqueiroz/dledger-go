#!/usr/bin/env bash
#
# dledger-go quickstart via curl + the Connect JSON codec.
#
# Prereqs:
#   - The server is running at http://localhost:8080
#   - jq is installed
#
# Usage:
#   ./examples/curl/quickstart.sh
set -euo pipefail

URL="${URL:-http://localhost:8080}"
TENANT="${TENANT:-t1}"

post() {
  curl -fsS -X POST "$URL/ledger.v1.LedgerService/$1" \
    -H 'Content-Type: application/json' \
    -H "X-Tenant-Id: $TENANT" \
    -d "$2"
}

# Funding source account (credit-normal, allow negative)
SRC=$(post CreateAccount '{
  "tenant_id":"'"$TENANT"'","owner_type":"platform","owner_id":"0",
  "account_type":"source","currency":"USD",
  "normal_balance":"NORMAL_BALANCE_CREDIT","allow_negative":true
}' | jq -r '.account.id')
echo "src=$SRC"

# User's cash available
AVAIL=$(post CreateAccount '{
  "tenant_id":"'"$TENANT"'","owner_type":"user","owner_id":"1",
  "account_type":"cash_available","currency":"USD",
  "normal_balance":"NORMAL_BALANCE_DEBIT"
}' | jq -r '.account.id')
echo "avail=$AVAIL"

# User's cash reserved
RESV=$(post CreateAccount '{
  "tenant_id":"'"$TENANT"'","owner_type":"user","owner_id":"1",
  "account_type":"cash_reserved","currency":"USD",
  "normal_balance":"NORMAL_BALANCE_DEBIT"
}' | jq -r '.account.id')
echo "resv=$RESV"

# Seed 500 USD
post PostJournal '{
  "tenant_id":"'"$TENANT"'","idempotency_key":"seed-1","source_service":"curl",
  "journal":{"event_id":"seed-1","entries":[
    {"account_id":"'"$AVAIL"'","currency":"USD","direction":"DIRECTION_DEBIT","amount":"500"},
    {"account_id":"'"$SRC"'","currency":"USD","direction":"DIRECTION_CREDIT","amount":"500"}
  ]}
}' > /dev/null
echo "seeded 500 USD"

# Place an order: ExecuteFlow PLACE_ORDER, reserves 100 USD
post ExecuteFlow '{
  "tenant_id":"'"$TENANT"'","flow_type":"PLACE_ORDER","idempotency_key":"ord-1",
  "source_service":"curl",
  "steps":[{"step_id":"reserve","journal":{
    "event_id":"ord-1-reserve","entries":[
      {"account_id":"'"$RESV"'","currency":"USD","direction":"DIRECTION_DEBIT","amount":"100"},
      {"account_id":"'"$AVAIL"'","currency":"USD","direction":"DIRECTION_CREDIT","amount":"100"}
    ]
  }}]
}' | jq '{flow_run_id, status}'

# Balances
echo "available:"
post GetBalance "{\"tenant_id\":\"$TENANT\",\"account_id\":\"$AVAIL\",\"currency\":\"USD\"}" | jq '.balance | {normalized, version}'
echo "reserved:"
post GetBalance "{\"tenant_id\":\"$TENANT\",\"account_id\":\"$RESV\",\"currency\":\"USD\"}" | jq '.balance | {normalized, version}'

# Snapshot the whole tenant
echo "snapshot:"
post TakeBalanceSnapshot "{\"tenant_id\":\"$TENANT\"}" | jq

# Create a reservation expiring in 1 hour
EXPIRES=$(python3 -c 'from datetime import datetime, timezone, timedelta; print((datetime.now(timezone.utc)+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))' 2>/dev/null \
  || date -u -v+1H +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
  || date -u -d '+1 hour' +"%Y-%m-%dT%H:%M:%SZ")
RES=$(post CreateReservation "{
  \"tenant_id\":\"$TENANT\",\"idempotency_key\":\"res-1\",
  \"source_account_id\":\"$AVAIL\",\"reserved_account_id\":\"$RESV\",
  \"currency\":\"USD\",\"amount\":\"50\",\"expires_at\":\"$EXPIRES\",
  \"source_service\":\"curl\"
}")
RESID=$(echo "$RES" | jq -r '.reservation.id')
echo "reservation:"
echo "$RES" | jq '.reservation | {id, status, outstanding_amount, expires_at}'

# Inspect via GetReservation
echo "get reservation:"
post GetReservation "{\"tenant_id\":\"$TENANT\",\"reservation_id\":\"$RESID\"}" | jq '.reservation | {id, status, outstanding_amount}'
