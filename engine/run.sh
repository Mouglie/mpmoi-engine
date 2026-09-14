#!/usr/bin/env bash
# Stage 0 spike runner: seed the FK rows the sink needs, then run the spike
# against the desktop dev product Postgres. Scan the QR, send yourself a
# WhatsApp message, and watch it land in public.messages (and the web inbox).
set -euo pipefail

DSN="${MPMOI_PRODUCT_DSN:-postgres://matrix:matrix@127.0.0.1:5433/mpmoi_product?sslmode=disable}"
export MPMOI_PRODUCT_DSN="$DSN"

echo "product DB: $DSN"
psql "$DSN" -c "select 1" >/dev/null || { echo "!! product Postgres not reachable — start the desktop dev stack first"; exit 1; }

# Signed-in user + their default space.
read -r USER_ID SPACE_ID <<<"$(psql "$DSN" -At -F' ' -c \
  "select u.id, s.id from auth.users u
     join public.spaces s on s.user_id = u.id and s.is_default
   order by u.created_at limit 1")"
[ -n "${USER_ID:-}" ] || { echo "!! no signed-in user found — sign in to the app once first"; exit 1; }

# Reuse an existing whatsapp account row, else create one.
ACCOUNT_ID="$(psql "$DSN" -At -c \
  "select id from public.connected_accounts
     where user_id='$USER_ID' and network='whatsapp' order by created_at limit 1")"
if [ -z "$ACCOUNT_ID" ]; then
  ACCOUNT_ID="$(psql "$DSN" -At -c \
    "insert into public.connected_accounts (user_id, space_id, network, external_handle, status)
       values ('$USER_ID','$SPACE_ID','whatsapp','spike','active') returning id")"
fi

export MPMOI_USER_ID="$USER_ID" MPMOI_SPACE_ID="$SPACE_ID" MPMOI_ACCOUNT_ID="$ACCOUNT_ID"
echo "user=$USER_ID space=$SPACE_ID account=$ACCOUNT_ID"
echo "bridge state db: ${MPMOI_BRIDGE_DB:-$TMPDIR/mpmoi-spike-bridge.db}"

cd "$(dirname "$0")"
exec go run . "$@"
