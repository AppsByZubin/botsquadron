#!/usr/bin/env bash
set -Eeuo pipefail

# One-off migration for an already-initialized OMS database.
# It:
# - renames legacy public."Trades" to public.trades
# - adds trades.account_id
# - adds a foreign key from trades.account_id to accounts.id
# - backfills account_id for already-closed trades when the monthly account row exists
#
# Usage:
#   sudo DB_NAME=omsdb bash scripts/migrate_trades_table_and_account_fk.sh

DB_NAME="${DB_NAME:-omsdb}"

if [[ $EUID -ne 0 ]]; then
  echo "Run this script as root or with sudo."
  exit 1
fi

validate_ident() {
  local value="$1"
  local label="$2"
  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "$label must match ^[A-Za-z_][A-Za-z0-9_]*$. Got: $value"
    exit 1
  fi
}

validate_ident "$DB_NAME" "DB_NAME"

# Avoid noisy "could not change directory" warnings from sudo -u postgres.
cd /

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  echo "Database \"${DB_NAME}\" does not exist."
  exit 1
fi

sudo -u postgres psql -v ON_ERROR_STOP=1 -d "$DB_NAME" <<'SQL'
BEGIN;

DO $$
BEGIN
  IF to_regclass('public.accounts') IS NULL THEN
    RAISE EXCEPTION 'public.accounts does not exist. Run the accounts schema setup before this migration.';
  END IF;

  IF to_regclass('public."Trades"') IS NOT NULL AND to_regclass('public.trades') IS NOT NULL THEN
    RAISE EXCEPTION 'Both public."Trades" and public.trades exist. Consolidate them before running this migration.';
  END IF;

  IF to_regclass('public."Trades"') IS NOT NULL THEN
    ALTER TABLE "Trades" RENAME TO trades;
  END IF;

  IF to_regclass('public.trades') IS NULL THEN
    RAISE EXCEPTION 'public.trades does not exist. Nothing to migrate.';
  END IF;
END
$$;

ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_name TEXT;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS account_id UUID;
CREATE INDEX IF NOT EXISTS idx_trades_account_id ON trades (account_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_trades_account_id'
      AND conrelid = to_regclass('public.trades')
  ) THEN
    ALTER TABLE trades
      ADD CONSTRAINT fk_trades_account_id
      FOREIGN KEY (account_id) REFERENCES accounts(id);
  END IF;
END
$$;

UPDATE trades AS t
SET account_id = a.id
FROM accounts AS a
WHERE t.account_id IS NULL
  AND t.exit_time IS NOT NULL
  AND a.year = EXTRACT(YEAR FROM t.exit_time)::int
  AND a.month = EXTRACT(MONTH FROM t.exit_time)::int
  AND a.bot_name = COALESCE(NULLIF(BTRIM(t.bot_name), ''), 'default');

COMMIT;
SQL

cat <<EOF
Done.

Migration applied to database: ${DB_NAME}

Quick checks:
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ trades'
  sudo -u postgres psql -d ${DB_NAME} -c 'SELECT id, account_id, bot_name, exit_time FROM trades ORDER BY "timestamp" DESC LIMIT 10;'
EOF
