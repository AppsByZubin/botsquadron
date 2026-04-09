#!/usr/bin/env bash
set -Eeuo pipefail

# One-off migration for an already-initialized OMS database.
# It:
# - renames legacy public."Trades" to public.trades
# - adds trades.account_id
# - adds a foreign key from trades.account_id to accounts.id
# - aligns accounts to the daily snapshot schema used by OMS
# - backfills account_id for already-closed trades when the matching daily account row exists
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

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS botname TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS month_year TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS init_cash NUMERIC(18,6);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS current_date TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS profit NUMERIC(18,6);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS max_drawdown NUMERIC(18,6);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'bot_name'
  ) THEN
    UPDATE accounts
    SET botname = COALESCE(NULLIF(BTRIM(botname), ''), NULLIF(BTRIM(bot_name), ''))
    WHERE botname IS NULL OR BTRIM(botname) = '';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'initial_cash'
  ) THEN
    UPDATE accounts
    SET init_cash = COALESCE(init_cash, initial_cash)
    WHERE init_cash IS NULL;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'max_dradown'
  ) THEN
    UPDATE accounts
    SET max_drawdown = COALESCE(
      max_drawdown,
      CASE
        WHEN max_dradown > 0 THEN -ABS(max_dradown)
        ELSE max_dradown
      END
    )
    WHERE max_drawdown IS NULL;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'year'
  ) AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'month'
  ) THEN
    UPDATE accounts
    SET month_year = COALESCE(
          NULLIF(BTRIM(month_year), ''),
          LPAD(month::text, 2, '0') || year::text
        ),
        current_date = COALESCE(
          NULLIF(BTRIM(current_date), ''),
          TO_CHAR(MAKE_DATE(year, month, 1), 'DD-MM-YYYY')
        )
    WHERE year IS NOT NULL
      AND month IS NOT NULL
      AND month BETWEEN 1 AND 12
      AND (
        month_year IS NULL OR BTRIM(month_year) = ''
        OR current_date IS NULL OR BTRIM(current_date) = ''
      );
  END IF;

  UPDATE accounts
  SET profit = COALESCE(profit, 0),
      max_drawdown = COALESCE(max_drawdown, 0);
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_bot_month_date ON accounts (botname, month_year, current_date);
CREATE INDEX IF NOT EXISTS idx_accounts_bot_month_year ON accounts (botname, month_year);

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
  AND a.month_year = TO_CHAR(t.exit_time, 'MMYYYY')
  AND a.current_date = TO_CHAR(t.exit_time, 'DD-MM-YYYY')
  AND a.botname = COALESCE(NULLIF(BTRIM(t.bot_name), ''), 'default');

COMMIT;
SQL

cat <<EOF
Done.

Migration applied to database: ${DB_NAME}

Quick checks:
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ trades'
  sudo -u postgres psql -d ${DB_NAME} -c 'SELECT id, account_id, bot_name, exit_time FROM trades ORDER BY "timestamp" DESC LIMIT 10;'
EOF
