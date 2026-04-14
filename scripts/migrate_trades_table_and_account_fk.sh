#!/usr/bin/env bash
set -Eeuo pipefail

# One-off migration for an already-initialized OMS database.
# It moves the schema toward:
# - accounts: daily bot account/PnL rows
# - trades: trade-level metadata and trailing state
# - orders: one row per broker order id
#
# Legacy trade columns are backfilled into the new structure but are not dropped.
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

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
  IF to_regclass('public."Trades"') IS NOT NULL AND to_regclass('public.trades') IS NOT NULL THEN
    RAISE EXCEPTION 'Both public."Trades" and public.trades exist. Consolidate them before running this migration.';
  END IF;

  IF to_regclass('public."Trades"') IS NOT NULL THEN
    ALTER TABLE "Trades" RENAME TO trades;
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  botname TEXT,
  curr_date TEXT,
  month_year TEXT,
  init_cash NUMERIC(18,6),
  net_profit NUMERIC(18,6) DEFAULT 0
);

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS botname TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS curr_date TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS month_year TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS init_cash NUMERIC(18,6);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS net_profit NUMERIC(18,6) DEFAULT 0;
ALTER TABLE accounts DROP COLUMN IF EXISTS max_drawdown;

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
      AND column_name = 'profit'
  ) THEN
    UPDATE accounts
    SET net_profit = COALESCE(net_profit, profit, 0)
    WHERE net_profit IS NULL OR net_profit = 0;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'current_date'
  ) THEN
    UPDATE accounts
    SET curr_date = COALESCE(NULLIF(BTRIM(curr_date), ''), NULLIF(BTRIM(current_date), ''))
    WHERE curr_date IS NULL OR BTRIM(curr_date) = '';
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
        )
    WHERE year IS NOT NULL
      AND month IS NOT NULL
      AND month BETWEEN 1 AND 12
      AND (month_year IS NULL OR BTRIM(month_year) = '');
  END IF;

  UPDATE accounts
  SET net_profit = COALESCE(net_profit, 0);
END
$$;

CREATE INDEX IF NOT EXISTS idx_accounts_bot_curr_date ON accounts (botname, curr_date);
CREATE INDEX IF NOT EXISTS idx_accounts_bot_month_year ON accounts (botname, month_year);

CREATE TABLE IF NOT EXISTS trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  acct_id UUID,
  symbol TEXT,
  instrument_token TEXT,
  side TEXT,
  qty INTEGER,
  product TEXT,
  validity TEXT,
  tsl_active BOOLEAN DEFAULT FALSE,
  start_trail_after NUMERIC(18,6),
  entry_spot NUMERIC(18,6),
  spot_ltp NUMERIC(18,6),
  spot_trail_anchor NUMERIC(18,6),
  trail_points NUMERIC(18,6),
  status TEXT,
  timestamp TIMESTAMPTZ DEFAULT NOW(),
  total_brokerage NUMERIC(18,6) DEFAULT 0,
  tag_entry TEXT,
  tag_sl TEXT,
  description TEXT
);

ALTER TABLE trades ADD COLUMN IF NOT EXISTS acct_id UUID;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS total_brokerage NUMERIC(18,6) DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_trades_acct_id ON trades (acct_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades ("timestamp");
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'account_id'
  ) THEN
    UPDATE trades
    SET acct_id = COALESCE(acct_id, account_id)
    WHERE acct_id IS NULL;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'taxes'
  ) THEN
    UPDATE trades
    SET total_brokerage = COALESCE(total_brokerage, taxes, 0)
    WHERE total_brokerage IS NULL OR total_brokerage = 0;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'bot_name'
  ) THEN
    EXECUTE $sql$
      INSERT INTO accounts (botname, curr_date, month_year, init_cash, net_profit)
      SELECT DISTINCT
        COALESCE(NULLIF(BTRIM(t.bot_name), ''), 'default'),
        TO_CHAR(COALESCE(t."timestamp", NOW()), 'DD-MM-YYYY'),
        TO_CHAR(COALESCE(t."timestamp", NOW()), 'MMYYYY'),
        0,
        0
      FROM trades AS t
      WHERE t.acct_id IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM accounts AS a
          WHERE a.botname = COALESCE(NULLIF(BTRIM(t.bot_name), ''), 'default')
            AND a.curr_date = TO_CHAR(COALESCE(t."timestamp", NOW()), 'DD-MM-YYYY')
        )
    $sql$;

    EXECUTE $sql$
      UPDATE trades AS t
      SET acct_id = a.id
      FROM accounts AS a
      WHERE t.acct_id IS NULL
        AND a.botname = COALESCE(NULLIF(BTRIM(t.bot_name), ''), 'default')
        AND a.curr_date = TO_CHAR(COALESCE(t."timestamp", NOW()), 'DD-MM-YYYY')
    $sql$;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_trades_acct_id'
      AND conrelid = to_regclass('public.trades')
  ) THEN
    ALTER TABLE trades
      ADD CONSTRAINT fk_trades_acct_id
      FOREIGN KEY (acct_id) REFERENCES accounts(id);
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id TEXT,
  trade_id UUID REFERENCES trades(id) ON DELETE CASCADE,
  instrument_token TEXT,
  order_type TEXT,
  entry_price NUMERIC(18,6),
  target NUMERIC(18,6),
  stoploss NUMERIC(18,6),
  sl_limit NUMERIC(18,6),
  exit_price NUMERIC(18,6),
  pnl NUMERIC(18,6),
  exit_time TIMESTAMPTZ,
  brokerage NUMERIC(18,6)
);

CREATE INDEX IF NOT EXISTS idx_orders_trade_id ON orders (trade_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_trade_type ON orders (trade_id, order_type);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'entry_order_ids'
  ) THEN
    EXECUTE $sql$
      INSERT INTO orders (order_id, trade_id, instrument_token, order_type, entry_price, target)
      SELECT NULLIF(entry_id.value, ''), t.id, t.instrument_token, 'entry', t.entry_price, t.target
      FROM trades AS t
      CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(t.entry_order_ids, '[]'::jsonb)) AS entry_id(value)
      WHERE NOT EXISTS (
        SELECT 1
        FROM orders AS o
        WHERE o.trade_id = t.id
          AND lower(COALESCE(o.order_type, '')) = 'entry'
          AND COALESCE(o.order_id, '') = COALESCE(NULLIF(entry_id.value, ''), '')
      )
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'sl_order_ids'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'trades'
        AND column_name = 'taxes'
    ) THEN
      EXECUTE $sql$
        INSERT INTO orders (order_id, trade_id, instrument_token, order_type, stoploss, sl_limit, exit_price, pnl, exit_time, brokerage)
        SELECT NULLIF(sl_id.value, ''), t.id, t.instrument_token, 'sl', t.stoploss, t.sl_limit, t.exit_price, t.pnl, t.exit_time, t.taxes
        FROM trades AS t
        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(t.sl_order_ids, '[]'::jsonb)) AS sl_id(value)
        WHERE NOT EXISTS (
          SELECT 1
          FROM orders AS o
          WHERE o.trade_id = t.id
            AND lower(COALESCE(o.order_type, '')) = 'sl'
            AND COALESCE(o.order_id, '') = COALESCE(NULLIF(sl_id.value, ''), '')
        )
      $sql$;
    ELSE
      EXECUTE $sql$
        INSERT INTO orders (order_id, trade_id, instrument_token, order_type, stoploss, sl_limit, exit_price, pnl, exit_time, brokerage)
        SELECT NULLIF(sl_id.value, ''), t.id, t.instrument_token, 'sl', t.stoploss, t.sl_limit, t.exit_price, t.pnl, t.exit_time, NULL
        FROM trades AS t
        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(t.sl_order_ids, '[]'::jsonb)) AS sl_id(value)
        WHERE NOT EXISTS (
          SELECT 1
          FROM orders AS o
          WHERE o.trade_id = t.id
            AND lower(COALESCE(o.order_type, '')) = 'sl'
            AND COALESCE(o.order_id, '') = COALESCE(NULLIF(sl_id.value, ''), '')
        )
      $sql$;
    END IF;
  END IF;
END
$$;

COMMIT;
SQL

cat <<EOF
Done.

Migration applied to database: ${DB_NAME}

Quick checks:
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ accounts'
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ trades'
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ orders'
  sudo -u postgres psql -d ${DB_NAME} -c 'SELECT trade_id, order_type, order_id FROM orders ORDER BY id DESC LIMIT 10;'
EOF
