package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	tradesTableName = "trades"
	ordersTableName = "orders"
)

type Store struct {
	pool               *pgxpool.Pool
	tradesTable        string
	loc                *time.Location
	accountInitialCash float64
}

type CreateTradeParams struct {
	AccountID       string
	Symbol          string
	InstrumentToken string
	Side            string
	Qty             int
	Product         string
	Validity        string
	TSLActive       bool
	StartTrailAfter *float64
	EntrySpot       *float64
	SpotLTP         *float64
	SpotTrailAnchor *float64
	TrailPoints     *float64
	Status          string
	TotalBrokerage  *float64
	TagEntry        string
	TagSL           string
	Description     string
	Orders          []CreateOrderParams
}

type CreateOrderParams struct {
	OrderID         string
	InstrumentToken string
	OrderType       string
	Qty             *int
	EntryPrice      *float64
	Target          *float64
	Stoploss        *float64
	SLLimit         *float64
	ExitPrice       *float64
	PNL             *float64
	ExitTime        *time.Time
	Brokerage       *float64
}

type CreateAccountParams struct {
	BotName     string
	CurrDate    string
	MonthYear   string
	InitCash    *float64
	CurrentTime time.Time
}

func New(pool *pgxpool.Pool, timezone string, accountInitialCash float64) (*Store, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("load timezone %q: %w", timezone, err)
	}
	return &Store{
		pool:               pool,
		loc:                loc,
		accountInitialCash: accountInitialCash,
	}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("ensure pgcrypto extension: %w", err)
	}

	if err := s.normalizeTradesTable(ctx); err != nil {
		return err
	}
	if err := s.ensureAccountsSchema(ctx); err != nil {
		return err
	}
	if err := s.detectTradesTable(ctx); err != nil {
		return err
	}
	if err := s.ensureTradesSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureAccountUniqueness(ctx); err != nil {
		return err
	}
	if err := s.ensureOrdersSchema(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Store) ensureAccountsSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
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
CREATE INDEX IF NOT EXISTS idx_accounts_bot_curr_date ON accounts (botname, curr_date);
CREATE INDEX IF NOT EXISTS idx_accounts_bot_month_year ON accounts (botname, month_year);
`); err != nil {
		return fmt.Errorf("ensure accounts table: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `
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
$$;`); err != nil {
		return fmt.Errorf("backfill accounts columns: %w", err)
	}

	return nil
}

func (s *Store) ensureAccountUniqueness(ctx context.Context) error {
	if strings.TrimSpace(s.tradesTable) == "" {
		return fmt.Errorf("trades table is not detected")
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
WITH ranked AS (
    SELECT
        id,
        FIRST_VALUE(id) OVER account_key AS keeper_id,
        ROW_NUMBER() OVER account_key AS row_num
    FROM accounts
    WHERE NULLIF(BTRIM(botname), '') IS NOT NULL
      AND NULLIF(BTRIM(curr_date), '') IS NOT NULL
    WINDOW account_key AS (
        PARTITION BY botname, curr_date
        ORDER BY id::text
    )
),
grouped AS (
    SELECT
        keeper_id,
        SUM(COALESCE(a.net_profit, 0)) AS net_profit,
        MAX(CASE WHEN COALESCE(a.init_cash, 0) > 0 THEN a.init_cash ELSE NULL END) AS init_cash
    FROM ranked AS r
    JOIN accounts AS a ON a.id = r.id
    GROUP BY keeper_id
)
UPDATE accounts AS a
SET
    net_profit = COALESCE(grouped.net_profit, 0),
    init_cash = CASE
        WHEN COALESCE(a.init_cash, 0) > 0 THEN a.init_cash
        WHEN grouped.init_cash IS NOT NULL THEN grouped.init_cash
        ELSE a.init_cash
    END
FROM grouped
WHERE a.id = grouped.keeper_id;

WITH ranked AS (
    SELECT
        id,
        FIRST_VALUE(id) OVER account_key AS keeper_id,
        ROW_NUMBER() OVER account_key AS row_num
    FROM accounts
    WHERE NULLIF(BTRIM(botname), '') IS NOT NULL
      AND NULLIF(BTRIM(curr_date), '') IS NOT NULL
    WINDOW account_key AS (
        PARTITION BY botname, curr_date
        ORDER BY id::text
    )
)
UPDATE %s AS t
SET acct_id = ranked.keeper_id
FROM ranked
WHERE ranked.row_num > 1
  AND t.acct_id = ranked.id;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER account_key AS row_num
    FROM accounts
    WHERE NULLIF(BTRIM(botname), '') IS NOT NULL
      AND NULLIF(BTRIM(curr_date), '') IS NOT NULL
    WINDOW account_key AS (
        PARTITION BY botname, curr_date
        ORDER BY id::text
    )
)
DELETE FROM accounts AS a
USING ranked
WHERE a.id = ranked.id
  AND ranked.row_num > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_bot_curr_date
ON accounts (botname, curr_date)
WHERE botname IS NOT NULL
  AND BTRIM(botname) <> ''
  AND curr_date IS NOT NULL
  AND BTRIM(curr_date) <> '';
`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure account uniqueness: %w", err)
	}

	return nil
}

func (s *Store) ensureTradesSchema(ctx context.Context) error {
	if s.tradesTable == "" {
		s.tradesTable = tradesTableName
		if _, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    acct_id UUID REFERENCES accounts(id),
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
`); err != nil {
			return fmt.Errorf("create trades table: %w", err)
		}
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
ALTER TABLE %s ADD COLUMN IF NOT EXISTS acct_id UUID;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS symbol TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS instrument_token TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS side TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS qty INTEGER;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS product TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS validity TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS tsl_active BOOLEAN DEFAULT FALSE;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS start_trail_after NUMERIC(18,6);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS entry_spot NUMERIC(18,6);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS spot_ltp NUMERIC(18,6);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS spot_trail_anchor NUMERIC(18,6);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS trail_points NUMERIC(18,6);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE %s ADD COLUMN IF NOT EXISTS total_brokerage NUMERIC(18,6) DEFAULT 0;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS tag_entry TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS tag_sl TEXT;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS description TEXT;
CREATE INDEX IF NOT EXISTS idx_trades_acct_id ON %s (acct_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON %s ("timestamp");
CREATE INDEX IF NOT EXISTS idx_trades_status ON %s (status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON %s (symbol);
`, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable,
		s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable,
		s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable,
		s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable,
		s.tradesTable, s.tradesTable, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades columns: %w", err)
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = '%s'
          AND column_name = 'account_id'
    ) THEN
        UPDATE %s
        SET acct_id = COALESCE(acct_id, account_id)
        WHERE acct_id IS NULL;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = '%s'
          AND column_name = 'taxes'
    ) THEN
        UPDATE %s
        SET total_brokerage = COALESCE(total_brokerage, taxes, 0)
        WHERE total_brokerage IS NULL OR total_brokerage = 0;
    END IF;
END
$$;`, s.tradesTable, s.tradesTable, s.tradesTable, s.tradesTable)); err != nil {
		return fmt.Errorf("backfill trades columns: %w", err)
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_trades_acct_id'
          AND conrelid = to_regclass('public.%s')
    ) THEN
        ALTER TABLE %s
        ADD CONSTRAINT fk_trades_acct_id
        FOREIGN KEY (acct_id) REFERENCES accounts(id);
    END IF;
END
$$;`, s.tradesTable, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.acct_id foreign key: %w", err)
	}

	return nil
}

func (s *Store) ensureOrdersSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id TEXT,
    trade_id UUID REFERENCES trades(id) ON DELETE CASCADE,
    instrument_token TEXT,
    order_type TEXT,
    qty INTEGER,
    entry_price NUMERIC(18,6),
    target NUMERIC(18,6),
    stoploss NUMERIC(18,6),
    sl_limit NUMERIC(18,6),
    exit_price NUMERIC(18,6),
    pnl NUMERIC(18,6),
    exit_time TIMESTAMPTZ,
    brokerage NUMERIC(18,6)
);
CREATE INDEX IF NOT EXISTS idx_orders_trade_id ON %s (trade_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON %s (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_trade_type ON %s (trade_id, order_type);
`, ordersTableName, ordersTableName, ordersTableName, ordersTableName)); err != nil {
		return fmt.Errorf("ensure orders table: %w", err)
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS qty INTEGER`, ordersTableName)); err != nil {
		return fmt.Errorf("ensure orders qty column: %w", err)
	}

	hasEntryOrderIDs, err := s.columnExists(ctx, s.tradesTable, "entry_order_ids")
	if err != nil {
		return err
	}
	if hasEntryOrderIDs {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (order_id, trade_id, instrument_token, order_type, entry_price, target, brokerage)
SELECT NULLIF(entry_id.value, ''), t.id, t.instrument_token, 'entry', t.entry_price, t.target, NULL
FROM %s AS t
CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(t.entry_order_ids, '[]'::jsonb)) AS entry_id(value)
WHERE NOT EXISTS (
    SELECT 1 FROM %s AS o
    WHERE o.trade_id = t.id
      AND lower(COALESCE(o.order_type, '')) = 'entry'
      AND COALESCE(o.order_id, '') = COALESCE(NULLIF(entry_id.value, ''), '')
)`, ordersTableName, s.tradesTable, ordersTableName)); err != nil {
			return fmt.Errorf("backfill entry orders from legacy trade columns: %w", err)
		}
	}

	hasSLOrderIDs, err := s.columnExists(ctx, s.tradesTable, "sl_order_ids")
	if err != nil {
		return err
	}
	if hasSLOrderIDs {
		hasTaxes, err := s.columnExists(ctx, s.tradesTable, "taxes")
		if err != nil {
			return err
		}
		brokerageExpr := "NULL"
		if hasTaxes {
			brokerageExpr = "t.taxes"
		}
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (order_id, trade_id, instrument_token, order_type, stoploss, sl_limit, exit_price, pnl, exit_time, brokerage)
SELECT NULLIF(sl_id.value, ''), t.id, t.instrument_token, 'sl', t.stoploss, t.sl_limit, t.exit_price, t.pnl, t.exit_time, %s
FROM %s AS t
CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(t.sl_order_ids, '[]'::jsonb)) AS sl_id(value)
WHERE NOT EXISTS (
    SELECT 1 FROM %s AS o
    WHERE o.trade_id = t.id
      AND lower(COALESCE(o.order_type, '')) = 'sl'
      AND COALESCE(o.order_id, '') = COALESCE(NULLIF(sl_id.value, ''), '')
)`, ordersTableName, brokerageExpr, s.tradesTable, ordersTableName)); err != nil {
			return fmt.Errorf("backfill sl orders from legacy trade columns: %w", err)
		}
	}

	return nil
}

func (s *Store) CreateAccount(ctx context.Context, params CreateAccountParams) (model.Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Account{}, fmt.Errorf("begin account create: %w", err)
	}
	defer tx.Rollback(ctx)

	account, err := s.createAccountTx(ctx, tx, params)
	if err != nil {
		return model.Account{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Account{}, fmt.Errorf("commit account create: %w", err)
	}
	return account, nil
}

func (s *Store) createAccountTx(ctx context.Context, tx pgx.Tx, params CreateAccountParams) (model.Account, error) {
	currentTime := params.CurrentTime
	if currentTime.IsZero() {
		currentTime = time.Now()
	}
	currentTime = currentTime.In(s.loc)

	botName := normalizeBotName(params.BotName)
	currDate := normalizeCurrDate(params.CurrDate, currentTime)
	monthYear := normalizeMonthYear(params.MonthYear, timeForMonthYear(currDate, currentTime))

	initCash, err := s.resolveAccountInitCashTx(ctx, tx, botName, monthYear, params.InitCash)
	if err != nil {
		return model.Account{}, err
	}

	var account model.Account
	if err := tx.QueryRow(ctx, `
INSERT INTO accounts (botname, curr_date, month_year, init_cash, net_profit)
VALUES ($1, $2, $3, $4, 0)
ON CONFLICT (botname, curr_date)
WHERE botname IS NOT NULL
  AND BTRIM(botname) <> ''
  AND curr_date IS NOT NULL
  AND BTRIM(curr_date) <> ''
DO UPDATE SET
    init_cash = CASE
        WHEN COALESCE(accounts.init_cash, 0) > 0 THEN accounts.init_cash
        WHEN COALESCE(EXCLUDED.init_cash, 0) > 0 THEN EXCLUDED.init_cash
        ELSE accounts.init_cash
    END,
    month_year = EXCLUDED.month_year,
    net_profit = COALESCE(accounts.net_profit, 0)
RETURNING id::text, COALESCE(botname, ''), COALESCE(curr_date, ''), COALESCE(month_year, ''), COALESCE(init_cash, 0)::float8, COALESCE(net_profit, 0)::float8`,
		botName, currDate, monthYear, initCash,
	).Scan(&account.ID, &account.BotName, &account.CurrDate, &account.MonthYear, &account.InitCash, &account.NetProfit); err != nil {
		return model.Account{}, fmt.Errorf("upsert account: %w", err)
	}

	return account, nil
}

func (s *Store) GetAccountIDForBotDate(ctx context.Context, botName string, currDate string) (string, error) {
	account, err := s.GetAccountByBotDate(ctx, botName, currDate)
	if err != nil {
		return "", err
	}
	return account.ID, nil
}

func (s *Store) GetAccountByBotDate(ctx context.Context, botName string, currDate string) (model.Account, error) {
	currentTime := time.Now().In(s.loc)
	botName = normalizeBotName(botName)
	currDate = normalizeCurrDate(currDate, currentTime)

	var account model.Account
	if err := s.pool.QueryRow(ctx, `
SELECT id::text, COALESCE(botname, ''), COALESCE(curr_date, ''), COALESCE(month_year, ''), COALESCE(init_cash, 0)::float8, COALESCE(net_profit, 0)::float8
FROM accounts
WHERE botname = $1
  AND curr_date = $2
ORDER BY id
LIMIT 1`, botName, currDate).Scan(&account.ID, &account.BotName, &account.CurrDate, &account.MonthYear, &account.InitCash, &account.NetProfit); err != nil {
		if err == pgx.ErrNoRows {
			return model.Account{}, fmt.Errorf("account row not found for bot_name=%s curr_date=%s", botName, currDate)
		}
		return model.Account{}, fmt.Errorf("load account for bot/date: %w", err)
	}
	return account, nil
}

func (s *Store) refreshAccountNetProfitFromOrdersTx(ctx context.Context, tx pgx.Tx, accountID string) error {
	if strings.TrimSpace(accountID) == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE accounts
SET net_profit = (
    SELECT COALESCE(SUM(o.pnl), 0)
    FROM %s AS t
    JOIN %s AS o ON o.trade_id = t.id
    WHERE t.acct_id = accounts.id
      AND o.exit_time IS NOT NULL
)
WHERE id::text = $1`, s.tradesTable, ordersTableName), strings.TrimSpace(accountID)); err != nil {
		return fmt.Errorf("refresh account net_profit from orders: %w", err)
	}
	return nil
}

func (s *Store) normalizeTradesTable(ctx context.Context) error {
	var hasQuoted bool
	var hasLower bool

	err := s.pool.QueryRow(ctx, `
SELECT
    to_regclass('public."Trades"') IS NOT NULL,
    to_regclass('public.trades') IS NOT NULL`).Scan(&hasQuoted, &hasLower)
	if err != nil {
		return fmt.Errorf("check trades table variants: %w", err)
	}

	if hasQuoted && hasLower {
		return fmt.Errorf(`both public."Trades" and public.trades exist; consolidate them before starting ordersystem`)
	}

	if hasQuoted {
		if _, err := s.pool.Exec(ctx, `ALTER TABLE "Trades" RENAME TO trades`); err != nil {
			return fmt.Errorf(`rename public."Trades" to public.trades: %w`, err)
		}
	}

	return nil
}

func (s *Store) detectTradesTable(ctx context.Context) error {
	var table string
	err := s.pool.QueryRow(ctx, `
SELECT CASE
    WHEN to_regclass('public.trades') IS NOT NULL THEN 'trades'
    ELSE ''
END`).Scan(&table)
	if err != nil {
		return fmt.Errorf("detect trades table: %w", err)
	}
	s.tradesTable = table
	return nil
}

func (s *Store) columnExists(ctx context.Context, tableName string, columnName string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = $1
      AND column_name = $2
)`, tableName, columnName).Scan(&exists); err != nil {
		return false, fmt.Errorf("check column %s.%s: %w", tableName, columnName, err)
	}
	return exists, nil
}

func (s *Store) CreateTrade(ctx context.Context, params CreateTradeParams) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin create trade: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`
INSERT INTO %s (
    acct_id,
    symbol,
    instrument_token,
    side,
    qty,
    product,
    validity,
    tsl_active,
    start_trail_after,
    entry_spot,
    spot_ltp,
    spot_trail_anchor,
    trail_points,
    status,
    total_brokerage,
    tag_entry,
    tag_sl,
    description
) VALUES (
    $1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
)
RETURNING id::text`, s.tradesTable)

	var id string
	err = tx.QueryRow(ctx, query,
		nullIfEmpty(params.AccountID),
		nullIfEmpty(params.Symbol),
		nullIfEmpty(params.InstrumentToken),
		nullIfEmpty(params.Side),
		params.Qty,
		nullIfEmpty(params.Product),
		nullIfEmpty(params.Validity),
		params.TSLActive,
		nullFloatPtr(params.StartTrailAfter),
		nullFloatPtr(params.EntrySpot),
		nullFloatPtr(params.SpotLTP),
		nullFloatPtr(params.SpotTrailAnchor),
		nullFloatPtr(params.TrailPoints),
		nullIfEmpty(params.Status),
		nullFloatPtr(params.TotalBrokerage),
		nullIfEmpty(params.TagEntry),
		nullIfEmpty(params.TagSL),
		nullIfEmpty(params.Description),
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert trade: %w", err)
	}

	for _, order := range params.Orders {
		if err := insertOrderTx(ctx, tx, id, order); err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit create trade: %w", err)
	}
	return id, nil
}

func insertOrderTx(ctx context.Context, tx pgx.Tx, tradeID string, params CreateOrderParams) error {
	_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (
    order_id,
    trade_id,
    instrument_token,
    order_type,
    qty,
    entry_price,
    target,
    stoploss,
    sl_limit,
    exit_price,
    pnl,
    exit_time,
    brokerage
) VALUES (
    $1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
)`, ordersTableName),
		nullIfEmpty(params.OrderID),
		tradeID,
		nullIfEmpty(params.InstrumentToken),
		strings.ToLower(strings.TrimSpace(params.OrderType)),
		nullIntPtr(params.Qty),
		nullFloatPtr(params.EntryPrice),
		nullFloatPtr(params.Target),
		nullFloatPtr(params.Stoploss),
		nullFloatPtr(params.SLLimit),
		nullFloatPtr(params.ExitPrice),
		nullFloatPtr(params.PNL),
		nullTimePtr(params.ExitTime),
		nullFloatPtr(params.Brokerage),
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}
	return nil
}

func (s *Store) GetTradeByID(ctx context.Context, tradeID string) (model.Trade, error) {
	query := fmt.Sprintf(`
SELECT
    t.id::text,
    COALESCE(t.acct_id::text, ''),
    COALESCE(a.botname, ''),
    COALESCE(t.symbol, ''),
    COALESCE(t.instrument_token, ''),
    COALESCE(t.side, ''),
    COALESCE(t.qty, 0),
    COALESCE(t.product, ''),
    COALESCE(t.validity, ''),
    COALESCE(t.tsl_active, false),
    COALESCE(t.start_trail_after, 0)::float8,
    COALESCE(t.entry_spot, 0)::float8,
    COALESCE(t.spot_ltp, 0)::float8,
    COALESCE(t.spot_trail_anchor, 0)::float8,
    COALESCE(t.trail_points, 0)::float8,
    COALESCE(t.status, ''),
    COALESCE(t."timestamp", NOW()),
    COALESCE(t.total_brokerage, 0)::float8,
    COALESCE(t.tag_entry, ''),
    COALESCE(t.tag_sl, ''),
    COALESCE(t.description, '')
FROM %s AS t
LEFT JOIN accounts AS a ON a.id = t.acct_id
WHERE t.id::text = $1
LIMIT 1`, s.tradesTable)

	var trade model.Trade
	err := s.pool.QueryRow(ctx, query, strings.TrimSpace(tradeID)).Scan(
		&trade.ID,
		&trade.AccountID,
		&trade.BotName,
		&trade.Symbol,
		&trade.InstrumentToken,
		&trade.Side,
		&trade.Qty,
		&trade.Product,
		&trade.Validity,
		&trade.TSLActive,
		&trade.StartTrailAfter,
		&trade.EntrySpot,
		&trade.SpotLTP,
		&trade.SpotTrailAnchor,
		&trade.TrailPoints,
		&trade.Status,
		&trade.Timestamp,
		&trade.TotalBrokerage,
		&trade.TagEntry,
		&trade.TagSL,
		&trade.Description,
	)
	if err != nil {
		return model.Trade{}, fmt.Errorf("fetch trade by id: %w", err)
	}

	orders, err := s.listOrdersByTradeID(ctx, tradeID)
	if err != nil {
		return model.Trade{}, err
	}
	trade.Orders = orders
	applyOrderSummary(&trade)

	return trade, nil
}

func (s *Store) ListTradesByAccountID(ctx context.Context, accountID string) ([]model.Trade, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, fmt.Errorf("account id is required")
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT
    t.id::text,
    COALESCE(t.acct_id::text, ''),
    COALESCE(a.botname, ''),
    COALESCE(t.symbol, ''),
    COALESCE(t.instrument_token, ''),
    COALESCE(t.side, ''),
    COALESCE(t.qty, 0),
    COALESCE(t.product, ''),
    COALESCE(t.validity, ''),
    COALESCE(t.tsl_active, false),
    COALESCE(t.start_trail_after, 0)::float8,
    COALESCE(t.entry_spot, 0)::float8,
    COALESCE(t.spot_ltp, 0)::float8,
    COALESCE(t.spot_trail_anchor, 0)::float8,
    COALESCE(t.trail_points, 0)::float8,
    COALESCE(t.status, ''),
    COALESCE(t."timestamp", NOW()),
    COALESCE(t.total_brokerage, 0)::float8,
    COALESCE(t.tag_entry, ''),
    COALESCE(t.tag_sl, ''),
    COALESCE(t.description, '')
FROM %s AS t
LEFT JOIN accounts AS a ON a.id = t.acct_id
WHERE t.acct_id::text = $1
ORDER BY t."timestamp", t.id`, s.tradesTable), accountID)
	if err != nil {
		return nil, fmt.Errorf("query account trades: %w", err)
	}
	defer rows.Close()

	trades := make([]model.Trade, 0)
	for rows.Next() {
		var trade model.Trade
		if err := rows.Scan(
			&trade.ID,
			&trade.AccountID,
			&trade.BotName,
			&trade.Symbol,
			&trade.InstrumentToken,
			&trade.Side,
			&trade.Qty,
			&trade.Product,
			&trade.Validity,
			&trade.TSLActive,
			&trade.StartTrailAfter,
			&trade.EntrySpot,
			&trade.SpotLTP,
			&trade.SpotTrailAnchor,
			&trade.TrailPoints,
			&trade.Status,
			&trade.Timestamp,
			&trade.TotalBrokerage,
			&trade.TagEntry,
			&trade.TagSL,
			&trade.Description,
		); err != nil {
			return nil, fmt.Errorf("scan account trade: %w", err)
		}

		orders, err := s.listOrdersByTradeID(ctx, trade.ID)
		if err != nil {
			return nil, err
		}
		trade.Orders = orders
		applyOrderSummary(&trade)
		trades = append(trades, trade)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate account trades: %w", rows.Err())
	}

	return trades, nil
}

func (s *Store) listOrdersByTradeID(ctx context.Context, tradeID string) ([]model.Order, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT
    id::text,
    COALESCE(order_id, ''),
    COALESCE(trade_id::text, ''),
    COALESCE(instrument_token, ''),
    COALESCE(order_type, ''),
    COALESCE(qty, 0),
    COALESCE(entry_price, 0)::float8,
    COALESCE(target, 0)::float8,
    COALESCE(stoploss, 0)::float8,
    COALESCE(sl_limit, 0)::float8,
    COALESCE(exit_price, 0)::float8,
    COALESCE(pnl, 0)::float8,
    exit_time,
    COALESCE(brokerage, 0)::float8
FROM %s
WHERE trade_id::text = $1
ORDER BY order_type, id`, ordersTableName), strings.TrimSpace(tradeID))
	if err != nil {
		return nil, fmt.Errorf("query orders by trade id: %w", err)
	}
	defer rows.Close()

	orders := make([]model.Order, 0)
	for rows.Next() {
		var order model.Order
		var exitTime sql.NullTime
		if err := rows.Scan(
			&order.ID,
			&order.OrderID,
			&order.TradeID,
			&order.InstrumentToken,
			&order.OrderType,
			&order.Qty,
			&order.EntryPrice,
			&order.Target,
			&order.Stoploss,
			&order.SLLimit,
			&order.ExitPrice,
			&order.PNL,
			&exitTime,
			&order.Brokerage,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}
		if exitTime.Valid {
			t := exitTime.Time
			order.ExitTime = &t
		}
		orders = append(orders, order)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate orders: %w", rows.Err())
	}
	return orders, nil
}

func applyOrderSummary(trade *model.Trade) {
	totalBrokerageFromOrders := 0.0
	entryQty := 0
	entryValue := 0.0
	firstEntryPrice := 0.0
	for _, order := range trade.Orders {
		totalBrokerageFromOrders += order.Brokerage
		switch strings.ToLower(strings.TrimSpace(order.OrderType)) {
		case "entry":
			if order.OrderID != "" {
				trade.EntryOrderIDs = append(trade.EntryOrderIDs, order.OrderID)
			}
			if firstEntryPrice == 0 && order.EntryPrice > 0 {
				firstEntryPrice = order.EntryPrice
			}
			if order.EntryPrice > 0 && order.Qty > 0 {
				entryQty += order.Qty
				entryValue += order.EntryPrice * float64(order.Qty)
			}
			if trade.Target == 0 && order.Target > 0 {
				trade.Target = order.Target
			}
		case "sl":
			if order.OrderID != "" {
				trade.SLOrderIDs = append(trade.SLOrderIDs, order.OrderID)
			}
			if trade.Stoploss == 0 && order.Stoploss > 0 {
				trade.Stoploss = order.Stoploss
			}
			if trade.SLLimit == 0 && order.SLLimit > 0 {
				trade.SLLimit = order.SLLimit
			}
		}
		if order.ExitPrice > 0 {
			trade.ExitPrice = order.ExitPrice
		}
		if order.ExitTime != nil {
			t := *order.ExitTime
			trade.ExitTime = &t
		}
		trade.PNL += order.PNL
	}
	if entryQty > 0 {
		trade.EntryPrice = entryValue / float64(entryQty)
	} else if trade.EntryPrice == 0 && firstEntryPrice > 0 {
		trade.EntryPrice = firstEntryPrice
	}
	if totalBrokerageFromOrders > 0 {
		trade.TotalBrokerage = totalBrokerageFromOrders
	}
}

func (s *Store) ListOpenTradesForSLPolling(ctx context.Context) ([]model.TradeForSLPolling, error) {
	query := fmt.Sprintf(`
SELECT
    t.id::text,
    COALESCE(a.botname, ''),
    COALESCE(t.instrument_token, ''),
    UPPER(COALESCE(t.side, 'BUY')),
    COALESCE(t.qty, 0),
    COALESCE(t.product, ''),
    COALESCE((
        SELECT entry_price
        FROM %s AS entry_orders
        WHERE entry_orders.trade_id = t.id
          AND lower(COALESCE(entry_orders.order_type, '')) = 'entry'
          AND entry_orders.entry_price IS NOT NULL
        ORDER BY entry_orders.id
        LIMIT 1
    ), 0)::float8,
    COALESCE(t.total_brokerage, 0)::float8,
    COALESCE(o.order_id, ''),
    lower(COALESCE(o.order_type, '')),
    COALESCE(o.entry_price, 0)::float8,
    COALESCE(o.stoploss, 0)::float8,
    COALESCE(o.qty, 0),
    COALESCE(o.brokerage, 0)::float8
FROM %s AS t
JOIN %s AS o ON o.trade_id = t.id
LEFT JOIN accounts AS a ON a.id = t.acct_id
WHERE (t.status IS NULL OR UPPER(t.status) IN ('OPEN', 'PLACED', 'ENTRY_PLACED'))
  AND lower(COALESCE(o.order_type, '')) IN ('entry', 'sl')
  AND NULLIF(BTRIM(COALESCE(o.order_id, '')), '') IS NOT NULL
  AND (lower(COALESCE(o.order_type, '')) = 'entry' OR o.exit_time IS NULL)
ORDER BY t."timestamp", t.id, o.order_type, o.id`, ordersTableName, s.tradesTable, ordersTableName)

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query open trades for sl polling: %w", err)
	}
	defer rows.Close()

	tradeByID := make(map[string]*model.TradeForSLPolling)
	tradeOrder := make([]string, 0)
	for rows.Next() {
		var tradeID string
		var botName string
		var instrumentToken string
		var side string
		var qty int
		var product string
		var entryPrice float64
		var totalBrokerage float64
		var orderID string
		var orderType string
		var orderEntryPrice float64
		var stoploss float64
		var orderQty int
		var orderBrokerage float64
		if err := rows.Scan(&tradeID, &botName, &instrumentToken, &side, &qty, &product, &entryPrice, &totalBrokerage, &orderID, &orderType, &orderEntryPrice, &stoploss, &orderQty, &orderBrokerage); err != nil {
			return nil, fmt.Errorf("scan open trade for sl polling: %w", err)
		}
		trade := tradeByID[tradeID]
		if trade == nil {
			trade = &model.TradeForSLPolling{
				ID:              tradeID,
				BotName:         botName,
				InstrumentToken: instrumentToken,
				Side:            side,
				Qty:             qty,
				Product:         product,
				EntryPrice:      entryPrice,
				TotalBrokerage:  totalBrokerage,
			}
			tradeByID[tradeID] = trade
			tradeOrder = append(tradeOrder, tradeID)
		}
		switch orderType {
		case "entry":
			trade.EntryOrders = append(trade.EntryOrders, model.EntryOrderForPolling{
				OrderID:    orderID,
				EntryPrice: orderEntryPrice,
				Qty:        orderQty,
				Brokerage:  orderBrokerage,
			})
		case "sl":
			trade.SLOrders = append(trade.SLOrders, model.StopLossOrderForPolling{
				OrderID:   orderID,
				Stoploss:  stoploss,
				Qty:       orderQty,
				Brokerage: orderBrokerage,
			})
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate open trades for sl polling: %w", rows.Err())
	}

	trades := make([]model.TradeForSLPolling, 0, len(tradeOrder))
	for _, tradeID := range tradeOrder {
		trades = append(trades, *tradeByID[tradeID])
	}
	return trades, nil
}

func (s *Store) UpdateTradeStatus(ctx context.Context, tradeID string, status string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = $2 WHERE id::text = $1`, s.tradesTable)
	_, err := s.pool.Exec(ctx, query, strings.TrimSpace(tradeID), strings.TrimSpace(status))
	if err != nil {
		return fmt.Errorf("update trade status: %w", err)
	}
	return nil
}

func (s *Store) DisableTrailingByTradeID(ctx context.Context, tradeID string) error {
	query := fmt.Sprintf(`
UPDATE %s
SET tsl_active = false
WHERE id::text = $1
  AND (status IS NULL OR UPPER(status) IN ('OPEN', 'PLACED', 'ENTRY_PLACED'))`, s.tradesTable)
	_, err := s.pool.Exec(ctx, query, strings.TrimSpace(tradeID))
	if err != nil {
		return fmt.Errorf("disable trailing by trade id: %w", err)
	}
	return nil
}

func (s *Store) UpdateTrailingStateByTradeID(ctx context.Context, tradeID string, stoploss *float64, slLimit *float64, spotTrailAnchor *float64) error {
	if strings.TrimSpace(tradeID) == "" {
		return fmt.Errorf("trade id is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin update trailing state: %w", err)
	}
	defer tx.Rollback(ctx)

	tradeQuery := fmt.Sprintf(`
UPDATE %s
SET spot_trail_anchor = COALESCE($2::numeric, spot_trail_anchor)
WHERE id::text = $1`, s.tradesTable)
	if _, err := tx.Exec(ctx, tradeQuery, strings.TrimSpace(tradeID), nullFloatPtr(spotTrailAnchor)); err != nil {
		return fmt.Errorf("update trade trailing state: %w", err)
	}

	if stoploss != nil || slLimit != nil {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET
    stoploss = COALESCE($2::numeric, stoploss),
    sl_limit = COALESCE($3::numeric, sl_limit)
WHERE trade_id::text = $1
  AND lower(COALESCE(order_type, '')) = 'sl'
  AND exit_time IS NULL`, ordersTableName), strings.TrimSpace(tradeID), nullFloatPtr(stoploss), nullFloatPtr(slLimit)); err != nil {
			return fmt.Errorf("update sl order trailing state: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update trailing state: %w", err)
	}
	return nil
}

func (s *Store) SyncEntryOrderExecution(ctx context.Context, tradeID string, brokerOrderID string, entryPrice float64, qty int, brokerage *float64) error {
	tradeID = strings.TrimSpace(tradeID)
	brokerOrderID = strings.TrimSpace(brokerOrderID)
	if tradeID == "" {
		return fmt.Errorf("trade id is required")
	}
	if brokerOrderID == "" {
		return fmt.Errorf("broker order id is required")
	}
	if entryPrice <= 0 && qty <= 0 && brokerage == nil {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin sync entry order transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET
    entry_price = CASE WHEN $3 > 0 THEN $3 ELSE entry_price END,
    qty = CASE WHEN $4 > 0 THEN $4 ELSE qty END,
    brokerage = COALESCE($5::numeric, brokerage)
WHERE trade_id::text = $1
  AND lower(COALESCE(order_type, '')) = 'entry'
  AND order_id = $2`, ordersTableName), tradeID, brokerOrderID, entryPrice, qty, nullFloatPtr(brokerage))
	if err != nil {
		return fmt.Errorf("sync entry order execution: %w", err)
	}
	if result.RowsAffected() == 0 {
		return nil
	}

	accountID, err := s.recalculateTradeExecutionTx(ctx, tx, tradeID, "")
	if err != nil {
		return err
	}
	if err := s.refreshAccountNetProfitFromOrdersTx(ctx, tx, accountID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sync entry order transaction: %w", err)
	}

	return nil
}

func (s *Store) RecordStopLossFill(ctx context.Context, tradeID string, brokerOrderID string, exitPrice float64, qty int, brokerage *float64, exitStatus string) error {
	tradeID = strings.TrimSpace(tradeID)
	brokerOrderID = strings.TrimSpace(brokerOrderID)
	if tradeID == "" {
		return fmt.Errorf("trade id is required")
	}
	if brokerOrderID == "" {
		return fmt.Errorf("broker order id is required")
	}
	if exitPrice <= 0 && qty <= 0 && brokerage == nil {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin record sl fill transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET
    exit_price = CASE WHEN $3 > 0 THEN $3 ELSE exit_price END,
    qty = CASE WHEN $4 > 0 THEN $4 ELSE qty END,
    brokerage = COALESCE($5::numeric, brokerage),
    exit_time = COALESCE(exit_time, NOW())
WHERE trade_id::text = $1
  AND lower(COALESCE(order_type, '')) = 'sl'
  AND order_id = $2`, ordersTableName), tradeID, brokerOrderID, exitPrice, qty, nullFloatPtr(brokerage))
	if err != nil {
		return fmt.Errorf("record sl fill: %w", err)
	}
	if result.RowsAffected() == 0 {
		return nil
	}

	if strings.TrimSpace(exitStatus) == "" {
		exitStatus = "STOPLOSS HIT"
	}
	accountID, err := s.recalculateTradeExecutionTx(ctx, tx, tradeID, exitStatus)
	if err != nil {
		return err
	}
	if err := s.refreshAccountNetProfitFromOrdersTx(ctx, tx, accountID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit record sl fill transaction: %w", err)
	}

	return nil
}

func (s *Store) MarkTradeClosedFromSL(ctx context.Context, tradeID string, brokerOrderID string, exitPrice float64, additionalBrokerage float64, exitStatus string) error {
	var brokerage *float64
	if additionalBrokerage > 0 {
		value := additionalBrokerage
		brokerage = &value
	}
	return s.RecordStopLossFill(ctx, tradeID, brokerOrderID, exitPrice, 0, brokerage, exitStatus)
}

type slOrderExecution struct {
	id        string
	qty       int
	exitPrice float64
	brokerage float64
}

func (s *Store) recalculateTradeExecutionTx(ctx context.Context, tx pgx.Tx, tradeID string, exitStatus string) (string, error) {
	query := fmt.Sprintf(`
SELECT
    COALESCE(acct_id::text, ''),
    UPPER(COALESCE(side, 'BUY')),
    COALESCE(qty, 0)
FROM %s
WHERE id::text = $1
FOR UPDATE`, s.tradesTable)

	var accountID string
	var side string
	var tradeQty int
	if err := tx.QueryRow(ctx, query, strings.TrimSpace(tradeID)).Scan(&accountID, &side, &tradeQty); err != nil {
		return "", fmt.Errorf("load trade execution state: %w", err)
	}
	if tradeQty < 0 {
		tradeQty = int(math.Abs(float64(tradeQty)))
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT
    id::text,
    lower(COALESCE(order_type, '')),
    COALESCE(qty, 0),
    COALESCE(entry_price, 0)::float8,
    COALESCE(exit_price, 0)::float8,
    COALESCE(brokerage, 0)::float8,
    exit_time IS NOT NULL
FROM %s
WHERE trade_id::text = $1
ORDER BY order_type, id`, ordersTableName), strings.TrimSpace(tradeID))
	if err != nil {
		return "", fmt.Errorf("load order execution rows: %w", err)
	}
	defer rows.Close()

	entryQty := 0
	entryValue := 0.0
	entryBrokerage := 0.0
	firstEntryPrice := 0.0
	totalBrokerage := 0.0
	openSLCount := 0
	closedSLOrders := make([]slOrderExecution, 0)

	for rows.Next() {
		var orderID string
		var orderType string
		var orderQty int
		var entryPrice float64
		var exitPrice float64
		var orderBrokerage float64
		var hasExitTime bool
		if err := rows.Scan(&orderID, &orderType, &orderQty, &entryPrice, &exitPrice, &orderBrokerage, &hasExitTime); err != nil {
			return "", fmt.Errorf("scan order execution row: %w", err)
		}
		if orderQty < 0 {
			orderQty = int(math.Abs(float64(orderQty)))
		}
		totalBrokerage += orderBrokerage

		switch orderType {
		case "entry":
			entryBrokerage += orderBrokerage
			if firstEntryPrice == 0 && entryPrice > 0 {
				firstEntryPrice = entryPrice
			}
			if orderQty > 0 && entryPrice > 0 {
				entryQty += orderQty
				entryValue += entryPrice * float64(orderQty)
			}
		case "sl":
			if hasExitTime && exitPrice > 0 {
				closedSLOrders = append(closedSLOrders, slOrderExecution{
					id:        orderID,
					qty:       orderQty,
					exitPrice: exitPrice,
					brokerage: orderBrokerage,
				})
			} else {
				openSLCount++
			}
		}
	}
	if rows.Err() != nil {
		return "", fmt.Errorf("iterate order execution rows: %w", rows.Err())
	}

	entryPrice := firstEntryPrice
	if entryQty > 0 {
		entryPrice = entryValue / float64(entryQty)
	}
	baseQty := entryQty
	if baseQty <= 0 {
		baseQty = tradeQty
	}

	for _, slOrder := range closedSLOrders {
		orderQty := slOrder.qty
		if orderQty <= 0 && len(closedSLOrders) == 1 && tradeQty > 0 {
			orderQty = tradeQty
		}

		entryBrokerageShare := 0.0
		if orderQty > 0 && baseQty > 0 {
			entryBrokerageShare = entryBrokerage * float64(orderQty) / float64(baseQty)
		} else if len(closedSLOrders) == 1 {
			entryBrokerageShare = entryBrokerage
		}

		pnl := calculatePNL(side, entryPrice, slOrder.exitPrice, orderQty, slOrder.brokerage+entryBrokerageShare)
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET
    qty = CASE WHEN COALESCE(qty, 0) <= 0 AND $3 > 0 THEN $3 ELSE qty END,
    pnl = $2
WHERE id::text = $1`, ordersTableName), slOrder.id, pnl, orderQty); err != nil {
			return "", fmt.Errorf("recalculate sl order pnl: %w", err)
		}
	}

	if openSLCount == 0 && len(closedSLOrders) > 0 && strings.TrimSpace(exitStatus) != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET
    status = $2,
    total_brokerage = $3
WHERE id::text = $1`, s.tradesTable), strings.TrimSpace(tradeID), strings.TrimSpace(exitStatus), totalBrokerage); err != nil {
			return "", fmt.Errorf("update closed trade execution totals: %w", err)
		}
		return accountID, nil
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET total_brokerage = $2
WHERE id::text = $1`, s.tradesTable), strings.TrimSpace(tradeID), totalBrokerage); err != nil {
		return "", fmt.Errorf("update trade execution totals: %w", err)
	}
	return accountID, nil
}

func calculatePNL(side string, entryPrice float64, exitPrice float64, qty int, totalBrokerage float64) float64 {
	gross := 0.0
	if strings.EqualFold(strings.TrimSpace(side), "SELL") {
		gross = (entryPrice - exitPrice) * float64(qty)
	} else {
		gross = (exitPrice - entryPrice) * float64(qty)
	}
	return gross - totalBrokerage
}

func nullIfEmpty(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nullFloatPtr(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullIntPtr(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullTimePtr(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

func normalizeBotName(botName string) string {
	trimmed := strings.TrimSpace(botName)
	if trimmed == "" {
		return "default"
	}
	return trimmed
}

func normalizeMonthYear(monthYear string, currentTime time.Time) string {
	digitsOnly := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, strings.TrimSpace(monthYear))
	if len(digitsOnly) == 6 {
		return digitsOnly
	}
	return currentTime.Format("012006")
}

func normalizeCurrDate(currDate string, currentTime time.Time) string {
	trimmed := strings.TrimSpace(currDate)
	if trimmed == "" {
		return currentTime.Format("02-01-2006")
	}
	for _, layout := range []string{"02-01-2006", "2006-01-02", "02/01/2006"} {
		if parsed, err := time.ParseInLocation(layout, trimmed, currentTime.Location()); err == nil {
			return parsed.Format("02-01-2006")
		}
	}
	digitsOnly := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, trimmed)
	if len(digitsOnly) == 8 {
		return digitsOnly[:2] + "-" + digitsOnly[2:4] + "-" + digitsOnly[4:]
	}
	return trimmed
}

func timeForMonthYear(currDate string, fallback time.Time) time.Time {
	if parsed, err := time.ParseInLocation("02-01-2006", normalizeCurrDate(currDate, fallback), fallback.Location()); err == nil {
		return parsed
	}
	return fallback
}

func (s *Store) resolveAccountInitCashTx(ctx context.Context, tx pgx.Tx, botName string, monthYear string, requested *float64) (float64, error) {
	var existing float64
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(init_cash), 0)::float8
FROM accounts
WHERE botname = $1
  AND month_year = $2`, botName, monthYear).Scan(&existing); err != nil {
		return 0, fmt.Errorf("resolve account init_cash in tx: %w", err)
	}

	return resolveInitCashValue(requested, existing, s.accountInitialCash), nil
}

func resolveInitCashValue(requested *float64, existing float64, fallback float64) float64 {
	if requested != nil && *requested > 0 {
		return *requested
	}
	if existing > 0 {
		return existing
	}
	if fallback > 0 {
		return fallback
	}
	return 0
}
