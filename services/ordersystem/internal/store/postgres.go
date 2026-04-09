package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const tradesTableName = "trades"

type Store struct {
	pool               *pgxpool.Pool
	tradesTable        string
	loc                *time.Location
	accountInitialCash float64
}

type CreateTradeParams struct {
	BotName         string
	Symbol          string
	InstrumentToken string
	Side            string
	Qty             int
	Product         string
	Validity        string
	EntryOrderIDs   []string
	SLOrderIDs      []string
	TargetOrderID   *string
	EntryPrice      *float64
	Target          *float64
	Stoploss        *float64
	SLLimit         *float64
	TSLActive       bool
	StartTrailAfter *float64
	EntrySpot       *float64
	SpotLTP         *float64
	SpotTrailAnchor *float64
	TrailPoints     *float64
	Status          string
	Taxes           *float64
	TagEntry        string
	TagSL           string
	Description     string
	SLOrderQtyMap   map[string]int
}

type AccountSnapshotParams struct {
	BotName     string
	MonthYear   string
	InitCash    *float64
	CurrentTime time.Time
	ProfitDelta float64
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

	if err := s.detectTradesTable(ctx); err != nil {
		return err
	}

	if s.tradesTable == "" {
		s.tradesTable = tradesTableName
		if _, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_name TEXT,
    symbol TEXT,
    instrument_token TEXT,
    side TEXT,
    qty INTEGER,
    product TEXT,
    validity TEXT,
    entry_order_ids JSONB DEFAULT '[]'::jsonb,
    sl_order_ids JSONB DEFAULT '[]'::jsonb,
    target_order_id TEXT,
    entry_price NUMERIC(18,6),
    target NUMERIC(18,6),
    stoploss NUMERIC(18,6),
    sl_limit NUMERIC(18,6),
    tsl_active BOOLEAN DEFAULT FALSE,
    start_trail_after NUMERIC(18,6),
    entry_spot NUMERIC(18,6),
    spot_ltp NUMERIC(18,6),
    spot_trail_anchor NUMERIC(18,6),
    trail_points NUMERIC(18,6),
    status TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    exit_price NUMERIC(18,6),
    pnl NUMERIC(18,6),
    exit_time TIMESTAMPTZ,
    taxes NUMERIC(18,6),
    tag_entry TEXT,
    tag_sl TEXT,
    description TEXT,
    sl_order_qty_map JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades ("timestamp");
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);
`); err != nil {
			return fmt.Errorf("create trades table: %w", err)
		}
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS taxes NUMERIC(18,6)`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.taxes column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS bot_name TEXT`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.bot_name column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS spot_trail_anchor NUMERIC(18,6)`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.spot_trail_anchor column: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    botname TEXT,
    month_year TEXT,
    init_cash NUMERIC(18,6),
    current_date TEXT,
    profit NUMERIC(18,6),
    max_drawdown NUMERIC(18,6)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_bot_month_date ON accounts (botname, month_year, current_date);
CREATE INDEX IF NOT EXISTS idx_accounts_bot_month_year ON accounts (botname, month_year);
`); err != nil {
		return fmt.Errorf("ensure accounts table: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS botname TEXT`); err != nil {
		return fmt.Errorf("ensure accounts.botname column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS month_year TEXT`); err != nil {
		return fmt.Errorf("ensure accounts.month_year column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS init_cash NUMERIC(18,6)`); err != nil {
		return fmt.Errorf("ensure accounts.init_cash column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS current_date TEXT`); err != nil {
		return fmt.Errorf("ensure accounts.current_date column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS profit NUMERIC(18,6)`); err != nil {
		return fmt.Errorf("ensure accounts.profit column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE accounts ADD COLUMN IF NOT EXISTS max_drawdown NUMERIC(18,6)`); err != nil {
		return fmt.Errorf("ensure accounts.max_drawdown column: %w", err)
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
$$;`); err != nil {
		return fmt.Errorf("backfill accounts daily columns: %w", err)
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS account_id UUID`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.account_id column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_trades_account_id ON %s (account_id)`, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.account_id index: %w", err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_trades_account_id'
          AND conrelid = to_regclass('public.%s')
    ) THEN
        ALTER TABLE %s
        ADD CONSTRAINT fk_trades_account_id
        FOREIGN KEY (account_id) REFERENCES accounts(id);
    END IF;
END
$$;`, tradesTableName, s.tradesTable)); err != nil {
		return fmt.Errorf("ensure trades.account_id foreign key: %w", err)
	}

	return nil
}

func (s *Store) UpsertAccountSnapshot(ctx context.Context, params AccountSnapshotParams) (string, error) {
	currentTime := params.CurrentTime
	if currentTime.IsZero() {
		currentTime = time.Now()
	}
	currentTime = currentTime.In(s.loc)

	botName := normalizeBotName(params.BotName)
	monthYear := normalizeMonthYear(params.MonthYear, currentTime)
	currentDate := currentTime.Format("02-01-2006")

	initCash, err := s.resolveAccountInitCash(ctx, botName, monthYear, params.InitCash)
	if err != nil {
		return "", err
	}

	var accountID string
	if err := s.pool.QueryRow(ctx, `
INSERT INTO accounts (botname, month_year, init_cash, current_date, profit, max_drawdown)
VALUES ($1, $2, $3, $4, $5, CASE WHEN $5 < 0 THEN $5 ELSE 0 END)
ON CONFLICT (botname, month_year, current_date)
DO UPDATE SET
    init_cash = CASE
        WHEN COALESCE(accounts.init_cash, 0) = 0 AND EXCLUDED.init_cash > 0 THEN EXCLUDED.init_cash
        ELSE accounts.init_cash
    END,
    profit = COALESCE(accounts.profit, 0) + EXCLUDED.profit,
    max_drawdown = LEAST(COALESCE(accounts.max_drawdown, 0), COALESCE(accounts.profit, 0) + EXCLUDED.profit)
RETURNING id::text
`, botName, monthYear, initCash, currentDate, params.ProfitDelta).Scan(&accountID); err != nil {
		return "", fmt.Errorf("upsert account snapshot: %w", err)
	}

	return accountID, nil
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

func (s *Store) CreateTrade(ctx context.Context, params CreateTradeParams) (string, error) {
	entryOrderIDsJSON, err := json.Marshal(params.EntryOrderIDs)
	if err != nil {
		return "", fmt.Errorf("marshal entry order ids: %w", err)
	}
	slOrderIDsJSON, err := json.Marshal(params.SLOrderIDs)
	if err != nil {
		return "", fmt.Errorf("marshal sl order ids: %w", err)
	}
	slOrderQtyMapJSON, err := json.Marshal(params.SLOrderQtyMap)
	if err != nil {
		return "", fmt.Errorf("marshal sl order qty map: %w", err)
	}

	query := fmt.Sprintf(`
INSERT INTO %s (
    bot_name,
    symbol,
    instrument_token,
    side,
    qty,
    product,
    validity,
    entry_order_ids,
    sl_order_ids,
    target_order_id,
    entry_price,
    target,
    stoploss,
    sl_limit,
    tsl_active,
    start_trail_after,
    entry_spot,
    spot_ltp,
    spot_trail_anchor,
    trail_points,
    status,
    taxes,
    tag_entry,
    tag_sl,
    description,
    sl_order_qty_map
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,
    $8::jsonb,
    $9::jsonb,
    $10,$11,$12,$13,$14,$15,
    $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26::jsonb
)
RETURNING id::text`, s.tradesTable)

	var id string
	err = s.pool.QueryRow(ctx, query,
		nullIfEmpty(params.BotName),
		nullIfEmpty(params.Symbol),
		nullIfEmpty(params.InstrumentToken),
		nullIfEmpty(params.Side),
		params.Qty,
		nullIfEmpty(params.Product),
		nullIfEmpty(params.Validity),
		string(entryOrderIDsJSON),
		string(slOrderIDsJSON),
		nullStringPtr(params.TargetOrderID),
		nullFloatPtr(params.EntryPrice),
		nullFloatPtr(params.Target),
		nullFloatPtr(params.Stoploss),
		nullFloatPtr(params.SLLimit),
		params.TSLActive,
		nullFloatPtr(params.StartTrailAfter),
		nullFloatPtr(params.EntrySpot),
		nullFloatPtr(params.SpotLTP),
		nullFloatPtr(params.SpotTrailAnchor),
		nullFloatPtr(params.TrailPoints),
		nullIfEmpty(params.Status),
		nullFloatPtr(params.Taxes),
		nullIfEmpty(params.TagEntry),
		nullIfEmpty(params.TagSL),
		nullIfEmpty(params.Description),
		string(slOrderQtyMapJSON),
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert trade: %w", err)
	}
	return id, nil
}

func (s *Store) GetTradeByID(ctx context.Context, tradeID string) (model.Trade, error) {
	query := fmt.Sprintf(`
SELECT
    id::text,
    COALESCE(account_id::text, ''),
    COALESCE(bot_name, ''),
    COALESCE(symbol, ''),
    COALESCE(instrument_token, ''),
    COALESCE(side, ''),
    COALESCE(qty, 0),
    COALESCE(product, ''),
    COALESCE(validity, ''),
    COALESCE(entry_order_ids, '[]'::jsonb),
    COALESCE(sl_order_ids, '[]'::jsonb),
    COALESCE(sl_order_qty_map, '{}'::jsonb),
    COALESCE(target_order_id, ''),
    COALESCE(entry_price, 0)::float8,
    COALESCE(target, 0)::float8,
    COALESCE(stoploss, 0)::float8,
    COALESCE(sl_limit, 0)::float8,
    COALESCE(tsl_active, false),
    COALESCE(start_trail_after, 0)::float8,
    COALESCE(entry_spot, 0)::float8,
    COALESCE(spot_ltp, 0)::float8,
    COALESCE(spot_trail_anchor, 0)::float8,
    COALESCE(trail_points, 0)::float8,
    COALESCE(status, ''),
    COALESCE("timestamp", NOW()),
    COALESCE(exit_price, 0)::float8,
    COALESCE(pnl, 0)::float8,
    COALESCE(taxes, 0)::float8,
    exit_time,
    COALESCE(tag_entry, ''),
    COALESCE(tag_sl, ''),
    COALESCE(description, '')
FROM %s
WHERE id::text = $1
LIMIT 1`, s.tradesTable)

	var trade model.Trade
	var entryOrderIDsRaw []byte
	var slOrderIDsRaw []byte
	var slOrderQtyMapRaw []byte
	var exitTime sql.NullTime

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
		&entryOrderIDsRaw,
		&slOrderIDsRaw,
		&slOrderQtyMapRaw,
		&trade.TargetOrderID,
		&trade.EntryPrice,
		&trade.Target,
		&trade.Stoploss,
		&trade.SLLimit,
		&trade.TSLActive,
		&trade.StartTrailAfter,
		&trade.EntrySpot,
		&trade.SpotLTP,
		&trade.SpotTrailAnchor,
		&trade.TrailPoints,
		&trade.Status,
		&trade.Timestamp,
		&trade.ExitPrice,
		&trade.PNL,
		&trade.Taxes,
		&exitTime,
		&trade.TagEntry,
		&trade.TagSL,
		&trade.Description,
	)
	if err != nil {
		return model.Trade{}, fmt.Errorf("fetch trade by id: %w", err)
	}

	if len(entryOrderIDsRaw) > 0 {
		_ = json.Unmarshal(entryOrderIDsRaw, &trade.EntryOrderIDs)
	}
	if len(slOrderIDsRaw) > 0 {
		_ = json.Unmarshal(slOrderIDsRaw, &trade.SLOrderIDs)
	}
	if len(slOrderQtyMapRaw) > 0 {
		_ = json.Unmarshal(slOrderQtyMapRaw, &trade.SLOrderQtyMap)
	}
	if exitTime.Valid {
		t := exitTime.Time
		trade.ExitTime = &t
	}

	return trade, nil
}

func (s *Store) ListOpenTradesForSLPolling(ctx context.Context) ([]model.TradeForSLPolling, error) {
	query := fmt.Sprintf(`
SELECT
    id::text,
    COALESCE(bot_name, ''),
    UPPER(COALESCE(side, 'BUY')),
    COALESCE(qty, 0),
    COALESCE(entry_price, 0)::float8,
    COALESCE(stoploss, 0)::float8,
    COALESCE(taxes, 0)::float8,
    COALESCE(sl_order_ids, '[]'::jsonb)
FROM %s
WHERE exit_time IS NULL
  AND (status IS NULL OR UPPER(status) IN ('OPEN', 'PLACED', 'ENTRY_PLACED'))
  AND sl_order_ids IS NOT NULL
  AND jsonb_typeof(sl_order_ids) = 'array'
  AND jsonb_array_length(sl_order_ids) > 0`, s.tradesTable)

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query open trades for sl polling: %w", err)
	}
	defer rows.Close()

	trades := make([]model.TradeForSLPolling, 0)
	for rows.Next() {
		var trade model.TradeForSLPolling
		var slOrderIDsRaw []byte
		if err := rows.Scan(
			&trade.ID,
			&trade.BotName,
			&trade.Side,
			&trade.Qty,
			&trade.EntryPrice,
			&trade.Stoploss,
			&trade.Taxes,
			&slOrderIDsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan open trade for sl polling: %w", err)
		}
		if len(slOrderIDsRaw) > 0 {
			_ = json.Unmarshal(slOrderIDsRaw, &trade.SLOrderIDs)
		}
		if len(trade.SLOrderIDs) == 0 {
			continue
		}
		trades = append(trades, trade)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate open trades for sl polling: %w", rows.Err())
	}

	return trades, nil
}

func (s *Store) UpdateTradeStatus(ctx context.Context, tradeID string, status string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = $2 WHERE id::text = $1 AND exit_time IS NULL`, s.tradesTable)
	_, err := s.pool.Exec(ctx, query, strings.TrimSpace(tradeID), strings.TrimSpace(status))
	if err != nil {
		return fmt.Errorf("update trade status: %w", err)
	}
	return nil
}

func (s *Store) UpdateTrailingStateByTradeID(ctx context.Context, tradeID string, stoploss *float64, slLimit *float64, spotTrailAnchor *float64) error {
	if strings.TrimSpace(tradeID) == "" {
		return fmt.Errorf("trade id is required")
	}

	query := fmt.Sprintf(`
UPDATE %s
SET
    stoploss = COALESCE($2::numeric, stoploss),
    sl_limit = COALESCE($3::numeric, sl_limit),
    spot_trail_anchor = COALESCE($4::numeric, spot_trail_anchor)
WHERE exit_time IS NULL
  AND id::text = $1`, s.tradesTable)

	if _, err := s.pool.Exec(ctx, query, strings.TrimSpace(tradeID), nullFloatPtr(stoploss), nullFloatPtr(slLimit), nullFloatPtr(spotTrailAnchor)); err != nil {
		return fmt.Errorf("update trailing state by trade id: %w", err)
	}

	return nil
}

func (s *Store) MarkTradeClosedFromSL(ctx context.Context, tradeID string, exitPrice float64, additionalTaxes float64, exitStatus string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin close trade transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`
SELECT
    COALESCE(bot_name, ''),
    UPPER(COALESCE(side, 'BUY')),
    COALESCE(qty, 0),
    COALESCE(entry_price, 0)::float8,
    COALESCE(taxes, 0)::float8
FROM %s
WHERE id::text = $1
FOR UPDATE`, s.tradesTable)

	var botName string
	var side string
	var qty int
	var entryPrice float64
	var currentTaxes float64
	if err := tx.QueryRow(ctx, query, strings.TrimSpace(tradeID)).Scan(&botName, &side, &qty, &entryPrice, &currentTaxes); err != nil {
		return fmt.Errorf("load trade for closure: %w", err)
	}

	if qty < 0 {
		qty = int(math.Abs(float64(qty)))
	}

	totalTaxes := currentTaxes + math.Max(0, additionalTaxes)
	pnl := calculatePNL(side, entryPrice, exitPrice, qty, totalTaxes)

	if strings.TrimSpace(exitStatus) == "" {
		exitStatus = "STOPLOSS HIT"
	}

	updateQuery := fmt.Sprintf(`
UPDATE %s
SET
    status = $2,
    exit_price = $3,
    taxes = $4,
    pnl = $5,
    exit_time = NOW()
WHERE id::text = $1
  AND exit_time IS NULL`, s.tradesTable)

	result, err := tx.Exec(ctx, updateQuery, strings.TrimSpace(tradeID), exitStatus, exitPrice, totalTaxes, pnl)
	if err != nil {
		return fmt.Errorf("mark trade closed: %w", err)
	}
	if result.RowsAffected() == 0 {
		return nil
	}

	now := time.Now().In(s.loc)
	botName = normalizeBotName(botName)
	monthYear := normalizeMonthYear("", now)
	currentDate := now.Format("02-01-2006")

	initCash, err := s.resolveAccountInitCashTx(ctx, tx, botName, monthYear, nil)
	if err != nil {
		return err
	}

	var accountID string
	if err := tx.QueryRow(ctx, `
INSERT INTO accounts (botname, month_year, init_cash, current_date, profit, max_drawdown)
VALUES ($1, $2, $3, $4, $5, CASE WHEN $5 < 0 THEN $5 ELSE 0 END)
ON CONFLICT (botname, month_year, current_date)
DO UPDATE SET
    init_cash = CASE
        WHEN COALESCE(accounts.init_cash, 0) = 0 AND EXCLUDED.init_cash > 0 THEN EXCLUDED.init_cash
        ELSE accounts.init_cash
    END,
    profit = COALESCE(accounts.profit, 0) + EXCLUDED.profit,
    max_drawdown = LEAST(COALESCE(accounts.max_drawdown, 0), COALESCE(accounts.profit, 0) + EXCLUDED.profit)
RETURNING id::text
`, botName, monthYear, initCash, currentDate, pnl).Scan(&accountID); err != nil {
		return fmt.Errorf("upsert account daily pnl: %w", err)
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET account_id = $2 WHERE id::text = $1`, s.tradesTable), strings.TrimSpace(tradeID), accountID); err != nil {
		return fmt.Errorf("link trade to account: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit close trade transaction: %w", err)
	}

	return nil
}

func calculatePNL(side string, entryPrice float64, exitPrice float64, qty int, totalTaxes float64) float64 {
	gross := 0.0
	if strings.EqualFold(strings.TrimSpace(side), "SELL") {
		gross = (entryPrice - exitPrice) * float64(qty)
	} else {
		gross = (exitPrice - entryPrice) * float64(qty)
	}
	return gross - totalTaxes
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

func nullStringPtr(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
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

func (s *Store) resolveAccountInitCash(ctx context.Context, botName string, monthYear string, requested *float64) (float64, error) {
	var existing float64
	if err := s.pool.QueryRow(ctx, `
SELECT COALESCE(MAX(init_cash), 0)::float8
FROM accounts
WHERE botname = $1
  AND month_year = $2`, botName, monthYear).Scan(&existing); err != nil {
		return 0, fmt.Errorf("resolve account init_cash: %w", err)
	}

	return resolveInitCashValue(requested, existing, s.accountInitialCash), nil
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
