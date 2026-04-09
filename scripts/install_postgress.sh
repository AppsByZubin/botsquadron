#!/usr/bin/env bash
set -Eeuo pipefail

# Installs PostgreSQL on the VM, configures remote access for K3s pods,
# creates database/user, and creates the "trades" table.
#
# Example:
#   sudo DB_NAME=omsdb DB_USER=omsuser DB_PASS='change-me' \
#        POD_CIDR='10.42.0.0/16' K8S_NAMESPACE='botspace' \
#        K8S_SERVICE_NAME='postgres-oms' \
#        bash install_postgres_oms.sh
#
# Notes:
# - Default K3s pod CIDR is commonly 10.42.0.0/16; change POD_CIDR if your cluster uses a different value.
# - If the VM has multiple IPs, override NODE_IP explicitly.
# - The generated kubernetes manifest gives your OMS pod a stable in-cluster DNS name.

DB_NAME="${DB_NAME:-omsdb}"
DB_USER="${DB_USER:-omsuser}"
DB_PASS="${DB_PASS:-password123}"
PG_PORT="${PG_PORT:-5432}"
POD_CIDR="${POD_CIDR:-10.42.0.0/16}"
K8S_NAMESPACE="${K8S_NAMESPACE:-botspace}"
K8S_SERVICE_NAME="${K8S_SERVICE_NAME:-postgres-oms}"
NODE_IP="${NODE_IP:-$(ip route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')}"
BIND_ADDRESS="${BIND_ADDRESS:-*}"
INITIAL_DIR="$(pwd -P)"
OUTPUT_DIR="${OUTPUT_DIR:-$INITIAL_DIR}"

if [[ "$OUTPUT_DIR" != /* ]]; then
  OUTPUT_DIR="${INITIAL_DIR}/${OUTPUT_DIR}"
fi

if [[ $EUID -ne 0 ]]; then
  echo "Run this script as root or with sudo."
  exit 1
fi

if [[ -z "$NODE_IP" ]]; then
  echo "Could not auto-detect NODE_IP. Re-run with NODE_IP=<vm_ip>."
  exit 1
fi

validate_ident() {
  local value="$1"
  local label="$2"
  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "$label must match ^[A-Za-z_][A-Za-z0-9_]*$ for this script. Got: $value"
    exit 1
  fi
}

validate_ident "$DB_NAME" "DB_NAME"
validate_ident "$DB_USER" "DB_USER"

escape_sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

DB_NAME_ESC="$(escape_sql_literal "$DB_NAME")"
DB_USER_ESC="$(escape_sql_literal "$DB_USER")"
DB_PASS_ESC="$(escape_sql_literal "$DB_PASS")"

export DEBIAN_FRONTEND=noninteractive
if dpkg -s postgresql >/dev/null 2>&1 && dpkg -s postgresql-contrib >/dev/null 2>&1; then
  echo "PostgreSQL packages already installed. Skipping package installation."
else
  apt-get update -y
  apt-get install -y postgresql postgresql-contrib
fi

systemctl enable postgresql
systemctl start postgresql

# `sudo -u postgres` preserves the current working directory. When the script is
# launched from a user home/repo path, the postgres user may not be able to
# traverse that directory, which causes noisy "could not change directory"
# warnings even though the SQL succeeds.
cd /

PG_CONF="$(sudo -u postgres psql -tAc 'SHOW config_file;' | xargs)"
PG_HBA="$(sudo -u postgres psql -tAc 'SHOW hba_file;' | xargs)"

if [[ -z "$PG_CONF" || -z "$PG_HBA" ]]; then
  echo "Failed to detect PostgreSQL config paths."
  exit 1
fi

cp -n "$PG_CONF" "${PG_CONF}.bak" || true
cp -n "$PG_HBA" "${PG_HBA}.bak" || true

# Allow TCP connections from pods by listening on the desired address.
if grep -Eq "^[#[:space:]]*listen_addresses\s*=" "$PG_CONF"; then
  sed -ri "s|^[#[:space:]]*listen_addresses\s*=.*|listen_addresses = '${BIND_ADDRESS}'|" "$PG_CONF"
else
  printf "\nlisten_addresses = '%s'\n" "$BIND_ADDRESS" >> "$PG_CONF"
fi

# Keep password auth strong.
if grep -Eq "^[#[:space:]]*password_encryption\s*=" "$PG_CONF"; then
  sed -ri "s|^[#[:space:]]*password_encryption\s*=.*|password_encryption = scram-sha-256|" "$PG_CONF"
else
  printf "\npassword_encryption = scram-sha-256\n" >> "$PG_CONF"
fi

HBA_LINE="host    ${DB_NAME}    ${DB_USER}    ${POD_CIDR}    scram-sha-256"
if ! grep -Fq "$HBA_LINE" "$PG_HBA"; then
  printf "\n# Allow OMS pod traffic from the K3s pod CIDR\n%s\n" "$HBA_LINE" >> "$PG_HBA"
fi

systemctl restart postgresql

sudo -u postgres psql <<SQL
DO \
\$\$\
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER_ESC}') THEN
    CREATE ROLE "${DB_USER}" LOGIN PASSWORD '${DB_PASS_ESC}';
  ELSE
    ALTER ROLE "${DB_USER}" WITH LOGIN PASSWORD '${DB_PASS_ESC}';
  END IF;
END
\$\$;
SQL

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME_ESC}'" | grep -q 1; then
  sudo -u postgres createdb -O "$DB_USER" "$DB_NAME"
fi

sudo -u postgres psql -d "$DB_NAME" <<SQL
GRANT ALL PRIVILEGES ON DATABASE "${DB_NAME}" TO "${DB_USER}";
GRANT USAGE, CREATE ON SCHEMA public TO "${DB_USER}";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
SQL

sudo -u postgres psql -d "$DB_NAME" <<SQL
DO \
\$\$\
BEGIN
  IF to_regclass('public."Trades"') IS NOT NULL AND to_regclass('public.trades') IS NOT NULL THEN
    RAISE EXCEPTION 'Both public."Trades" and public.trades exist. Consolidate them before rerunning this script.';
  END IF;

  IF to_regclass('public."Trades"') IS NOT NULL THEN
    ALTER TABLE "Trades" RENAME TO trades;
  END IF;
END
\$\$;
SQL

if sudo -u postgres psql -d "$DB_NAME" -tAc "SELECT to_regclass('public.trades') IS NOT NULL;" | grep -q t; then
  echo "A trades table already exists in database \"${DB_NAME}\". Skipping table creation."
else
  sudo -u postgres psql -d "$DB_NAME" <<SQL
CREATE TABLE trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

ALTER TABLE trades OWNER TO "${DB_USER}";
GRANT ALL PRIVILEGES ON TABLE trades TO "${DB_USER}";
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades ("timestamp");
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);
SQL
fi

sudo -u postgres psql -d "$DB_NAME" <<SQL
ALTER TABLE IF EXISTS trades ADD COLUMN IF NOT EXISTS taxes NUMERIC(18,6);

CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_name TEXT,
    year INTEGER,
    month INTEGER,
    initial_cash NUMERIC(18,6),
    profit NUMERIC(18,6),
    max_dradown NUMERIC(18,6)
);

ALTER TABLE accounts OWNER TO "${DB_USER}";
GRANT ALL PRIVILEGES ON TABLE accounts TO "${DB_USER}";
CREATE INDEX IF NOT EXISTS idx_accounts_bot_year_month ON accounts (bot_name, year, month);
SQL

sudo -u postgres psql -d "$DB_NAME" <<SQL
DO \
\$\$\
BEGIN
  IF to_regclass('public.trades') IS NOT NULL AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'trades'
      AND column_name = 'id'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'trades'
        AND column_name = 'id'
        AND udt_name <> 'uuid'
    ) THEN
      ALTER TABLE trades ALTER COLUMN id DROP DEFAULT;
      ALTER TABLE trades ALTER COLUMN id TYPE uuid USING gen_random_uuid();
    END IF;
    ALTER TABLE trades ALTER COLUMN id SET DEFAULT gen_random_uuid();
  END IF;

  IF to_regclass('public.accounts') IS NOT NULL AND EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'accounts'
      AND column_name = 'id'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'accounts'
        AND column_name = 'id'
        AND udt_name <> 'uuid'
    ) THEN
      ALTER TABLE accounts ALTER COLUMN id DROP DEFAULT;
      ALTER TABLE accounts ALTER COLUMN id TYPE uuid USING gen_random_uuid();
    END IF;
    ALTER TABLE accounts ALTER COLUMN id SET DEFAULT gen_random_uuid();
  END IF;
END
\$\$;
SQL

sudo -u postgres psql -d "$DB_NAME" <<SQL
ALTER TABLE IF EXISTS trades ADD COLUMN IF NOT EXISTS account_id UUID;
CREATE INDEX IF NOT EXISTS idx_trades_account_id ON trades (account_id);

DO \
\$\$\
BEGIN
  IF to_regclass('public.trades') IS NOT NULL AND NOT EXISTS (
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
\$\$;
SQL

if command -v ufw >/dev/null 2>&1; then
  if ufw status | grep -q "Status: active"; then
    ufw allow from "$POD_CIDR" to any port "$PG_PORT" proto tcp
  fi
fi

cat > "${OUTPUT_DIR}/k8s-postgres-oms.yaml" <<YAML
apiVersion: v1
kind: Service
metadata:
  name: ${K8S_SERVICE_NAME}
  namespace: ${K8S_NAMESPACE}
spec:
  ports:
    - name: postgres
      port: ${PG_PORT}
      targetPort: ${PG_PORT}
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ${K8S_SERVICE_NAME}
  namespace: ${K8S_NAMESPACE}
subsets:
  - addresses:
      - ip: ${NODE_IP}
    ports:
      - name: postgres
        port: ${PG_PORT}
---
apiVersion: v1
kind: Secret
metadata:
  name: ${K8S_SERVICE_NAME}-secret
  namespace: ${K8S_NAMESPACE}
type: Opaque
stringData:
  POSTGRES_HOST: ${K8S_SERVICE_NAME}.${K8S_NAMESPACE}.svc.cluster.local
  POSTGRES_PORT: "${PG_PORT}"
  POSTGRES_DB: ${DB_NAME}
  POSTGRES_USER: ${DB_USER}
  POSTGRES_PASSWORD: ${DB_PASS}
  DATABASE_URL: postgresql://${DB_USER}:${DB_PASS}@${K8S_SERVICE_NAME}.${K8S_NAMESPACE}.svc.cluster.local:${PG_PORT}/${DB_NAME}?sslmode=disable
YAML

cat <<EOF2
Done.

PostgreSQL config:
  config_file : ${PG_CONF}
  hba_file    : ${PG_HBA}
  node_ip     : ${NODE_IP}
  pod_cidr    : ${POD_CIDR}
  db          : ${DB_NAME}
  user        : ${DB_USER}
  port        : ${PG_PORT}

Generated Kubernetes manifest:
  ${OUTPUT_DIR}/k8s-postgres-oms.yaml

Apply it with:
  kubectl apply -f ${OUTPUT_DIR}/k8s-postgres-oms.yaml

Then in your OMS deployment, import env from the generated secret:
  envFrom:
    - secretRef:
        name: ${K8S_SERVICE_NAME}-secret

SQL check:
  sudo -u postgres psql -d ${DB_NAME} -c '\\d+ trades'
EOF2
