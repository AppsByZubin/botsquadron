import pandas as pd
import uuid
import time
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import common.constants as constants
from logger import create_logger

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("MockOrderLogger")

class MockOrderSystem:
    def __init__(self, tsl_buffer=5, orders_csv=constants.ORDER_LOG, daily_csv=constants.DAILY_PNL):
        self.orders = []
        self.positions = {}
        self.tsl_buffer = tsl_buffer
        self.orders_csv = orders_csv
        self.daily_csv = daily_csv

        # Initialize CSVs
        if not os.path.exists(orders_csv):
            pd.DataFrame(columns=[
                "id","symbol","side","qty","strike_price","type","entry_price","target","stoploss",
                "tsl_active","status","max_price","min_price","timestamp",
                "exit_price","pnl","exit_time","description"
            ]).to_csv(orders_csv, index=False)

        if not os.path.exists(daily_csv):
            pd.DataFrame(columns=["date","daily_pnl","num_trades","win_rate"]).to_csv(daily_csv, index=False)

        try:
            df = pd.read_csv(self.orders_csv)
            if not df.empty:
                open_orders = df[df["status"] == "OPEN"].to_dict(orient="records")
                self.orders.extend(open_orders)
                # Rebuild positions from open orders
                for order in open_orders:
                    sym = order["symbol"]
                    qty = order["qty"]
                    side = order["side"].upper()
                    self.positions[sym] = self.positions.get(sym, 0)
                    self.positions[sym] += qty if side == "BUY" else -qty
                logger.info(f"[INIT] Loaded {len(open_orders)} open orders from {self.orders_csv}")
        except Exception as e:
            raise Exception(f"[ERROR] Could not read order CSV: {e}")  



    def _generate_id(self):
        return str(uuid.uuid4())[:8]

    # ---------------- PLACE ORDER ---------------- #
    def place_order(self, symbol, side, qty, price, strike_price, description, target=None, stoploss=None, tsl_active=True):
        ist_time = datetime.now(ist)
        order = {
            "id": self._generate_id(),
            "symbol": symbol,
            "side": side.upper(),
            "qty": qty,
            "entry_price": price,
            "strike_price": strike_price,
            "target": target,
            "stoploss": stoploss,
            "tsl_active": tsl_active,
            "status": "OPEN",
            "max_price": price if side.upper() == "BUY" else None,
            "min_price": price if side.upper() == "SELL" else None,
            "timestamp": ist_time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_price": None,
            "pnl": None,
            "exit_time": None,
            "description": description
        }

        self.positions[symbol] = self.positions.get(symbol, 0)
        self.positions[symbol] += qty if side.lower() == "buy" else -qty

        self.orders.append(order)
        self._save_order(order)
        logger.info(f"[ORDER] {side.upper()} {qty} {symbol} @ {price} (SL={stoploss}, TGT={target})")
        return order["id"]

    # ---------------- MODIFY ORDER ---------------- #
    def modify_order(self, order_id, new_sl=None, new_target=None, new_qty=None):
        for order in self.orders:
            if order["id"] == order_id and order["status"] == "OPEN":
                if new_sl:
                    order["stoploss"] = new_sl
                if new_target:
                    order["target"] = new_target
                if new_qty:
                    order["qty"] = new_qty
                self._update_order(order)
                logger.info(f"[MODIFY] {order['symbol']} | SL={order['stoploss']} | TGT={order['target']} | QTY={order['qty']}")
                return True
        logger.warning(f"[WARN] Cannot modify order {order_id}. It may be closed or not found.")
        return False

    # ---------------- CANCEL ORDER ---------------- #
    def cancel_order(self, order_id):
        for order in self.orders:
            if order["id"] == order_id and order["status"] == "OPEN":
                order["status"] = "CANCELLED"
                self._update_order(order)
                self.positions[order["symbol"]] -= order["qty"] if order["side"] == "BUY" else -order["qty"]
                logger.info(f"[CANCEL] {order['symbol']} order cancelled.")
                return True
        logger.warning(f"[WARN] Cannot cancel order {order_id}. It may already be closed.")
        return False

    # ---------------- CHECK TARGETS & SL ---------------- #
    def check_targets(self, symbol, ltp):
        for order in self.orders:
            if order["symbol"] == symbol and order["status"] == "OPEN":
                side = order["side"]
                sl, tgt = order["stoploss"], order["target"]

                # Auto trailing SL
                if order["tsl_active"]:
                    if side == "BUY":
                        if order["max_price"] is None or ltp > order["max_price"]:
                            order["max_price"] = ltp
                            new_sl = ltp - self.tsl_buffer
                            if new_sl > sl:
                                order["stoploss"] = new_sl
                                logger.info(f"[TSL] BUY {symbol} | New SL = {new_sl}")
                    else:
                        if order["min_price"] is None or ltp < order["min_price"]:
                            order["min_price"] = ltp
                            new_sl = ltp + self.tsl_buffer
                            if new_sl < sl:
                                order["stoploss"] = new_sl
                                logger.info(f"[TSL] SELL {symbol} | New SL = {new_sl}")

                # Exit checks
                if side == "BUY":
                    if tgt and ltp >= tgt:
                        order["status"] = "TARGET HIT"
                    elif sl and ltp <= sl:
                        order["status"] = "STOPLOSS HIT"
                else:
                    if tgt and ltp <= tgt:
                        order["status"] = "TARGET HIT"
                    elif sl and ltp >= sl:
                        order["status"] = "STOPLOSS HIT"

                if order["status"] != "OPEN":
                    logger.info(f"[EXIT] {symbol} {order['status']} at LTP={ltp}")
                    self.close_position(order, ltp)

    # ---------------- MANUAL SL UPDATE ---------------- #
    def update_stoploss(self, order_id, new_sl):
        return self.modify_order(order_id, new_sl=new_sl)

    # ---------------- CLOSE POSITION ---------------- #
    def close_position(self, order, exit_price):
        ist_time = datetime.now(ist)
        side, qty, entry = order["side"], order["qty"], order["entry_price"]
        pnl = (exit_price - entry) * qty if side == "BUY" else (entry - exit_price) * qty
        order["exit_price"] = exit_price
        order["pnl"] = pnl
        order["exit_time"] = ist_time.strftime("%Y-%m-%d %H:%M:%S")
        self.positions[order["symbol"]] -= qty if side == "BUY" else -qty
        self._update_order(order)
        logger.info(f"[PnL] {order['symbol']} | {side} | Exit={exit_price} | PnL={pnl:.2f}")
        self._update_daily_pnl()

    # ---------------- SQUARE-OFF ---------------- #
    def square_off(self):
        logger.info("[SQUARE-OFF] Closing all open positions...")
        for order in self.orders:
            if order["status"] == "OPEN":
                side = order["side"]
                symbol = order["symbol"]
                qty = order["qty"]
                ltp = order["max_price"] if side == "BUY" else order["min_price"]
                if not ltp:
                    continue
                exit_price = ltp
                order["status"] = "MANUAL EXIT"
                self.close_position(order, exit_price)
        logger.info("[DONE] All open positions squared off.")

    # ---------------- CSV HELPERS ---------------- #
    def _save_order(self, order):
        df = pd.read_csv(self.orders_csv)
        df = pd.concat([df, pd.DataFrame([order])], ignore_index=True)
        df.to_csv(self.orders_csv, index=False)

    def _update_order(self, order):
        df = pd.read_csv(self.orders_csv)
        df.loc[df["id"] == order["id"], list(order.keys())] = list(order.values())
        df.to_csv(self.orders_csv, index=False)

    def _update_daily_pnl(self):
        df = pd.read_csv(self.orders_csv)
        df_closed = df[df["status"].isin(["TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT"])]
        if df_closed.empty: return
        today = datetime.now(ist).strftime("%Y-%m-%d")
        daily_trades = df_closed[df_closed["exit_time"].str.startswith(today)]
        if daily_trades.empty: return
        daily_pnl = daily_trades["pnl"].sum()
        wins = (daily_trades["pnl"] > 0).sum()
        win_rate = (wins / len(daily_trades)) * 100

        daily_df = pd.read_csv(self.daily_csv)
        if today in daily_df["date"].values:
            daily_df.loc[daily_df["date"] == today, ["daily_pnl", "num_trades", "win_rate"]] = [daily_pnl, len(daily_trades), win_rate]
        else:
            daily_df = pd.concat([daily_df, pd.DataFrame([{
                "date": today,
                "daily_pnl": daily_pnl,
                "num_trades": len(daily_trades),
                "win_rate": win_rate
            }])], ignore_index=True)
        daily_df.to_csv(self.daily_csv, index=False)
        logger.info(f"[DAILY PnL] {today}: PnL={daily_pnl:.2f}, Trades={len(daily_trades)}, Win%={win_rate:.1f}")

    # ---------------- UTILITY METHODS ---------------- #
    def get_orderbook(self):
        return pd.DataFrame(self.orders)
    
    def get_order_id(self, order_id):
        for order in self.orders:
            if order_id == order['id']:
                return order
        return None

    def get_open_order(self):
        for order in self.orders:
            if order["status"] == "OPEN":
                return order
        return None

    def is_order_long(self, order_id):
        for order in self.orders:
            if order_id == order['id'] and order["status"] == "OPEN" and order["side"] == "BUY":
                return True
        return False
    
    def is_order_short(self, order_id):
        for order in self.orders:
            if order_id == order['id'] and order["status"] == "OPEN" and order["side"] == "SELL":
                return True
        return False

    def get_positions(self):
        return self.positions

    def get_daily_summary(self):
        return pd.read_csv(self.daily_csv) if os.path.exists(self.daily_csv) else pd.DataFrame()
