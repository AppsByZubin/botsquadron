#!/usr/bin/env python3
"""
Quick PnL / trade analytics for your MockOrderSystem logs.

- Reads:
    * daily_pnl.csv         (your DAILY_PNL path)
    * orders.csv            (your ORDER_LOG path)
    * order_events.jsonl    (optional, for debugging / timeline view)

- Plots:
    * Equity curve
    * Daily PnL bar chart
    * Trade PnL histogram

Usage examples:

    python pnl_analysis.py \
        --orders-csv path/to/orders.csv \
        --daily-csv path/to/daily_pnl.csv \
        --events-json order_events.jsonl

If you wired your MockOrderSystem with constants.ORDER_LOG / constants.DAILY_PNL,
just point to those files.
"""

import argparse
import json
import os
from dataclasses import dataclass
from typing import Optional, List

import pandas as pd
import matplotlib.pyplot as plt


# ------------- Simple stats helpers ------------- #

@dataclass
class TradeStats:
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    gross_pnl: float
    avg_pnl: float
    best_trade: float
    worst_trade: float
    max_drawdown: float


def compute_trade_stats(pnl_series: pd.Series) -> TradeStats:
    pnl = pnl_series.dropna()
    total = len(pnl)
    if total == 0:
        return TradeStats(0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    wins = (pnl > 0).sum()
    losses = (pnl < 0).sum()
    win_rate = wins / total * 100.0
    gross_pnl = pnl.sum()
    avg_pnl = pnl.mean()
    best_trade = pnl.max()
    worst_trade = pnl.min()

    # Simple max drawdown on cumulative PnL
    equity = pnl.cumsum()
    roll_max = equity.cummax()
    drawdown = equity - roll_max
    max_dd = drawdown.min()  # negative value

    return TradeStats(
        total_trades=total,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        gross_pnl=gross_pnl,
        avg_pnl=avg_pnl,
        best_trade=best_trade,
        worst_trade=worst_trade,
        max_drawdown=max_dd,
    )


# ------------- Plot helpers ------------- #

def plot_equity_and_daily_pnl(daily_df: pd.DataFrame, title: str = "Equity Curve & Daily PnL"):
    if daily_df.empty:
        print("[INFO] daily_pnl CSV is empty, skipping equity curve.")
        return

    # Ensure sorted by date
    daily_df = daily_df.sort_values("date")

    # If equity column not present, build from daily_pnl assuming initial cash=0
    if "equity" not in daily_df.columns:
        daily_df["equity"] = daily_df["daily_pnl"].cumsum()

    fig, ax1 = plt.subplots(figsize=(10, 6))
    plt.title(title)

    # Equity curve (left axis)
    ax1.plot(daily_df["date"], daily_df["equity"], marker="o")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Equity")

    # Daily PnL as bar (right axis)
    ax2 = ax1.twinx()
    ax2.bar(daily_df["date"], daily_df["daily_pnl"], alpha=0.3)
    ax2.set_ylabel("Daily PnL")

    fig.autofmt_xdate(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_trade_pnl_hist(pnl_series: pd.Series, title: str = "Trade PnL Distribution"):
    pnl = pnl_series.dropna()
    if pnl.empty:
        print("[INFO] No closed trades with PnL found, skipping histogram.")
        return

    plt.figure(figsize=(8, 5))
    plt.hist(pnl, bins=30)
    plt.title(title)
    plt.xlabel("Per-trade PnL")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()


# ------------- Event log loader (optional) ------------- #

def load_events(events_path: str) -> pd.DataFrame:
    rows: List[dict] = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def print_recent_events(events_df: pd.DataFrame, n: int = 20):
    if events_df.empty:
        print("[INFO] No events to show.")
        return
    # Show last n events
    recent = events_df.tail(n)
    print("\n=== Last events ===")
    print(recent[["ts", "event_type", "symbol", "side", "qty", "status", "pnl"]]
          if "pnl" in recent.columns
          else recent[["ts", "event_type", "symbol", "side", "qty", "status"]])


# ------------- Main ------------- #

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze PnL & trades from MockOrderSystem logs")
    p.add_argument("--orders-csv", required=True, help="Path to orders CSV (ORDER_LOG)")
    p.add_argument("--daily-csv", required=True, help="Path to daily PnL CSV (DAILY_PNL)")
    p.add_argument("--events-json", default=None, help="Path to order_events.jsonl (optional)")
    p.add_argument("--no-plots", action="store_true", help="Disable plotting, only print stats")
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.orders_csv):
        raise SystemExit(f"[ERROR] orders CSV not found: {args.orders_csv}")
    if not os.path.exists(args.daily_csv):
        raise SystemExit(f"[ERROR] daily_pnl CSV not found: {args.daily_csv}")

    # --- Load daily_pnl ---
    daily_df = pd.read_csv(args.daily_csv)
    print("=== Daily PnL summary ===")
    print(daily_df)

    # --- Load orders and closed trades ---
    orders_df = pd.read_csv(args.orders_csv)
    # Closed trades – whatever statuses you use for exits
    closed_mask = orders_df["status"].isin(["TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT", "CLOSE", "CLOSED"])
    closed_trades = orders_df[closed_mask]

    print("\n=== Closed trades (head) ===")
    print(closed_trades.head())

    # --- Compute stats ---
    stats = compute_trade_stats(closed_trades["pnl"])
    print("\n=== Trade stats ===")
    print(f"Total trades   : {stats.total_trades}")
    print(f"Wins / Losses  : {stats.wins} / {stats.losses}")
    print(f"Win rate       : {stats.win_rate:.2f}%")
    print(f"Gross PnL      : {stats.gross_pnl:.2f}")
    print(f"Avg PnL/trade  : {stats.avg_pnl:.2f}")
    print(f"Best trade     : {stats.best_trade:.2f}")
    print(f"Worst trade    : {stats.worst_trade:.2f}")
    print(f"Max drawdown   : {stats.max_drawdown:.2f}")

    # --- Optional: load events ---
    if args.events_json and os.path.exists(args.events_json):
        events_df = load_events(args.events_json)
        print_recent_events(events_df, n=20)
    else:
        events_df = None

    # --- Plots ---
    if not args.no_plots:
        plot_equity_and_daily_pnl(daily_df, title="Equity Curve & Daily PnL")
        plot_trade_pnl_hist(closed_trades["pnl"], title="Trade PnL Distribution")


if __name__ == "__main__":
    main()
