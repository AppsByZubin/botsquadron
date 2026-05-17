#!/usr/bin/env python3
"""Generate quick trade / PnL summaries from your order logs.

This script is intended as a lightweight utility for producing:
  * overall trade summary
  * weekly summary (last 7 days)
  * monthly summary (calendar month)

It works entirely from the CSV logs produced by `UpstoxOrderManager`.

Usage examples:
    python order_summary.py \
        --orders-csv files/execution_results/prod/order_log.csv \
        --daily-csv files/execution_results/prod/daily_pnl.csv

"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

IST = ZoneInfo("Asia/Kolkata")


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


def _parse_date(s: str) -> datetime.date:
    return datetime.fromisoformat(s).date()


def _compute_trade_stats(pnl_series: pd.Series) -> TradeStats:
    pnl = pnl_series.dropna().astype(float)
    total = len(pnl)
    if total == 0:
        return TradeStats(0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)

    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    win_rate = (wins / total * 100.0) if total else 0.0
    gross_pnl = float(pnl.sum())
    avg_pnl = float(pnl.mean())
    best_trade = float(pnl.max())
    worst_trade = float(pnl.min())

    return TradeStats(
        total_trades=total,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        gross_pnl=gross_pnl,
        avg_pnl=avg_pnl,
        best_trade=best_trade,
        worst_trade=worst_trade,
    )


def _format_stats(stats: TradeStats) -> str:
    return (
        f"Trades: {stats.total_trades:>4} | "
        f"W/L: {stats.wins:>3}/{stats.losses:<3} | "
        f"Win%: {stats.win_rate:>5.1f}% | "
        f"PnL: {stats.gross_pnl:>10.2f} | "
        f"Avg: {stats.avg_pnl:>8.2f} | "
        f"Best: {stats.best_trade:>8.2f} | "
        f"Worst: {stats.worst_trade:>8.2f}"
    )


def _parse_mixed_datetime_column(series: pd.Series) -> pd.Series:
    """Parse timezone-aware and naive timestamps into naive IST datetimes."""

    def _normalize(value: object) -> pd.Timestamp:
        if pd.isna(value):
            return pd.NaT

        raw = str(value).strip()
        if not raw:
            return pd.NaT

        try:
            ts = pd.Timestamp(raw)
        except (TypeError, ValueError):
            return pd.NaT

        if ts.tzinfo is not None:
            return ts.tz_convert(IST).tz_localize(None)
        return ts

    return pd.to_datetime(series.apply(_normalize), errors="coerce")


def _load_orders(orders_csv: str) -> pd.DataFrame:
    df = pd.read_csv(orders_csv)
    if "exit_time" in df.columns:
        df["exit_time"] = _parse_mixed_datetime_column(df["exit_time"])
    if "timestamp" in df.columns:
        df["timestamp"] = _parse_mixed_datetime_column(df["timestamp"])
    return df


def _load_daily(daily_csv: str) -> pd.DataFrame:
    df = pd.read_csv(daily_csv)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.date
    return df


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)


def _summarize_trade_period(
    orders_df: pd.DataFrame,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    label: str = "",
) -> None:
    # Consider only closed trades (any status other than OPEN)
    closed_mask = ~orders_df["status"].astype(str).str.upper().isin(["OPEN"])
    closed = orders_df[closed_mask].copy()

    if start is not None or end is not None:
        if start is not None:
            closed = closed[closed["exit_time"] >= start]
        if end is not None:
            closed = closed[closed["exit_time"] < end]

    stats = _compute_trade_stats(closed.get("pnl", pd.Series(dtype="float")))
    print(f"{label} (trades {len(closed)})")
    print(_format_stats(stats))


def _summarize_daily_period(
    daily_df: pd.DataFrame, start: Optional[datetime.date], end: Optional[datetime.date], label: str
) -> None:
    mask = pd.Series([True] * len(daily_df))
    if start is not None:
        mask &= daily_df["date"] >= start
    if end is not None:
        mask &= daily_df["date"] < end

    period = daily_df[mask].copy()
    if period.empty:
        print(f"{label}: no daily rows")
        return

    total_pnl = float(period["pnl"].sum())
    last_row = period.iloc[-1]
    cash = float(last_row.get("cash", 0.0))
    initial_cash = float(period.iloc[0].get("initial_cash", 0.0))
    print(f"{label}: days={len(period)} | pnl={total_pnl:.2f} | cash={cash:.2f} | initial_cash={initial_cash:.2f}")


def generate_summary(
    orders_csv: str, daily_csv: str, as_of: Optional[datetime.date] = None
) -> None:
    """Print weekly and monthly trade summaries based on order/daily logs."""
    as_of = as_of or datetime.now().date()

    orders_csv_path = Path(orders_csv)
    daily_csv_path = Path(daily_csv)

    if not orders_csv_path.exists():
        raise FileNotFoundError(f"orders CSV not found: {orders_csv_path}")
    if not daily_csv_path.exists():
        raise FileNotFoundError(f"daily CSV not found: {daily_csv_path}")

    orders_df = _load_orders(str(orders_csv_path))
    daily_df = _load_daily(str(daily_csv_path))

    _print_header("ORDER SUMMARY")
    print(f"As-of date : {as_of.isoformat()}")
    print(f"Orders CSV : {orders_csv_path}")
    print(f"Daily CSV  : {daily_csv_path}")

    # --- Overall ---
    _print_header("Overall (all time)")
    _summarize_trade_period(orders_df, label="All-time")
    _summarize_daily_period(daily_df, start=None, end=None, label="All-time daily")

    # --- Week (last 7 days) ---
    week_start = as_of - timedelta(days=7)
    _print_header("Last 7 days")
    _summarize_trade_period(
        orders_df,
        start=datetime.combine(week_start, datetime.min.time()),
        end=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
        label=f"Weekly ({week_start} -> {as_of})",
    )
    _summarize_daily_period(
        daily_df, start=week_start, end=as_of + timedelta(days=1), label="Weekly daily"
    )

    # --- Month (calendar month containing as_of) ---
    month_start = as_of.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    _print_header("Calendar month")
    _summarize_trade_period(
        orders_df,
        start=datetime.combine(month_start, datetime.min.time()),
        end=datetime.combine(next_month, datetime.min.time()),
        label=f"Monthly ({month_start} -> {next_month - timedelta(days=1)})",
    )
    _summarize_daily_period(
        daily_df, start=month_start, end=next_month, label="Monthly daily")


def main() -> None:
    p = argparse.ArgumentParser(description="Order summary helper (weekly/monthly stats)")
    p.add_argument("--orders-csv", required=True, help="Path to order_log.csv")
    p.add_argument("--daily-csv", required=True, help="Path to daily_pnl.csv")
    p.add_argument(
        "--as-of",
        default=None,
        help="Reference date (ISO) for weekly/monthly summaries; defaults to today",
    )
    args = p.parse_args()

    as_of_date = datetime.fromisoformat(args.as_of).date() if args.as_of else None
    generate_summary(args.orders_csv, args.daily_csv, as_of=as_of_date)


if __name__ == "__main__":
    main()
