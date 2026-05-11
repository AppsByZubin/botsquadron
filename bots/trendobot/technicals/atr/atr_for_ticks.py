from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Deque, Tuple
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

@dataclass
class Candle:
    ts_minute: datetime  # minute bucket (09:15:00)
    o: float
    h: float
    l: float
    c: float

@dataclass
class PerSymbolState:
    cur: Optional[Candle] = None
    candles: Deque[Candle] = field(default_factory=lambda: deque(maxlen=50))
    atr5: Optional[float] = None
    prev_close: Optional[float] = None

def minute_bucket(ts: datetime) -> datetime:
    ts = ts.astimezone(IST) if ts.tzinfo else ts.replace(tzinfo=IST)
    return ts.replace(second=0, microsecond=0)

def wilder_atr_update(prev_atr: Optional[float], tr: float, period: int) -> float:
    # First ATR = simple average of first N TRs is ideal,
    # but for streaming, we can bootstrap with EMA-like start.
    if prev_atr is None:
        return tr
    return (prev_atr * (period - 1) + tr) / period

def true_range(h: float, l: float, prev_c: Optional[float]) -> float:
    if prev_c is None:
        return h - l
    return max(h - l, abs(h - prev_c), abs(l - prev_c))

class AtrEngine:
    def __init__(self, atr_period: int = 2):
        self.atr_period = atr_period
        self.state: Dict[str, PerSymbolState] = {}

    def on_tick(self, instrument_key: str, ltp: float, ts: datetime) -> None:
        st = self.state.setdefault(instrument_key, PerSymbolState())
        mb = minute_bucket(ts)

        if st.cur is None:
            st.cur = Candle(ts_minute=mb, o=ltp, h=ltp, l=ltp, c=ltp)
            return

        # same minute → update candle
        if st.cur.ts_minute == mb:
            st.cur.h = max(st.cur.h, ltp)
            st.cur.l = min(st.cur.l, ltp)
            st.cur.c = ltp
            return

        # minute changed → close previous candle, compute ATR update
        closed = st.cur
        st.candles.append(closed)

        tr = true_range(closed.h, closed.l, st.prev_close)
        st.atr5 = wilder_atr_update(st.atr5, tr, self.atr_period)
        st.prev_close = closed.c

        # start new candle
        st.cur = Candle(ts_minute=mb, o=ltp, h=ltp, l=ltp, c=ltp)

    def get_atr(self, instrument_key: str) -> Optional[float]:
        st = self.state.get(instrument_key)
        return None if not st else st.atr5

    def get_last_close(self, instrument_key: str) -> Optional[float]:
        st = self.state.get(instrument_key)
        return None if not st or not st.cur else st.cur.c