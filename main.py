# main.py
# Polymarket "Up or Down 15m" ladder bot (ETH/BTC/SOL/XRP)
# Strategy:
# - For each new 15m window (market slug), at start: buy BASE_USDC of BOTH YES and NO (hedge).
# - Every EVAL_INTERVAL_SEC (default 120s) for EVAL_COUNT times (default 6):
#     - Look at how prices moved since last eval (mid price).
#     - Buy STEP_USDC more of the side that increased more (momentum / “more likely”).
# - At EXIT_AT_SEC after start (default 780s = 13 min): sell ALL positions (YES and NO).
#
# Notes:
# - Uses Gamma API to discover active 15m markets.
# - Uses py-clob-client to read order books + place orders.
# - Works even if you do NOT have API creds env vars: it will derive API creds using your wallet.

import os
import time
import json
import math
import logging
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, List

import requests

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import BUY, SELL
from py_clob_client.exceptions import PolyApiException


# -----------------------------
# Config helpers
# -----------------------------
def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    return v if v is not None else ""


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, str(default)).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_json_list(name: str, default: List[str]) -> List[str]:
    raw = _env(name, "")
    if not raw:
        return default
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x).strip() for x in v]
    except Exception:
        pass
    # allow "ETH,BTC,SOL"
    return [x.strip() for x in raw.split(",") if x.strip()]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso8601(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        # Gamma returns ...Z
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("updown15m-ladder")


# -----------------------------
# Env / Params
# -----------------------------
CLOB_HOST = _env("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = _env("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = _env_int("CHAIN_ID", 137)

PM_PRIVATE_KEY = _env("PM_PRIVATE_KEY", "")
PM_FUNDER = _env("PM_FUNDER", "")
PM_SIGNATURE_TYPE = _env_int("PM_SIGNATURE_TYPE", 1)

ASSETS = _env_json_list("ASSETS", ["ETH", "BTC", "SOL", "XRP"])

POLL_SEC = _env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = _env_float("RESOLVE_EVERY_SEC", 15.0)

# Market selection
FETCH_LIMIT = _env_int("FETCH_LIMIT", 200)          # how many Gamma markets to fetch per asset per refresh
MAX_SLUG_LOOKUPS = _env_int("MAX_SLUG_LOOKUPS", 50) # when using event slugs fallback
MIN_TTE_SELECT_SEC = _env_float("MIN_TTE_SELECT_SEC", 10.0)     # don't pick markets that end in <10s
MAX_TTE_SELECT_SEC = _env_float("MAX_TTE_SELECT_SEC", 900.0)    # don't pick markets ending later than 15m
MIN_TIME_TO_END_SEC = _env_float("MIN_TIME_TO_END_SEC", 60.0)   # if less than this, don't start new cycle

# Execution / risk
MAX_SLIPPAGE = _env_float("MAX_SLIPPAGE", 0.02)
MAX_RETRIES = _env_int("MAX_RETRIES", 3)
FILL_TIMEOUT_SEC = _env_float("FILL_TIMEOUT_SEC", 10.0)

MAX_TRADES_PER_MIN = _env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = _env_float("COOLDOWN_SEC", 0.25)

# Ladder strategy
BASE_USDC = _env_float("BASE_USDC", 1.0)
STEP_USDC = _env_float("STEP_USDC", 1.0)
EVAL_INTERVAL_SEC = _env_int("EVAL_INTERVAL_SEC", 120)  # every 2 min
EVAL_COUNT = _env_int("EVAL_COUNT", 6)                  # 6 evals => 12 minutes
EXIT_AT_SEC = _env_int("EXIT_AT_SEC", 780)              # 13 minutes after start

# Close safety (if you want earlier sell)
CLOSE_HARD_SEC = _env_int("CLOSE_HARD_SEC", 30)          # don't place new orders if end < 30s

DEBUG = _env_bool("DEBUG", False)


# -----------------------------
# Data model
# -----------------------------
@dataclass
class MarketInfo:
    asset: str
    slug: str
    condition_id: str
    token_yes: str
    token_no: str
    start_ts: float
    end_ts: float


@dataclass
class PositionState:
    yes_shares: float = 0.0
    no_shares: float = 0.0
    last_mid_yes: Optional[float] = None
    last_mid_no: Optional[float] = None
    evals_done: int = 0
    opened: bool = False
    closed: bool = False
    market: Optional[MarketInfo] = None
    last_action_ts: float = 0.0


state: Dict[str, PositionState] = {a: PositionState() for a in ASSETS}


# -----------------------------
# Rate limiting
# -----------------------------
class TradeRateLimiter:
    def __init__(self, max_per_min: int, cooldown_sec: float):
        self.max_per_min = max_per_min
        self.cooldown_sec = cooldown_sec
        self._events: List[float] = []
        self._last_ts = 0.0

    def allow(self) -> bool:
        t = time.time()
        # cooldown
        if t - self._last_ts < self.cooldown_sec:
            return False
        # sliding 60s window
        self._events = [x for x in self._events if t - x < 60.0]
        if len(self._events) >= self.max_per_min:
            return False
        self._events.append(t)
        self._last_ts = t
        return True


limiter = TradeRateLimiter(MAX_TRADES_PER_MIN, COOLDOWN_SEC)


# -----------------------------
# CLOB init
# -----------------------------
def init_clob_client() -> ClobClient:
    if not PM_PRIVATE_KEY or not PM_FUNDER:
        raise RuntimeError("Missing PM_PRIVATE_KEY or PM_FUNDER in Railway env vars.")

    # py-clob-client expects "key=" (private key) not "private_key="
    client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PM_PRIVATE_KEY,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FUNDER,
    )

    # If you don't have API creds env vars, derive them (this matches Polymarket docs workflow).
    # Different versions expose this with slightly different names; we try both.
    try:
        if hasattr(client, "create_or_derive_api_creds"):
            client.create_or_derive_api_creds()
            log.info("[AUTH] derived API creds via create_or_derive_api_creds()")
        elif hasattr(client, "derive_api_creds"):
            client.derive_api_creds()
            log.info("[AUTH] derived API creds via derive_api_creds()")
        else:
            log.info("[AUTH] no API-cred derivation method found on client; assuming creds already present.")
    except Exception as e:
        # Derivation might fail if the account isn't properly set up; but your previous bot worked,
        # so if it fails now, it’s almost always a key/funder mismatch.
        raise RuntimeError(f"Failed to derive API creds. Check PM_PRIVATE_KEY/PM_FUNDER. Error: {e}")

    return client


# -----------------------------
# Gamma helpers
# -----------------------------
def gamma_get(path: str, params: Dict) -> dict:
    url = f"{GAMMA_HOST.rstrip('/')}{path}"
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _asset_prefixes(asset: str) -> List[str]:
    # Slugs vary between "bitcoin-up-or-down-..." and "btc-updown-15m-..."
    a = asset.upper()
    if a == "ETH":
        return ["eth-updown-15m-", "ethereum-up-or-down"]
    if a == "BTC":
        return ["btc-updown-15m-", "bitcoin-up-or-down"]
    if a == "SOL":
        return ["sol-updown-15m-", "solana-up-or-down"]
    if a == "XRP":
        return ["xrp-updown-15m-", "xrp-up-or-down"]
    return [a.lower() + "-updown-15m-"]


def find_active_updown15m_market(asset: str) -> Optional[MarketInfo]:
    """
    Robust discovery:
    1) Pull a chunk of markets from Gamma (active-ish, orderbook enabled).
    2) Filter by slug prefix.
    3) Select the market whose endDate is soonest in the future, within [MIN_TTE_SELECT_SEC, MAX_TTE_SELECT_SEC].
    """
    prefixes = _asset_prefixes(asset)

    # Gamma markets endpoint returns list; exact path is stable in Polymarket docs.
    # We keep the filter logic permissive because Gamma sometimes doesn't mark "active" consistently.
    try:
        data = gamma_get(
            "/markets",
            {
                "limit": FETCH_LIMIT,
                "offset": 0,
                "closed": "false",
                "enableOrderBook": "true",
                "sort": "endDate",
                "order": "asc",
            },
        )
    except Exception as e:
        log.warning("[GAMMA] /markets fetch failed: %s", e)
        return None

    markets = data if isinstance(data, list) else data.get("markets") or data.get("data") or []
    if not isinstance(markets, list):
        return None

    tnow = now_utc().timestamp()

    candidates: List[MarketInfo] = []
    for m in markets:
        slug = str(m.get("slug") or "")
        if not slug:
            continue
        if not any(slug.startswith(p) for p in prefixes):
            continue

        if str(m.get("closed")).lower() == "true":
            continue
        if str(m.get("enableOrderBook")).lower() != "true":
            continue

        end_dt = parse_iso8601(str(m.get("endDate") or ""))
        if not end_dt:
            continue
        end_ts = end_dt.timestamp()
        tte = end_ts - tnow
        if tte < MIN_TTE_SELECT_SEC or tte > MAX_TTE_SELECT_SEC:
            continue

        clob_token_ids = m.get("clobTokenIds") or []
        if not isinstance(clob_token_ids, list) or len(clob_token_ids) < 2:
            continue

        cond = str(m.get("conditionId") or "")
        if not cond:
            continue

        # Start of the 15m window is end - 900 seconds
        start_ts = end_ts - 900.0

        # For binary: token 0 is "YES"/Up and token 1 is "NO"/Down for these markets in practice.
        # If Polymarket flips ordering someday, you can detect it by reading the outcomes field,
        # but for up/down 15m slugs this convention matches what you've been seeing in logs.
        token_yes = str(clob_token_ids[0])
        token_no = str(clob_token_ids[1])

        candidates.append(
            MarketInfo(
                asset=asset,
                slug=slug,
                condition_id=cond,
                token_yes=token_yes,
                token_no=token_no,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )

    if not candidates:
        return None

    # choose the nearest endDate in the future
    candidates.sort(key=lambda x: x.end_ts)
    chosen = candidates[0]

    if DEBUG:
        log.info(
            "[MARKET] %s picked slug=%s tte=%.1fs start=%s end=%s",
            asset,
            chosen.slug,
            chosen.end_ts - tnow,
            dt.datetime.fromtimestamp(chosen.start_ts, tz=dt.timezone.utc).isoformat(),
            dt.datetime.fromtimestamp(chosen.end_ts, tz=dt.timezone.utc).isoformat(),
        )

    return chosen


# -----------------------------
# Order book helpers
# -----------------------------
def best_bid_ask(clob: ClobClient, token_id: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (best_bid, best_ask) for token_id.
    """
    try:
        ob = clob.get_order_book(token_id)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return best_bid, best_ask
    except Exception as e:
        log.warning("[OBOOK] token=%s failed: %s", token_id, e)
        return None, None


def mid_price(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None and ask is None:
        return None
    if bid is None:
        return ask
    if ask is None:
        return bid
    return (bid + ask) / 2.0


def clamp_price(p: float) -> float:
    # Polymarket prices are in [0.0, 1.0]
    return max(0.0001, min(0.9999, p))


def to_shares(usdc: float, price: float) -> float:
    # shares = dollars / price
    price = max(price, 0.0001)
    return usdc / price


# -----------------------------
# Trading helpers (limit orders)
# -----------------------------
def place_limit_order(
    clob: ClobClient,
    token_id: str,
    side: str,
    usdc_amount: float,
    ref_price: float,
) -> Optional[dict]:
    """
    Places a limit order targeting usdc_amount notional.
    For BUY: price = ask * (1 + MAX_SLIPPAGE)
    For SELL: price = bid * (1 - MAX_SLIPPAGE)
    """
    if usdc_amount <= 0:
        return None

    if not limiter.allow():
        log.info("[RATE] blocked order (rate limit)")
        return None

    price = clamp_price(ref_price)
    if side == BUY:
        price = clamp_price(price * (1.0 + MAX_SLIPPAGE))
    else:
        price = clamp_price(price * (1.0 - MAX_SLIPPAGE))

    size = to_shares(usdc_amount, price)

    args = OrderArgs(
        price=round(price, 4),
        size=round(size, 6),
        side=side,
        token_id=token_id,
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # create_and_post_order exists in many versions; fallback to create_order + post_order
            if hasattr(clob, "create_and_post_order"):
                resp = clob.create_and_post_order(args, orderType=OrderType.FOK)
            else:
                order = clob.create_order(args)
                resp = clob.post_order(order, orderType=OrderType.FOK)
            return resp
        except PolyApiException as e:
            log.warning("[ORDER] PolyApiException attempt=%d token=%s side=%s err=%s", attempt, token_id, side, e)
        except Exception as e:
            log.warning("[ORDER] attempt=%d token=%s side=%s err=%s", attempt, token_id, side, e)
        time.sleep(0.25)

    return None


# -----------------------------
# Strategy logic
# -----------------------------
def should_start_cycle(mkt: MarketInfo) -> bool:
    """
    Start if market has enough time left (>= MIN_TIME_TO_END_SEC),
    and we are within the window (now >= start).
    """
    t = time.time()
    if t < mkt.start_ts:
        return False
    tte = mkt.end_ts - t
    return tte >= MIN_TIME_TO_END_SEC


def cycle_elapsed(mkt: MarketInfo) -> float:
    return max(0.0, time.time() - mkt.start_ts)


def cycle_time_to_end(mkt: MarketInfo) -> float:
    return mkt.end_ts - time.time()


def open_initial_hedge(clob: ClobClient, ps: PositionState, mkt: MarketInfo):
    # Buy BASE_USDC of BOTH sides at current best ask.
    y_bid, y_ask = best_bid_ask(clob, mkt.token_yes)
    n_bid, n_ask = best_bid_ask(clob, mkt.token_no)
    y_mid = mid_price(y_bid, y_ask)
    n_mid = mid_price(n_bid, n_ask)

    if y_ask is None or n_ask is None:
        log.info("[SKIP] %s missing asks (y_ask=%s n_ask=%s)", mkt.asset, y_ask, n_ask)
        return

    # Safety: if close to end, do nothing
    if cycle_time_to_end(mkt) < CLOSE_HARD_SEC:
        log.info("[SKIP] %s too close to end to open hedge", mkt.asset)
        return

    ry = place_limit_order(clob, mkt.token_yes, BUY, BASE_USDC, y_ask)
    rn = place_limit_order(clob, mkt.token_no, BUY, BASE_USDC, n_ask)

    if ry:
        ps.yes_shares += to_shares(BASE_USDC, clamp_price(y_ask))
    if rn:
        ps.no_shares += to_shares(BASE_USDC, clamp_price(n_ask))

    ps.last_mid_yes = y_mid
    ps.last_mid_no = n_mid
    ps.opened = True
    ps.market = mkt

    log.info(
        "[OPEN] %s slug=%s hedge BASE_USDC=%.2f yes_ask=%.4f no_ask=%.4f yes_shares=%.4f no_shares=%.4f",
        mkt.asset, mkt.slug, BASE_USDC, y_ask, n_ask, ps.yes_shares, ps.no_shares
    )


def eval_and_ladder(clob: ClobClient, ps: PositionState):
    mkt = ps.market
    if not mkt:
        return

    # Stop if close to end (don’t add more)
    tte = cycle_time_to_end(mkt)
    if tte < CLOSE_HARD_SEC:
        return

    # Only evaluate every EVAL_INTERVAL_SEC
    elapsed = cycle_elapsed(mkt)
    next_eval_at = EVAL_INTERVAL_SEC * (ps.evals_done + 1)
    if elapsed < next_eval_at:
        return

    # Already did all evals
    if ps.evals_done >= EVAL_COUNT:
        return

    y_bid, y_ask = best_bid_ask(clob, mkt.token_yes)
    n_bid, n_ask = best_bid_ask(clob, mkt.token_no)
    y_mid = mid_price(y_bid, y_ask)
    n_mid = mid_price(n_bid, n_ask)

    if y_mid is None or n_mid is None:
        log.info("[EVAL] %s missing mid prices y_mid=%s n_mid=%s", mkt.asset, y_mid, n_mid)
        return
    if y_ask is None or n_ask is None:
        log.info("[EVAL] %s missing asks y_ask=%s n_ask=%s", mkt.asset, y_ask, n_ask)
        return

    # Determine which side increased more since last eval
    last_y = ps.last_mid_yes if ps.last_mid_yes is not None else y_mid
    last_n = ps.last_mid_no if ps.last_mid_no is not None else n_mid

    dy = y_mid - last_y
    dn = n_mid - last_n

    # Tie-break: pick the side with higher current mid
    pick_yes = (dy > dn) or (abs(dy - dn) < 1e-9 and y_mid >= n_mid)

    ps.last_mid_yes = y_mid
    ps.last_mid_no = n_mid
    ps.evals_done += 1

    side_name = "YES" if pick_yes else "NO"
    log.info(
        "[EVAL] %s #%d elapsed=%.1fs y_mid=%.4f n_mid=%.4f dy=%.4f dn=%.4f -> BUY %s STEP_USDC=%.2f",
        mkt.asset, ps.evals_done, elapsed, y_mid, n_mid, dy, dn, side_name, STEP_USDC
    )

    if pick_yes:
        r = place_limit_order(clob, mkt.token_yes, BUY, STEP_USDC, y_ask)
        if r:
            ps.yes_shares += to_shares(STEP_USDC, clamp_price(y_ask))
    else:
        r = place_limit_order(clob, mkt.token_no, BUY, STEP_USDC, n_ask)
        if r:
            ps.no_shares += to_shares(STEP_USDC, clamp_price(n_ask))


def close_all(clob: ClobClient, ps: PositionState):
    mkt = ps.market
    if not mkt or ps.closed:
        return

    # Sell everything at best bid (with slippage buffer)
    y_bid, y_ask = best_bid_ask(clob, mkt.token_yes)
    n_bid, n_ask = best_bid_ask(clob, mkt.token_no)

    if y_bid is None or n_bid is None:
        log.info("[CLOSE] %s missing bids (y_bid=%s n_bid=%s) will retry", mkt.asset, y_bid, n_bid)
        return

    # Convert shares back into approximate USDC notionals for selling
    # (We sell notional based on current bid * shares; using that avoids size math drift.)
    yes_usdc = max(0.0, ps.yes_shares * y_bid)
    no_usdc = max(0.0, ps.no_shares * n_bid)

    if yes_usdc > 0:
        place_limit_order(clob, mkt.token_yes, SELL, yes_usdc, y_bid)
    if no_usdc > 0:
        place_limit_order(clob, mkt.token_no, SELL, no_usdc, n_bid)

    ps.closed = True
    log.info(
        "[CLOSE] %s slug=%s sold approx yes_usdc=%.4f no_usdc=%.4f (shares yes=%.4f no=%.4f)",
        mkt.asset, mkt.slug, yes_usdc, no_usdc, ps.yes_shares, ps.no_shares
    )


def reset_if_new_market(ps: PositionState, new_mkt: MarketInfo):
    """
    If slug changed, reset the state so we can start a fresh cycle.
    """
    if not ps.market:
        return
    if ps.market.slug != new_mkt.slug:
        log.info("[RESET] %s switching market %s -> %s", new_mkt.asset, ps.market.slug, new_mkt.slug)
        ps.yes_shares = 0.0
        ps.no_shares = 0.0
        ps.last_mid_yes = None
        ps.last_mid_no = None
        ps.evals_done = 0
        ps.opened = False
        ps.closed = False
        ps.market = None
        ps.last_action_ts = 0.0


def loop_once(clob: ClobClient, last_resolve_ts: Dict[str, float]):
    t = time.time()

    for asset in ASSETS:
        ps = state[asset]

        # Refresh market info periodically
        if (t - last_resolve_ts.get(asset, 0.0)) >= RESOLVE_EVERY_SEC or ps.market is None:
            mkt = find_active_updown15m_market(asset)
            last_resolve_ts[asset] = t
            if not mkt:
                log.info("[MARKET] %s no active 15m market found", asset)
                continue

            # If we were tracking something else, reset
            if ps.market and ps.market.slug != mkt.slug:
                reset_if_new_market(ps, mkt)

            # Attach market if not attached
            if ps.market is None:
                ps.market = mkt

            # Log selection
            tte = mkt.end_ts - t
            log.info(
                "[MARKET] %s slug=%s tte=%.1fs start=%s end=%s yes=%s no=%s",
                asset,
                mkt.slug,
                tte,
                dt.datetime.fromtimestamp(mkt.start_ts, tz=dt.timezone.utc).strftime("%H:%M:%S"),
                dt.datetime.fromtimestamp(mkt.end_ts, tz=dt.timezone.utc).strftime("%H:%M:%S"),
                mkt.token_yes,
                mkt.token_no,
            )

        mkt = ps.market
        if not mkt:
            continue

        # If not within tradable window, skip
        if time.time() < mkt.start_ts:
            continue

        # If we’re too close to market end, only try closing if not closed
        if cycle_time_to_end(mkt) <= CLOSE_HARD_SEC:
            if ps.opened and not ps.closed:
                close_all(clob, ps)
            continue

        # Start cycle (open hedge) only if it makes sense
        if not ps.opened:
            if should_start_cycle(mkt):
                open_initial_hedge(clob, ps, mkt)
            else:
                # Too early (before start) or too late (near end)
                continue

        # Ladder evaluations
        if ps.opened and not ps.closed:
            eval_and_ladder(clob, ps)

        # Exit condition: EXIT_AT_SEC after start
        if ps.opened and not ps.closed:
            elapsed = cycle_elapsed(mkt)
            if elapsed >= EXIT_AT_SEC:
                close_all(clob, ps)


def main():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info("CLOB_HOST=%s CHAIN_ID=%s GAMMA_HOST=%s", CLOB_HOST, CHAIN_ID, GAMMA_HOST)
    log.info("ASSETS=%s", ASSETS)
    log.info("POLL_SEC=%.1f RESOLVE_EVERY_SEC=%.1f MAX_SLUG_LOOKUPS=%d", POLL_SEC, RESOLVE_EVERY_SEC, MAX_SLUG_LOOKUPS)
    log.info(
        "LADDER: BASE_USDC=%.2f STEP_USDC=%.2f EVAL_INTERVAL_SEC=%d EVAL_COUNT=%d EXIT_AT_SEC=%d",
        BASE_USDC, STEP_USDC, EVAL_INTERVAL_SEC, EVAL_COUNT, EXIT_AT_SEC
    )
    log.info(
        "EXEC: MAX_SLIPPAGE=%.3f MAX_RETRIES=%d MAX_TRADES_PER_MIN=%d COOLDOWN_SEC=%.2f",
        MAX_SLIPPAGE, MAX_RETRIES, MAX_TRADES_PER_MIN, COOLDOWN_SEC
    )

    clob = init_clob_client()

    last_resolve_ts: Dict[str, float] = {}
    while True:
        try:
            loop_once(clob, last_resolve_ts)
        except Exception as e:
            log.exception("[LOOP] error: %s", e)
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
