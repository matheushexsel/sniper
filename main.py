import os
import re
import time
import json
import math
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import requests

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL


# -----------------------------
# Config helpers
# -----------------------------
def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_list(name: str, default: list[str]) -> list[str]:
    v = os.getenv(name)
    if not v:
        return default
    try:
        # allow ASSETS='["ETH","BTC"]'
        if v.strip().startswith("["):
            return [x.strip().strip('"').strip("'") for x in json.loads(v)]
        # allow ASSETS='ETH,BTC'
        return [x.strip() for x in v.split(",") if x.strip()]
    except Exception:
        return default


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(dt: str) -> datetime:
    # supports '2026-02-06T20:00:00Z'
    if dt.endswith("Z"):
        dt = dt[:-1] + "+00:00"
    return datetime.fromisoformat(dt)


def q_price(p: Decimal) -> str:
    # price must be sensible; clamp
    p = max(Decimal("0.001"), min(Decimal("0.999"), p))
    return str(p.quantize(Decimal("0.0001"), rounding=ROUND_DOWN))


def q_size(s: Decimal) -> str:
    # sizes are commonly accepted with 4 dp
    s = max(Decimal("0.0001"), s)
    return str(s.quantize(Decimal("0.0001"), rounding=ROUND_DOWN))


# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("updown15m_ladder")


# -----------------------------
# Env vars
# -----------------------------
CLOB_HOST = env_str("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = env_str("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = env_int("CHAIN_ID", 137)

PM_FOUNDER = env_str("PM_FOUNDER")
PM_PRIVATE_KEY = env_str("PM_PRIVATE_KEY")
PM_SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 1)  # keep as you already use

ASSETS = env_list("ASSETS", ["ETH", "BTC", "SOL", "XRP"])

POLL_SEC = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)
MAX_SLUG_LOOKUPS = env_int("MAX_SLUG_LOOKUPS", 50)

# ladder strategy
BASE_USDC = Decimal(str(env_float("BASE_USDC", 1.0)))
STEP_USDC = Decimal(str(env_float("STEP_USDC", 1.0)))
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120)
EVAL_COUNT = env_int("EVAL_COUNT", 6)
EXIT_AT_SEC = env_int("EXIT_AT_SEC", 780)  # 13 minutes from start

# execution controls
MAX_SLIPPAGE = Decimal(str(env_float("MAX_SLIPPAGE", 0.02)))
MAX_RETRIES = env_int("MAX_RETRIES", 3)
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

# optional: sell opposite side early when the market strongly moves
SELL_OPPOSITE_ON_DIVERGENCE = env_bool("SELL_OPPOSITE_ON_DIVERGENCE", True)
DIVERGENCE_THRESHOLD = Decimal(str(env_float("DIVERGENCE_THRESHOLD", 0.25)))  # e.g. yes-no >= 0.25

DEBUG = env_bool("DEBUG", False)


# -----------------------------
# Rate limiter (simple)
# -----------------------------
class RateLimiter:
    def __init__(self, max_per_min: int):
        self.max_per_min = max_per_min
        self.ts: list[float] = []

    def allow(self) -> bool:
        now = time.time()
        cutoff = now - 60.0
        self.ts = [t for t in self.ts if t >= cutoff]
        if len(self.ts) >= self.max_per_min:
            return False
        self.ts.append(now)
        return True


rl = RateLimiter(MAX_TRADES_PER_MIN)


# -----------------------------
# Polymarket clients
# -----------------------------
def init_clob_client() -> ClobClient:
    # IMPORTANT: py-clob-client expects `key=`, NOT `private_key=`
    client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PM_PRIVATE_KEY,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FOUNDER,
    )

    # Derive API creds once; this is what your working bot likely does
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    log.info("[AUTH] derived API creds via create_or_derive_api_creds()")
    return client


def gamma_get(path: str, params: dict) -> dict | None:
    url = f"{GAMMA_HOST}{path}"
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"[GAMMA] {path} fetch failed: {e}")
        if DEBUG:
            log.warning(f"[GAMMA] url={url} params={params}")
        return None


def gamma_list_markets(limit: int = 200, offset: int = 0) -> list[dict]:
    """
    Gamma is finicky. We try known-good param sets.
    If Gamma changes again, we still try to fetch *something* and then filter client-side.
    """
    attempts = [
        # Most common working filter on Gamma: active=true
        {"limit": limit, "offset": offset, "active": "true"},
        # Minimal
        {"limit": limit, "offset": offset},
        # Some deployments accept closed=false
        {"limit": limit, "offset": offset, "closed": "false"},
    ]

    for p in attempts:
        data = gamma_get("/markets", p)
        if not data:
            continue

        # Gamma sometimes returns {"markets":[...]} or directly [...]
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("markets", "data", "results"):
                if k in data and isinstance(data[k], list):
                    return data[k]

    return []


# -----------------------------
# Market selection
# -----------------------------
def slug_prefix(asset: str) -> str:
    # slug uses asset tickers in lowercase for these: eth, btc, sol, xrp
    a = asset.strip().lower()
    # Accept "BTC" but slug is "btc-updown-15m-..."
    return f"{a}-updown-15m-"


def pick_active_15m_market_for_asset(asset: str) -> dict | None:
    """
    Find a currently open 15m up/down market for this asset by scanning Gamma markets
    and choosing the one with endDate closest in the future.
    """
    prefix = slug_prefix(asset)
    markets = gamma_list_markets(limit=200, offset=0)
    if not markets:
        return None

    candidates: list[dict] = []
    for m in markets:
        slug = (m.get("slug") or "").strip()
        if not slug.startswith(prefix):
            continue

        end = m.get("endDate") or m.get("end_date") or m.get("end_date_iso")
        if not end:
            continue

        try:
            end_dt = parse_iso(end)
        except Exception:
            continue

        if end_dt <= now_utc():
            continue

        # make sure orderbook is enabled and not closed if the fields exist
        if m.get("closed") is True:
            continue
        if "enableOrderBook" in m and m.get("enableOrderBook") is not True:
            continue

        # Must have token IDs
        token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
        if not token_ids or not isinstance(token_ids, list) or len(token_ids) < 2:
            continue

        candidates.append(m)

    if not candidates:
        return None

    # pick soonest endDate in the future
    candidates.sort(key=lambda x: parse_iso(x.get("endDate")))
    return candidates[0]


def market_start_end(m: dict) -> tuple[datetime, datetime]:
    end_dt = parse_iso(m["endDate"])
    start_dt = end_dt - timedelta_seconds(15 * 60)
    return start_dt, end_dt


def timedelta_seconds(seconds: int) -> datetime:
    # helper to avoid importing timedelta separately
    return datetime.fromtimestamp(0, tz=timezone.utc) + (datetime.fromtimestamp(seconds, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))


# -----------------------------
# CLOB execution
# -----------------------------
def get_best_bid_ask(clob: ClobClient, token_id: str) -> tuple[Decimal | None, Decimal | None]:
    ob = clob.get_order_book(token_id)
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    best_bid = Decimal(str(bids[0]["price"])) if bids else None
    best_ask = Decimal(str(asks[0]["price"])) if asks else None
    return best_bid, best_ask


def place_limit(clob: ClobClient, token_id: str, side: str, price: Decimal, size: Decimal) -> bool:
    if not rl.allow():
        log.info("[RATE] blocked by MAX_TRADES_PER_MIN")
        return False

    if side not in (BUY, SELL):
        raise ValueError("side must be BUY or SELL")

    args = OrderArgs(
        price=q_price(price),
        size=q_size(size),
        side=side,
        token_id=str(token_id),
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            signed = clob.create_order(args)
            resp = clob.post_order(signed, OrderType.GTC)
            if DEBUG:
                log.info(f"[ORDER] resp={resp}")
            return True
        except Exception as e:
            log.warning(f"[ORDER] failed attempt {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(0.25 * attempt)

    return False


# -----------------------------
# Strategy state
# -----------------------------
class LadderState:
    def __init__(self):
        self.active_slug: dict[str, str] = {}  # asset -> slug
        self.start_ts: dict[str, float] = {}   # asset -> unix seconds
        self.end_ts: dict[str, float] = {}     # asset -> unix seconds

        # token ids
        self.yes_id: dict[str, str] = {}
        self.no_id: dict[str, str] = {}

        # snapshots
        self.last_yes_price: dict[str, Decimal] = {}
        self.last_no_price: dict[str, Decimal] = {}
        self.eval_n: dict[str, int] = {}
        self.next_eval_ts: dict[str, float] = {}
        self.did_initial: dict[str, bool] = {}

        # tracked position sizes (shares)
        self.pos_yes: dict[str, Decimal] = {}
        self.pos_no: dict[str, Decimal] = {}

    def reset_asset(self, asset: str):
        for d in (
            self.active_slug, self.start_ts, self.end_ts,
            self.yes_id, self.no_id,
            self.last_yes_price, self.last_no_price,
            self.eval_n, self.next_eval_ts, self.did_initial,
            self.pos_yes, self.pos_no
        ):
            d.pop(asset, None)


state = LadderState()


# -----------------------------
# Core loop
# -----------------------------
def ensure_market_bound(asset: str, m: dict):
    slug = m["slug"]
    token_ids = m["clobTokenIds"]
    # convention: token_ids[0]=YES, token_ids[1]=NO in these up/down markets (as you observed)
    yes_id, no_id = str(token_ids[0]), str(token_ids[1])

    end_dt = parse_iso(m["endDate"])
    end_ts = end_dt.timestamp()
    start_ts = end_ts - 15 * 60  # 15m window

    state.active_slug[asset] = slug
    state.yes_id[asset] = yes_id
    state.no_id[asset] = no_id
    state.start_ts[asset] = start_ts
    state.end_ts[asset] = end_ts

    if asset not in state.eval_n:
        state.eval_n[asset] = 0
    if asset not in state.did_initial:
        state.did_initial[asset] = False
    if asset not in state.pos_yes:
        state.pos_yes[asset] = Decimal("0")
    if asset not in state.pos_no:
        state.pos_no[asset] = Decimal("0")


def initial_buy_both(clob: ClobClient, asset: str):
    yes_id = state.yes_id[asset]
    no_id = state.no_id[asset]

    # buy YES
    _, yes_ask = get_best_bid_ask(clob, yes_id)
    if yes_ask is None:
        log.info(f"[SKIP] {asset} no YES ask")
        return
    yes_price = yes_ask * (Decimal("1") + MAX_SLIPPAGE)
    yes_size = (BASE_USDC / yes_price)
    ok1 = place_limit(clob, yes_id, BUY, yes_price, yes_size)
    if ok1:
        state.pos_yes[asset] += yes_size

    # buy NO
    _, no_ask = get_best_bid_ask(clob, no_id)
    if no_ask is None:
        log.info(f"[SKIP] {asset} no NO ask")
        return
    no_price = no_ask * (Decimal("1") + MAX_SLIPPAGE)
    no_size = (BASE_USDC / no_price)
    ok2 = place_limit(clob, no_id, BUY, no_price, no_size)
    if ok2:
        state.pos_no[asset] += no_size

    state.did_initial[asset] = True
    state.eval_n[asset] = 0
    state.next_eval_ts[asset] = time.time() + EVAL_INTERVAL_SEC

    # snapshot prices
    state.last_yes_price[asset] = yes_ask
    state.last_no_price[asset] = no_ask

    log.info(f"[INIT] {asset} bought both sides: YES~{BASE_USDC} NO~{BASE_USDC}")


def evaluate_and_ladder(clob: ClobClient, asset: str):
    yes_id = state.yes_id[asset]
    no_id = state.no_id[asset]

    yes_bid, yes_ask = get_best_bid_ask(clob, yes_id)
    no_bid, no_ask = get_best_bid_ask(clob, no_id)
    if yes_ask is None or no_ask is None:
        log.info(f"[SKIP] {asset} missing asks")
        return

    last_yes = state.last_yes_price.get(asset, yes_ask)
    last_no = state.last_no_price.get(asset, no_ask)

    dy = yes_ask - last_yes
    dn = no_ask - last_no

    # winner-lean: whichever ask increased more
    if dy > dn:
        winner = "YES"
    elif dn > dy:
        winner = "NO"
    else:
        winner = "TIE"

    # record snapshot for next eval
    state.last_yes_price[asset] = yes_ask
    state.last_no_price[asset] = no_ask

    log.info(
        f"[EVAL] {asset} yes={yes_ask:.4f} no={no_ask:.4f} Δyes={dy:.4f} Δno={dn:.4f} -> {winner}"
    )

    if winner == "TIE":
        return

    # buy STEP_USDC on the winner side
    if winner == "YES":
        price = yes_ask * (Decimal("1") + MAX_SLIPPAGE)
        size = STEP_USDC / price
        if place_limit(clob, yes_id, BUY, price, size):
            state.pos_yes[asset] += size
            log.info(f"[LADDER] {asset} bought YES +{STEP_USDC} USDC")
        # optional: sell NO if divergence big
        if SELL_OPPOSITE_ON_DIVERGENCE and (yes_ask - no_ask) >= DIVERGENCE_THRESHOLD:
            sell_all_side(clob, asset, side_name="NO")
    else:
        price = no_ask * (Decimal("1") + MAX_SLIPPAGE)
        size = STEP_USDC / price
        if place_limit(clob, no_id, BUY, price, size):
            state.pos_no[asset] += size
            log.info(f"[LADDER] {asset} bought NO +{STEP_USDC} USDC")
        if SELL_OPPOSITE_ON_DIVERGENCE and (no_ask - yes_ask) >= DIVERGENCE_THRESHOLD:
            sell_all_side(clob, asset, side_name="YES")


def sell_all_side(clob: ClobClient, asset: str, side_name: str):
    if side_name == "YES":
        token_id = state.yes_id[asset]
        pos = state.pos_yes.get(asset, Decimal("0"))
        if pos <= Decimal("0"):
            return
        best_bid, _ = get_best_bid_ask(clob, token_id)
        if best_bid is None:
            log.info(f"[EXIT] {asset} no YES bid to sell")
            return
        price = best_bid * (Decimal("1") - MAX_SLIPPAGE)
        if place_limit(clob, token_id, SELL, price, pos):
            log.info(f"[EXIT] {asset} sold ALL YES size={pos}")
            state.pos_yes[asset] = Decimal("0")

    elif side_name == "NO":
        token_id = state.no_id[asset]
        pos = state.pos_no.get(asset, Decimal("0"))
        if pos <= Decimal("0"):
            return
        best_bid, _ = get_best_bid_ask(clob, token_id)
        if best_bid is None:
            log.info(f"[EXIT] {asset} no NO bid to sell")
            return
        price = best_bid * (Decimal("1") - MAX_SLIPPAGE)
        if place_limit(clob, token_id, SELL, price, pos):
            log.info(f"[EXIT] {asset} sold ALL NO size={pos}")
            state.pos_no[asset] = Decimal("0")


def exit_all(clob: ClobClient, asset: str):
    sell_all_side(clob, asset, "YES")
    sell_all_side(clob, asset, "NO")


def loop(clob: ClobClient):
    last_resolve = 0.0

    while True:
        t = time.time()

        # Resolve markets periodically
        if (t - last_resolve) >= RESOLVE_EVERY_SEC:
            last_resolve = t

            for asset in ASSETS:
                m = pick_active_15m_market_for_asset(asset)

                if not m:
                    log.info(f"[MARKET] {asset} no active 15m market found")
                    state.reset_asset(asset)
                    continue

                ensure_market_bound(asset, m)

                slug = state.active_slug[asset]
                end_ts = state.end_ts[asset]
                start_ts = state.start_ts[asset]
                tte = end_ts - t

                log.info(f"[MARKET] {asset} slug={slug} tte={tte:.1f}s")

                # If we swapped to a new slug, reset the ladder state but keep positions as tracked (safer reset)
                # (For now: if slug changed, fully reset tracking for that asset to avoid mixing.)
                if asset in state.active_slug and state.active_slug[asset] != slug:
                    state.reset_asset(asset)
                    ensure_market_bound(asset, m)

                # Wait until market start moment
                if t < start_ts:
                    continue

                # If already beyond exit point, exit now
                if (t - start_ts) >= EXIT_AT_SEC:
                    exit_all(clob, asset)
                    continue

                # Initial buy once right after start
                if not state.did_initial.get(asset, False):
                    initial_buy_both(clob, asset)
                    continue

                # Ladder evaluations
                if state.eval_n.get(asset, 0) < EVAL_COUNT:
                    next_eval = state.next_eval_ts.get(asset, t + EVAL_INTERVAL_SEC)
                    if t >= next_eval:
                        evaluate_and_ladder(clob, asset)
                        state.eval_n[asset] = state.eval_n.get(asset, 0) + 1
                        state.next_eval_ts[asset] = t + EVAL_INTERVAL_SEC

        time.sleep(POLL_SEC)


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(f"LADDER: BASE_USDC={BASE_USDC:.2f} STEP_USDC={STEP_USDC:.2f} EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}")
    log.info(f"EXEC: MAX_SLIPPAGE={MAX_SLIPPAGE:.3f} MAX_RETRIES={MAX_RETRIES} MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}")

    clob = init_clob_client()
    loop(clob)
