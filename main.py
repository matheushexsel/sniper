# main.py
import os
import time
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, List

import requests

# Polymarket CLOB client (pip: py-clob-client)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, ApiCreds

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("updown15m-ladder")

# -----------------------------
# Env helpers
# -----------------------------
def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return default if not v else float(v)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return default if not v else int(float(v))

def env_list(name: str, default: List[str]) -> List[str]:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    if v.startswith("["):
        return [str(x).strip() for x in json.loads(v)]
    return [x.strip() for x in v.split(",") if x.strip()]

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_z(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

# -----------------------------
# Strategy params (Railway env vars)
# -----------------------------
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")
CHAIN_ID = env_int("CHAIN_ID", 137)

ASSETS = [a.upper() for a in env_list("ASSETS", ["ETH", "BTC", "SOL", "XRP"])]

# Trading cadence
POLL_SEC = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)

# Window / ladder timing (seconds)
WINDOW_SEC = env_int("WINDOW_SEC", 900)               # 15m
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120) # every 2m
EVAL_COUNT = env_int("EVAL_COUNT", 6)                 # 6 adds => 12 minutes
EXIT_AT_SEC = env_int("EXIT_AT_SEC", 780)             # 13 minutes
CLOSE_HARD_SEC = env_int("CLOSE_HARD_SEC", 60)        # if <=60s to end, exit now

# Notional sizing
BASE_USDC = env_float("BASE_USDC", 1.0)               # buy both sides at start
STEP_USDC = env_float("STEP_USDC", 1.0)               # add to winner each eval

# Execution constraints
MAX_SLIPPAGE = env_float("MAX_SLIPPAGE", 0.02)         # 2%
FILL_TIMEOUT_SEC = env_float("FILL_TIMEOUT_SEC", 2.0)
MAX_RETRIES = env_int("MAX_RETRIES", 3)

# Rate limiting
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

# Market selection
MAX_SLUG_LOOKUPS = env_int("MAX_SLUG_LOOKUPS", 20)
MAX_TTE_SELECT_SEC = env_int("MAX_TTE_SELECT_SEC", 1200)  # only consider markets ending within 20m
MIN_TTE_SELECT_SEC = env_int("MIN_TTE_SELECT_SEC", 15)    # ignore too-close endings

# Polymarket creds (accept common names + your existing typo variant)
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY") or os.getenv("PRIVATE_KEY")
PM_API_KEY = os.getenv("PM_API_KEY")
PM_API_SECRET = os.getenv("PM_API_SECRET")
PM_API_PASSPHRASE = os.getenv("PM_API_PASSPHRASE")
PM_FUNDER = os.getenv("PM_FUNDER") or os.getenv("PM_FOUNDER")

if not PM_PRIVATE_KEY:
    raise SystemExit("Missing PM_PRIVATE_KEY (or PRIVATE_KEY) in Railway env vars")
if not PM_FUNDER:
    raise SystemExit("Missing PM_FUNDER (wallet address) in Railway env vars")

# -----------------------------
# Asset -> slug prefix mapping (15m products)
# -----------------------------
SLUG_PREFIX = {
    "ETH": "eth-updown-15m-",
    "BTC": "btc-updown-15m-",
    "SOL": "sol-updown-15m-",
    "XRP": "xrp-updown-15m-",
}

# -----------------------------
# Simple trade limiter
# -----------------------------
class TradeLimiter:
    def __init__(self):
        self.window_start = time.time()
        self.count = 0

    def allow(self) -> bool:
        now = time.time()
        if now - self.window_start >= 60:
            self.window_start = now
            self.count = 0
        if self.count >= MAX_TRADES_PER_MIN:
            return False
        self.count += 1
        return True

trade_limiter = TradeLimiter()
last_trade_ts = 0.0

def cooldown_ok() -> bool:
    global last_trade_ts
    now = time.time()
    if now - last_trade_ts < COOLDOWN_SEC:
        return False
    last_trade_ts = now
    return True

# -----------------------------
# Gamma API wrappers
# -----------------------------
session = requests.Session()
session.headers.update({"User-Agent": "updown15m-ladder-bot/1.1"})

def gamma_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    url = f"{GAMMA_HOST}{path}"
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code != 200:
            log.warning(f"[GAMMA] {r.status_code} {url} {r.text[:200]}")
            return None
        return r.json()
    except Exception as e:
        log.warning(f"[GAMMA] error {url}: {e}")
        return None

def gamma_search_events(asset: str, limit: int = 50) -> List[dict]:
    # Try common query variants
    data = gamma_get("/events", params={"limit": limit, "offset": 0, "search": asset})
    if isinstance(data, dict) and isinstance(data.get("events"), list):
        return data["events"]
    if isinstance(data, list):
        return data

    data = gamma_get("/events", params={"limit": limit, "offset": 0, "q": asset})
    if isinstance(data, dict) and isinstance(data.get("events"), list):
        return data["events"]
    if isinstance(data, list):
        return data

    return []

def gamma_get_event_by_slug(slug: str) -> Optional[dict]:
    # Pattern 1: /events/{slug}
    data = gamma_get(f"/events/{slug}")
    if isinstance(data, dict) and data.get("slug"):
        return data

    # Pattern 2: /events?slug=...
    data = gamma_get("/events", params={"slug": slug, "limit": 1, "offset": 0})
    if isinstance(data, dict) and isinstance(data.get("events"), list) and data["events"]:
        return data["events"][0]
    if isinstance(data, list) and data:
        return data[0]

    return None

# -----------------------------
# CLOB init (fixed)
# -----------------------------
def init_clob_client() -> ClobClient:
    """
    Railway has a py-clob-client version where ClobClient(...) does NOT accept private_key=...
    So:
      1) instantiate with host + chain_id
      2) attach wallet via set_wallet(...)
      3) attach API creds via set_api_creds(...) if provided
    """
    client = ClobClient(
        CLOB_HOST,
        chain_id=CHAIN_ID,
    )

    # attach wallet signer
    client.set_wallet(
        private_key=PM_PRIVATE_KEY,
        funder=PM_FUNDER,
    )

    # attach api creds only if present
    if PM_API_KEY and PM_API_SECRET and PM_API_PASSPHRASE:
        client.set_api_creds(ApiCreds(PM_API_KEY, PM_API_SECRET, PM_API_PASSPHRASE))

    return client

clob = init_clob_client()

# -----------------------------
# CLOB helpers
# -----------------------------
def best_bid_ask(token_id: str) -> Optional[Tuple[float, float]]:
    try:
        ob = clob.get_order_book(token_id)
        bids = ob.get("bids", []) or []
        asks = ob.get("asks", []) or []
        if not bids or not asks:
            return None
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        return best_bid, best_ask
    except Exception as e:
        log.warning(f"[OBOOK] token={token_id} error: {e}")
        return None

def estimate_vwap_buy(token_id: str, qty: float) -> Optional[float]:
    try:
        ob = clob.get_order_book(token_id)
        asks = ob.get("asks", []) or []
        if not asks:
            return None
        remaining = qty
        cost = 0.0
        filled = 0.0
        for lvl in asks:
            p = float(lvl["price"])
            s = float(lvl["size"])
            take = min(remaining, s)
            cost += take * p
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        if filled <= 0 or remaining > 1e-9:
            return None
        return cost / filled
    except Exception:
        return None

def estimate_vwap_sell(token_id: str, qty: float) -> Optional[float]:
    try:
        ob = clob.get_order_book(token_id)
        bids = ob.get("bids", []) or []
        if not bids:
            return None
        remaining = qty
        proceeds = 0.0
        filled = 0.0
        for lvl in bids:
            p = float(lvl["price"])
            s = float(lvl["size"])
            take = min(remaining, s)
            proceeds += take * p
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        if filled <= 0 or remaining > 1e-9:
            return None
        return proceeds / filled
    except Exception:
        return None

def place_limit_order(token_id: str, side: str, qty: float, price: float) -> Optional[str]:
    try:
        if not trade_limiter.allow() or not cooldown_ok():
            return None

        args = OrderArgs(
            token_id=token_id,
            price=round(float(price), 4),
            size=round(float(qty), 8),
            side=side,
        )
        signed = clob.create_order(args)
        resp = clob.post_order(signed)

        oid = resp.get("orderID") or resp.get("orderId") or resp.get("id")
        return oid
    except Exception as e:
        log.warning(f"[ORDER] {side} token={token_id} qty={qty} price={price} error: {e}")
        return None

def get_order_status(order_id: str) -> Optional[dict]:
    try:
        return clob.get_order(order_id)
    except Exception:
        return None

def cancel_order(order_id: str) -> bool:
    try:
        clob.cancel_order(order_id)
        return True
    except Exception:
        return False

def wait_fill_or_cancel(order_id: str, timeout_sec: float) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        st = get_order_status(order_id)
        if not st:
            time.sleep(0.2)
            continue
        filled = float(st.get("sizeMatched") or st.get("filledSize") or 0.0)
        if filled > 0:
            return True
        if st.get("status") in ("CANCELED", "CANCELLED", "FILLED"):
            return st.get("status") == "FILLED"
        time.sleep(0.2)
    cancel_order(order_id)
    return False

# -----------------------------
# Strategy state
# -----------------------------
@dataclass
class MarketState:
    asset: str
    slug: str
    t_start: datetime
    t_end: datetime
    up_token: str
    down_token: str

    base_done: bool = False
    eval_index: int = 0  # 0..EVAL_COUNT
    exit_done: bool = False

    pos_up_qty: float = 0.0
    pos_down_qty: float = 0.0

    last_ask_up: Optional[float] = None
    last_ask_down: Optional[float] = None

    def elapsed_sec(self) -> float:
        return (utc_now() - self.t_start).total_seconds()

    def tte_sec(self) -> float:
        return (self.t_end - utc_now()).total_seconds()

states: Dict[str, MarketState] = {}
last_resolve_ts_by_asset: Dict[str, float] = {}

# -----------------------------
# Market resolution
# -----------------------------
def resolve_current_15m_market(asset: str) -> Optional[Tuple[str, datetime, datetime, str, str]]:
    """
    Choose the 15m market that ends soonest, within [MIN_TTE_SELECT_SEC, MAX_TTE_SELECT_SEC].
    Must be:
      - slug starts with correct prefix
      - enableOrderBook True
      - not closed
      - endDate present
      - has 2 clobTokenIds
    """
    prefix = SLUG_PREFIX.get(asset)
    if not prefix:
        return None

    events = gamma_search_events(asset, limit=50)
    slugs = []
    for ev in events:
        s = (ev.get("slug") or "").strip()
        if s.startswith(prefix):
            slugs.append(s)

    slugs = slugs[:MAX_SLUG_LOOKUPS]
    if not slugs:
        return None

    best_ev = None
    best_tte = None

    for slug in slugs:
        ev = gamma_get_event_by_slug(slug)
        if not ev:
            continue

        if bool(ev.get("closed", False)):
            continue
        if not bool(ev.get("enableOrderBook", False)):
            continue

        end_dt = parse_iso_z(ev.get("endDate") or "")
        if not end_dt:
            continue

        tte = (end_dt - utc_now()).total_seconds()
        if tte < MIN_TTE_SELECT_SEC:
            continue
        if tte > MAX_TTE_SELECT_SEC:
            continue

        clob_ids = ev.get("clobTokenIds") or []
        if not isinstance(clob_ids, list) or len(clob_ids) < 2:
            continue

        if best_tte is None or tte < best_tte:
            best_tte = tte
            best_ev = ev

    if not best_ev:
        return None

    slug = best_ev["slug"]
    t_end = parse_iso_z(best_ev.get("endDate"))
    t_start = t_end - timedelta(seconds=WINDOW_SEC)

    # token0 = UP, token1 = DOWN for these products
    up_token = str(best_ev["clobTokenIds"][0])
    down_token = str(best_ev["clobTokenIds"][1])

    return slug, t_start, t_end, up_token, down_token

# -----------------------------
# Winner rule (your spec)
# -----------------------------
def choose_winner(ask_up_now: float, ask_dn_now: float, last_up: float, last_dn: float) -> str:
    """
    Picks the side that increased more since last eval.
    If tied, pick the side with higher current ask.
    """
    d_up = ask_up_now - last_up
    d_dn = ask_dn_now - last_dn
    if d_up > d_dn:
        return "UP"
    if d_dn > d_up:
        return "DOWN"
    if ask_up_now > ask_dn_now:
        return "UP"
    if ask_dn_now > ask_up_now:
        return "DOWN"
    return "UP"

# -----------------------------
# Trading primitives (notional buys, qty sells)
# -----------------------------
def buy_notional(token_id: str, usdc_notional: float) -> float:
    ba = best_bid_ask(token_id)
    if not ba:
        return 0.0
    _, ask = ba
    if ask <= 0:
        return 0.0
    qty = usdc_notional / ask

    vwap = estimate_vwap_buy(token_id, qty)
    if vwap is None:
        log.info(f"[LIQ] buy token={token_id} qty={qty:.6f} no depth")
        return 0.0
    if vwap > ask * (1.0 + MAX_SLIPPAGE):
        log.info(f"[LIQ] buy token={token_id} vwap={vwap:.4f} > max={ask*(1+MAX_SLIPPAGE):.4f}")
        return 0.0

    for _ in range(MAX_RETRIES):
        oid = place_limit_order(token_id, "BUY", qty, ask)
        if not oid:
            time.sleep(0.2)
            continue
        ok = wait_fill_or_cancel(oid, FILL_TIMEOUT_SEC)
        st = get_order_status(oid) or {}
        filled = float(st.get("sizeMatched") or st.get("filledSize") or 0.0)
        if ok and filled > 0:
            return filled
        time.sleep(0.2)

    return 0.0

def sell_qty(token_id: str, qty: float) -> float:
    if qty <= 0:
        return 0.0

    ba = best_bid_ask(token_id)
    if not ba:
        return 0.0
    bid, _ = ba
    if bid <= 0:
        return 0.0

    vwap = estimate_vwap_sell(token_id, qty)
    if vwap is None:
        log.info(f"[LIQ] sell token={token_id} qty={qty:.6f} no depth")
        return 0.0
    if vwap < bid * (1.0 - MAX_SLIPPAGE):
        log.info(f"[LIQ] sell token={token_id} vwap={vwap:.4f} < min={bid*(1-MAX_SLIPPAGE):.4f}")
        return 0.0

    for _ in range(MAX_RETRIES):
        oid = place_limit_order(token_id, "SELL", qty, bid)
        if not oid:
            time.sleep(0.2)
            continue
        ok = wait_fill_or_cancel(oid, FILL_TIMEOUT_SEC)
        st = get_order_status(oid) or {}
        filled = float(st.get("sizeMatched") or st.get("filledSize") or 0.0)
        if ok and filled > 0:
            return filled
        time.sleep(0.2)

    return 0.0

# -----------------------------
# Strategy execution
# -----------------------------
def ensure_market(asset: str):
    now = time.time()
    last = last_resolve_ts_by_asset.get(asset, 0.0)
    if now - last < RESOLVE_EVERY_SEC:
        return
    last_resolve_ts_by_asset[asset] = now

    resolved = resolve_current_15m_market(asset)
    if not resolved:
        log.info(f"[MARKET] {asset}: no eligible 15m market in TTE bounds")
        return

    slug, t_start, t_end, up_token, down_token = resolved

    st = states.get(asset)
    if st and st.slug == slug:
        return

    states[asset] = MarketState(
        asset=asset,
        slug=slug,
        t_start=t_start,
        t_end=t_end,
        up_token=up_token,
        down_token=down_token,
    )
    tte = (t_end - utc_now()).total_seconds()
    log.info(f"[MARKET] {asset}: selected slug={slug} tte={tte:.1f}s start={t_start.isoformat()} end={t_end.isoformat()}")

def do_exit(st: MarketState):
    sold_up = sell_qty(st.up_token, st.pos_up_qty)
    st.pos_up_qty = max(0.0, st.pos_up_qty - sold_up)

    sold_dn = sell_qty(st.down_token, st.pos_down_qty)
    st.pos_down_qty = max(0.0, st.pos_down_qty - sold_dn)

    st.exit_done = True
    log.info(
        f"[EXIT-DONE] {st.asset} slug={st.slug} sold_up={sold_up:.6f} rem_up={st.pos_up_qty:.6f} "
        f"sold_dn={sold_dn:.6f} rem_dn={st.pos_down_qty:.6f}"
    )

def tick_asset(asset: str):
    st = states.get(asset)
    if not st:
        return

    T = st.elapsed_sec()
    tte = st.tte_sec()

    # Hard exit if too close to end
    if not st.exit_done and tte <= CLOSE_HARD_SEC:
        log.info(f"[EXIT-HARD] {asset} slug={st.slug} tte={tte:.1f}s -> exiting")
        do_exit(st)
        return

    # Base entry: immediately when window starts (we consider start as t_end - 15m)
    # We allow within first 60 seconds of the window.
    if not st.base_done and 0 <= T < 60:
        up_ba = best_bid_ask(st.up_token)
        dn_ba = best_bid_ask(st.down_token)
        if not up_ba or not dn_ba:
            return

        _, ask_up = up_ba
        _, ask_dn = dn_ba

        qty_up = buy_notional(st.up_token, BASE_USDC)
        qty_dn = buy_notional(st.down_token, BASE_USDC)

        st.pos_up_qty += qty_up
        st.pos_down_qty += qty_dn

        st.last_ask_up = ask_up
        st.last_ask_down = ask_dn
        st.base_done = True

        log.info(
            f"[BASE] {asset} slug={st.slug} "
            f"buy_up=${BASE_USDC} qty={qty_up:.6f} ask={ask_up:.4f} "
            f"buy_dn=${BASE_USDC} qty={qty_dn:.6f} ask={ask_dn:.4f}"
        )
        return

    if not st.base_done:
        return

    # Evaluations: every 2 minutes, for 6 times
    if not st.exit_done and st.eval_index < EVAL_COUNT and T < EXIT_AT_SEC:
        target = (st.eval_index + 1) * EVAL_INTERVAL_SEC
        if T >= target:
            up_ba = best_bid_ask(st.up_token)
            dn_ba = best_bid_ask(st.down_token)
            if not up_ba or not dn_ba:
                return

            _, ask_up = up_ba
            _, ask_dn = dn_ba

            if st.last_ask_up is None or st.last_ask_down is None:
                st.last_ask_up, st.last_ask_down = ask_up, ask_dn

            winner = choose_winner(ask_up, ask_dn, st.last_ask_up, st.last_ask_down)
            win_token = st.up_token if winner == "UP" else st.down_token

            qty = buy_notional(win_token, STEP_USDC)
            if winner == "UP":
                st.pos_up_qty += qty
            else:
                st.pos_down_qty += qty

            d_up = ask_up - st.last_ask_up
            d_dn = ask_dn - st.last_ask_down
            st.last_ask_up, st.last_ask_down = ask_up, ask_dn

            st.eval_index += 1

            log.info(
                f"[EVAL] {asset} k={st.eval_index}/{EVAL_COUNT} T={T:.1f}s tte={tte:.1f}s "
                f"ask_up={ask_up:.4f} ask_dn={ask_dn:.4f} d_up={d_up:+.4f} d_dn={d_dn:+.4f} "
                f"winner={winner} buy=${STEP_USDC} qty={qty:.6f}"
            )
            return

    # Exit at 13 minutes
    if not st.exit_done and T >= EXIT_AT_SEC:
        log.info(f"[EXIT] {asset} slug={st.slug} T={T:.1f}s tte={tte:.1f}s -> exiting all")
        do_exit(st)
        return

    # Periodic visibility
    if int(time.time()) % 10 == 0:
        up_ba = best_bid_ask(st.up_token)
        dn_ba = best_bid_ask(st.down_token)
        if up_ba and dn_ba:
            up_bid, up_ask = up_ba
            dn_bid, dn_ask = dn_ba
            log.info(
                f"[PRICES] {asset} T={T:.0f}s tte={tte:.0f}s "
                f"UP {up_bid:.4f}/{up_ask:.4f} DN {dn_bid:.4f}/{dn_ask:.4f} "
                f"pos_up={st.pos_up_qty:.6f} pos_dn={st.pos_down_qty:.6f}"
            )

# -----------------------------
# Boot / main
# -----------------------------
def print_boot():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"WINDOW_SEC={WINDOW_SEC} EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}")
    log.info(f"BASE_USDC={BASE_USDC} STEP_USDC={STEP_USDC} MAX_SLIPPAGE={MAX_SLIPPAGE}")
    log.info(f"SELECT_TTE: MIN={MIN_TTE_SELECT_SEC}s MAX={MAX_TTE_SELECT_SEC}s MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC}")

def main():
    print_boot()
    while True:
        for asset in ASSETS:
            ensure_market(asset)
            tick_asset(asset)
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
