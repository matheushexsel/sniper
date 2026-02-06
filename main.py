import os
import time
import json
import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# --------- py_clob_client: defensive imports (your installed version may vary) ----------
from py_clob_client.client import ClobClient

try:
    # some versions have these
    from py_clob_client.order_builder.constants import BUY as _BUY, SELL as _SELL
    BUY = _BUY
    SELL = _SELL
except Exception:
    # fallback: most APIs accept strings
    BUY = "BUY"
    SELL = "SELL"


# =========================
# Config
# =========================

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try:
        return int(v) if v != "" else default
    except Exception:
        return default

def parse_assets(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return ["ETH", "BTC", "SOL", "XRP"]
    # supports "['ETH','BTC']" or "ETH,BTC"
    if raw.startswith("["):
        try:
            arr = json.loads(raw.replace("'", '"'))
            return [str(x).strip().upper() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("updown15m_ladder")

CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = env_int("CHAIN_ID", 137)

PM_FOUNDER = os.getenv("PM_FOUNDER", "").strip()
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY", "").strip()
PM_SIGNATURE_TYPE = os.getenv("PM_SIGNATURE_TYPE", "EIP712").strip()

ASSETS = parse_assets(os.getenv("ASSETS", "['ETH','BTC','SOL','XRP']"))

# Discovery / polling
POLL_SEC = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)
FETCH_LIMIT = env_int("FETCH_LIMIT", 50)
MAX_SLUG_LOOKUPS = env_int("MAX_SLUG_LOOKUPS", 50)

# Strategy (your ladder)
BASE_USDC = env_float("BASE_USDC", 1.0)
STEP_USDC = env_float("STEP_USDC", 1.0)
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120)  # every 2 minutes
EVAL_COUNT = env_int("EVAL_COUNT", 6)                  # 6 times
EXIT_AT_SEC = env_int("EXIT_AT_SEC", 780)              # 13 minutes into the 15m window

# Execution / safety
MAX_SLIPPAGE = env_float("MAX_SLIPPAGE", 0.02)
MAX_RETRIES = env_int("MAX_RETRIES", 3)
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

# If you want to disable live trading while testing:
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")

# =========================
# Helpers
# =========================

def now_ts() -> float:
    return time.time()

def parse_iso_to_ts(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        # e.g. 2026-02-06T20:00:00Z
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None

def http_get_json(url: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"[HTTP] GET failed: {url} ({e})")
        return None

def gamma_list_events(limit: int) -> List[Dict[str, Any]]:
    # IMPORTANT: don't add params that may cause 422.
    # Keep it minimal and filter client-side.
    url = f"{GAMMA_HOST}/events?limit={limit}&offset=0"
    data = http_get_json(url)
    if not data:
        return []
    # gamma often returns { "events": [...] } or direct list
    if isinstance(data, dict) and "events" in data and isinstance(data["events"], list):
        return data["events"]
    if isinstance(data, list):
        return data
    # sometimes "data"
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    return []

def gamma_expand_event(slug: str) -> Optional[Dict[str, Any]]:
    # This is what gave you: closed, enableOrderBook, endDate, clobTokenIds, conditionId
    url = f"{GAMMA_HOST}/events/{slug}"
    return http_get_json(url)

def pick_active_updown_15m_slug(asset: str) -> Optional[Dict[str, Any]]:
    """
    Returns:
      {
        "slug": str,
        "end_ts": float,
        "start_ts": float,
        "yes_token": str,
        "no_token": str
      }
    """
    asset_l = asset.lower()

    events = gamma_list_events(FETCH_LIMIT)
    if not events:
        return None

    # gather candidate slugs
    slugs: List[str] = []
    for e in events:
        slug = (e.get("slug") or "").strip()
        if not slug:
            continue
        if f"{asset_l}-updown-15m-" in slug:
            slugs.append(slug)

    slugs = slugs[:MAX_SLUG_LOOKUPS]
    if not slugs:
        return None

    best: Optional[Dict[str, Any]] = None
    best_tte = 1e18
    now = now_ts()

    for slug in slugs:
        expanded = gamma_expand_event(slug)
        if not expanded:
            continue

        closed = bool(expanded.get("closed", False))
        enable_ob = bool(expanded.get("enableOrderBook", False))
        end_date = expanded.get("endDate") or expanded.get("end_date") or ""

        end_ts = parse_iso_to_ts(end_date) if isinstance(end_date, str) else None
        token_ids = expanded.get("clobTokenIds") or expanded.get("clobTokenIDs") or expanded.get("clob_token_ids")

        if closed or not enable_ob or not end_ts or not token_ids or not isinstance(token_ids, list) or len(token_ids) < 2:
            continue

        # active window logic: end_ts in future, and within a reasonable horizon
        tte = end_ts - now
        if tte <= 0:
            continue
        # 15m window ends soon; we accept anything <= 30m out to be safe
        if tte > 1800:
            continue

        if tte < best_tte:
            best_tte = tte
            start_ts = end_ts - 900.0
            best = {
                "slug": slug,
                "end_ts": end_ts,
                "start_ts": start_ts,
                "yes_token": str(token_ids[0]),
                "no_token": str(token_ids[1]),
            }

    return best


def init_clob_client() -> ClobClient:
    if not PM_PRIVATE_KEY:
        raise RuntimeError("PM_PRIVATE_KEY is missing")
    if not PM_FOUNDER:
        raise RuntimeError("PM_FOUNDER is missing")

    # IMPORTANT: your installed py_clob_client version DOES NOT accept private_key=...
    # Most versions accept `key=...` and `signature_type=...`.
    client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PM_PRIVATE_KEY,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FOUNDER,
    )

    # Derive or load API creds if supported by your version.
    # Some versions do this lazily; some require it.
    if hasattr(client, "create_or_derive_api_creds"):
        try:
            client.create_or_derive_api_creds()
            log.info("[AUTH] derived API creds via create_or_derive_api_creds()")
        except Exception as e:
            log.warning(f"[AUTH] derive creds failed (continuing): {e}")

    return client


def best_bid_ask_from_orderbook(ob: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    ob structure varies by version. We support:
      - dict with 'bids'/'asks' list of {price:..} or [price, size]
      - object with .bids/.asks
    """
    def extract_side(side) -> List[Any]:
        if side is None:
            return []
        if isinstance(side, list):
            return side
        return []

    bids = None
    asks = None
    if isinstance(ob, dict):
        bids = ob.get("bids")
        asks = ob.get("asks")
    else:
        bids = getattr(ob, "bids", None)
        asks = getattr(ob, "asks", None)

    bids_l = extract_side(bids)
    asks_l = extract_side(asks)

    def get_price(level) -> Optional[float]:
        try:
            if isinstance(level, dict):
                return float(level.get("price"))
            if isinstance(level, (list, tuple)) and len(level) >= 1:
                return float(level[0])
        except Exception:
            return None
        return None

    best_bid = get_price(bids_l[0]) if bids_l else None
    best_ask = get_price(asks_l[0]) if asks_l else None
    return best_bid, best_ask


def clamp_price(p: float) -> float:
    # Polymarket prices are 0..1, tick sizes may vary but this keeps sanity.
    return max(0.001, min(0.999, p))


def rate_limiter_state() -> Dict[str, Any]:
    return {"minute": int(now_ts() // 60), "count": 0}

def allow_trade(rl: Dict[str, Any]) -> bool:
    m = int(now_ts() // 60)
    if rl["minute"] != m:
        rl["minute"] = m
        rl["count"] = 0
    if rl["count"] >= MAX_TRADES_PER_MIN:
        return False
    rl["count"] += 1
    return True


def place_limit_order(client: ClobClient, token_id: str, side: str, price: float, size: float) -> bool:
    if DRY_RUN:
        log.info(f"[DRY_RUN] {side} token={token_id} price={price:.4f} size={size:.6f}")
        return True

    side_v = side if isinstance(side, str) else str(side)
    price = clamp_price(price)
    size = max(0.000001, float(size))

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Most versions:
            #   order = client.create_order(token_id=..., side=..., price=..., size=..., time_in_force="GTC")
            #   client.post_order(order)
            if hasattr(client, "create_order") and hasattr(client, "post_order"):
                order = client.create_order(
                    token_id=token_id,
                    side=side_v,
                    price=price,
                    size=size,
                    time_in_force="GTC",
                )
                client.post_order(order)
                return True

            # Alternate: create_limit_order + post_order
            if hasattr(client, "create_limit_order") and hasattr(client, "post_order"):
                order = client.create_limit_order(
                    token_id=token_id,
                    side=side_v,
                    price=price,
                    size=size,
                )
                client.post_order(order)
                return True

            raise RuntimeError("No compatible order creation method found on ClobClient")

        except Exception as e:
            last_err = e
            log.warning(f"[ORDER] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(0.15 * attempt)

    log.error(f"[ORDER] failed permanently: {last_err}")
    return False


# =========================
# Ladder state per asset
# =========================

@dataclass
class AssetSession:
    slug: str = ""
    yes_token: str = ""
    no_token: str = ""
    start_ts: float = 0.0
    end_ts: float = 0.0
    started: bool = False
    initial_bought: bool = False
    eval_index: int = 0
    next_eval_ts: float = 0.0
    exit_ts: float = 0.0
    last_yes_price: Optional[float] = None
    last_no_price: Optional[float] = None
    # naive position tracking (shares). We assume fills at our limit price for sizing.
    yes_shares: float = 0.0
    no_shares: float = 0.0


def refresh_or_start_session(asset: str, sess: AssetSession) -> Optional[AssetSession]:
    info = pick_active_updown_15m_slug(asset)
    if not info:
        log.info(f"[MARKET] {asset} no active 15m market found")
        return None

    # If new slug, reset state
    if info["slug"] != sess.slug:
        sess = AssetSession(
            slug=info["slug"],
            yes_token=info["yes_token"],
            no_token=info["no_token"],
            start_ts=info["start_ts"],
            end_ts=info["end_ts"],
            started=False,
            initial_bought=False,
            eval_index=0,
            next_eval_ts=info["start_ts"] + EVAL_INTERVAL_SEC,
            exit_ts=info["start_ts"] + EXIT_AT_SEC,
            last_yes_price=None,
            last_no_price=None,
            yes_shares=0.0,
            no_shares=0.0,
        )
        log.info(
            f"[MARKET] {asset} slug={sess.slug} start={datetime.fromtimestamp(sess.start_ts, tz=timezone.utc)} "
            f"end={datetime.fromtimestamp(sess.end_ts, tz=timezone.utc)}"
        )
    return sess


def get_side_prices(client: ClobClient, yes_token: str, no_token: str) -> Optional[Tuple[float, float, float, float]]:
    """
    Returns (yes_bid, yes_ask, no_bid, no_ask)
    """
    try:
        ob_yes = client.get_order_book(yes_token)
        ob_no = client.get_order_book(no_token)
        yb, ya = best_bid_ask_from_orderbook(ob_yes)
        nb, na = best_bid_ask_from_orderbook(ob_no)
        if ya is None or na is None or yb is None or nb is None:
            return None
        return float(yb), float(ya), float(nb), float(na)
    except Exception as e:
        log.warning(f"[ORDERBOOK] fetch failed: {e}")
        return None


def run_asset_loop(client: ClobClient, asset: str, sess: AssetSession, rl: Dict[str, Any]) -> AssetSession:
    now = now_ts()

    # Wait until start window exists; if we discover late, we can still enter if before exit
    if now < sess.start_ts:
        return sess
    if now > sess.end_ts:
        # window over; will refresh next resolve
        return sess

    # If already past exit time, liquidate and mark done
    if now >= sess.exit_ts and sess.initial_bought:
        # sell everything
        prices = get_side_prices(client, sess.yes_token, sess.no_token)
        if prices and allow_trade(rl):
            yb, ya, nb, na = prices
            # sell at best bid (or slightly below to ensure fill)
            sell_yes_price = clamp_price(yb * (1.0 - MAX_SLIPPAGE))
            sell_no_price = clamp_price(nb * (1.0 - MAX_SLIPPAGE))
            if sess.yes_shares > 0:
                place_limit_order(client, sess.yes_token, SELL, sell_yes_price, sess.yes_shares)
            if sess.no_shares > 0:
                place_limit_order(client, sess.no_token, SELL, sell_no_price, sess.no_shares)
            log.info(f"[EXIT] {asset} sold all positions for slug={sess.slug}")
        return sess

    # Initial buys (both sides) as soon as we're inside the window
    if not sess.initial_bought and now < sess.exit_ts:
        prices = get_side_prices(client, sess.yes_token, sess.no_token)
        if prices and allow_trade(rl):
            yb, ya, nb, na = prices
            buy_yes_price = clamp_price(ya * (1.0 + MAX_SLIPPAGE))
            buy_no_price  = clamp_price(na * (1.0 + MAX_SLIPPAGE))
            yes_size = BASE_USDC / buy_yes_price
            no_size  = BASE_USDC / buy_no_price
            ok1 = place_limit_order(client, sess.yes_token, BUY, buy_yes_price, yes_size)
            ok2 = place_limit_order(client, sess.no_token,  BUY, buy_no_price,  no_size)
            if ok1:
                sess.yes_shares += yes_size
            if ok2:
                sess.no_shares += no_size
            sess.initial_bought = ok1 and ok2
            sess.last_yes_price = ya
            sess.last_no_price = na
            log.info(
                f"[INIT] {asset} bought both sides BASE_USDC={BASE_USDC:.2f} "
                f"(yes_ask={ya:.4f}, no_ask={na:.4f}) slug={sess.slug}"
            )

        time.sleep(COOLDOWN_SEC)
        return sess

    # Ladder evaluations
    if sess.initial_bought and sess.eval_index < EVAL_COUNT and now >= sess.next_eval_ts and now < sess.exit_ts:
        prices = get_side_prices(client, sess.yes_token, sess.no_token)
        if not prices:
            return sess

        yb, ya, nb, na = prices
        # Use ask as "current cost" proxy
        cur_yes = ya
        cur_no = na

        if sess.last_yes_price is None or sess.last_no_price is None:
            sess.last_yes_price = cur_yes
            sess.last_no_price = cur_no

        dy = cur_yes - sess.last_yes_price
        dn = cur_no - sess.last_no_price

        leader = "YES" if dy > dn else "NO"
        leader_price = cur_yes if leader == "YES" else cur_no
        leader_token = sess.yes_token if leader == "YES" else sess.no_token

        log.info(
            f"[EVAL] {asset} i={sess.eval_index+1}/{EVAL_COUNT} "
            f"yes_ask={cur_yes:.4f} (d={dy:+.4f}) no_ask={cur_no:.4f} (d={dn:+.4f}) leader={leader}"
        )

        if allow_trade(rl):
            buy_price = clamp_price(leader_price * (1.0 + MAX_SLIPPAGE))
            buy_size = STEP_USDC / buy_price
            ok = place_limit_order(client, leader_token, BUY, buy_price, buy_size)
            if ok:
                if leader == "YES":
                    sess.yes_shares += buy_size
                else:
                    sess.no_shares += buy_size
                log.info(f"[LADDER] {asset} bought +{STEP_USDC:.2f} on {leader} @ {buy_price:.4f}")

        sess.eval_index += 1
        sess.next_eval_ts = sess.start_ts + (sess.eval_index + 1) * EVAL_INTERVAL_SEC
        sess.last_yes_price = cur_yes
        sess.last_no_price = cur_no

        time.sleep(COOLDOWN_SEC)

    return sess


# =========================
# Main loop
# =========================

def main():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(
        f"LADDER: BASE_USDC={BASE_USDC:.2f} STEP_USDC={STEP_USDC:.2f} "
        f"EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}"
    )
    log.info(
        f"EXEC: MAX_SLIPPAGE={MAX_SLIPPAGE:.3f} MAX_RETRIES={MAX_RETRIES} "
        f"MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}"
    )
    log.info(f"DRY_RUN={DRY_RUN}")

    client = init_clob_client()

    sessions: Dict[str, AssetSession] = {a: AssetSession() for a in ASSETS}
    rl = rate_limiter_state()
    last_resolve = 0.0

    while True:
        t = now_ts()

        # refresh discovery on a cadence
        if t - last_resolve >= RESOLVE_EVERY_SEC:
            last_resolve = t
            for a in ASSETS:
                updated = refresh_or_start_session(a, sessions[a])
                if updated is not None:
                    sessions[a] = updated

        # run ladder logic fast polling
        for a in ASSETS:
            if sessions[a].slug:
                sessions[a] = run_asset_loop(client, a, sessions[a], rl)

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
