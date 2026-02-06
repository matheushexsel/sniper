import os
import time
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from py_clob_client.client import ClobClient

# ---- defensive BUY/SELL constants across py_clob_client versions ----
try:
    from py_clob_client.order_builder.constants import BUY as _BUY, SELL as _SELL
    BUY, SELL = _BUY, _SELL
except Exception:
    BUY, SELL = "BUY", "SELL"


# =========================
# Config helpers
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
    if raw.startswith("["):
        try:
            arr = json.loads(raw.replace("'", '"'))
            return [str(x).strip().upper() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


# =========================
# Environment / Settings
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("updown15m_ladder")

CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip()
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
CHAIN_ID = env_int("CHAIN_ID", 137)

PM_FOUNDER = os.getenv("PM_FOUNDER", "").strip()
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY", "").strip()
PM_SIGNATURE_TYPE = os.getenv("PM_SIGNATURE_TYPE", "EIP712").strip()

ASSETS = parse_assets(os.getenv("ASSETS", '["ETH","BTC","SOL","XRP"]'))

# polling
POLL_SEC = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 10.0)  # keep fast; deterministic

# ladder strategy
BASE_USDC = env_float("BASE_USDC", 1.0)
STEP_USDC = env_float("STEP_USDC", 1.0)
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120)  # every 2 min
EVAL_COUNT = env_int("EVAL_COUNT", 6)                  # 6 times
EXIT_AT_SEC = env_int("EXIT_AT_SEC", 780)              # 13 min after start

# execution / safety
MAX_SLIPPAGE = env_float("MAX_SLIPPAGE", 0.02)
MAX_RETRIES = env_int("MAX_RETRIES", 3)
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")


# =========================
# Time / HTTP helpers
# =========================
def now_ts() -> float:
    return time.time()

def iso_to_ts(s: str) -> Optional[float]:
    try:
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
        # keep this visible; it matters for debugging
        log.warning(f"[HTTP] GET failed: {url} ({e})")
        return None


# =========================
# Deterministic slug resolver (fixes your problem)
# =========================
def fifteen_min_bucket_start(ts: float) -> int:
    return int(ts // 900) * 900  # 900s = 15 min

def candidate_slugs(asset: str, now: float) -> List[str]:
    """
    We try current bucket start, previous, next.
    The slug format matches what you're literally seeing in the browser:
      <asset>-updown-15m-<start_epoch_utc>
    """
    a = asset.lower()
    start = fifteen_min_bucket_start(now)
    # try a few around it to handle clock skew / late starts
    starts = [start, start - 900, start + 900, start - 1800, start + 1800]
    return [f"{a}-updown-15m-{s}" for s in starts]

def gamma_expand_event(slug: str) -> Optional[Dict[str, Any]]:
    return http_get_json(f"{GAMMA_HOST}/events/{slug}")

def resolve_active_market(asset: str) -> Optional[Dict[str, Any]]:
    now = now_ts()
    for slug in candidate_slugs(asset, now):
        expanded = gamma_expand_event(slug)
        if not expanded:
            continue

        closed = bool(expanded.get("closed", False))
        enable_ob = bool(expanded.get("enableOrderBook", False))
        end_date = expanded.get("endDate") or expanded.get("end_date") or ""

        token_ids = (
            expanded.get("clobTokenIds")
            or expanded.get("clobTokenIDs")
            or expanded.get("clob_token_ids")
        )

        end_ts = iso_to_ts(end_date) if isinstance(end_date, str) else None
        if closed or not enable_ob or not end_ts or not token_ids or not isinstance(token_ids, list) or len(token_ids) < 2:
            continue

        # start is encoded in slug. parse it:
        try:
            start_ts = int(slug.split("-")[-1])
        except Exception:
            start_ts = int(end_ts - 900)

        # sanity: it should be a 15m window
        # Allow being anywhere from window start to window end
        if now < start_ts - 30:   # allow tiny drift
            continue
        if now > end_ts + 10:
            continue

        return {
            "slug": slug,
            "start_ts": float(start_ts),
            "end_ts": float(end_ts),
            "yes_token": str(token_ids[0]),
            "no_token": str(token_ids[1]),
        }

    return None


# =========================
# CLOB client init (compatible with your installed version)
# =========================
def init_clob_client() -> ClobClient:
    if not PM_PRIVATE_KEY:
        raise RuntimeError("PM_PRIVATE_KEY is missing")
    if not PM_FOUNDER:
        raise RuntimeError("PM_FOUNDER is missing")

    client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PM_PRIVATE_KEY,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FOUNDER,
    )

    if hasattr(client, "create_or_derive_api_creds"):
        try:
            client.create_or_derive_api_creds()
            log.info("[AUTH] derived API creds via create_or_derive_api_creds()")
        except Exception as e:
            log.warning(f"[AUTH] derive creds failed (continuing): {e}")

    return client


# =========================
# Orderbook + trading helpers
# =========================
def best_bid_ask(ob: Any) -> Tuple[Optional[float], Optional[float]]:
    bids = ob.get("bids") if isinstance(ob, dict) else getattr(ob, "bids", None)
    asks = ob.get("asks") if isinstance(ob, dict) else getattr(ob, "asks", None)

    def p(level) -> Optional[float]:
        try:
            if isinstance(level, dict):
                return float(level.get("price"))
            if isinstance(level, (list, tuple)) and len(level) >= 1:
                return float(level[0])
        except Exception:
            return None
        return None

    best_bid = p(bids[0]) if isinstance(bids, list) and bids else None
    best_ask = p(asks[0]) if isinstance(asks, list) and asks else None
    return best_bid, best_ask

def clamp_price(x: float) -> float:
    return max(0.001, min(0.999, float(x)))

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

    price = clamp_price(price)
    size = max(0.000001, float(size))

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if hasattr(client, "create_order") and hasattr(client, "post_order"):
                order = client.create_order(
                    token_id=token_id,
                    side=side,
                    price=price,
                    size=size,
                    time_in_force="GTC",
                )
                client.post_order(order)
                return True

            if hasattr(client, "create_limit_order") and hasattr(client, "post_order"):
                order = client.create_limit_order(
                    token_id=token_id,
                    side=side,
                    price=price,
                    size=size,
                )
                client.post_order(order)
                return True

            raise RuntimeError("No compatible create_order/create_limit_order on this py_clob_client version")

        except Exception as e:
            last_err = e
            log.warning(f"[ORDER] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(0.15 * attempt)

    log.error(f"[ORDER] permanent failure: {last_err}")
    return False


# =========================
# Session state per asset
# =========================
@dataclass
class AssetSession:
    slug: str = ""
    start_ts: float = 0.0
    end_ts: float = 0.0
    yes_token: str = ""
    no_token: str = ""

    initial_bought: bool = False
    eval_index: int = 0
    next_eval_ts: float = 0.0
    exit_ts: float = 0.0

    last_yes_ask: Optional[float] = None
    last_no_ask: Optional[float] = None

    yes_shares: float = 0.0
    no_shares: float = 0.0


def get_prices(client: ClobClient, yes_token: str, no_token: str) -> Optional[Tuple[float, float, float, float]]:
    try:
        ob_yes = client.get_order_book(yes_token)
        ob_no = client.get_order_book(no_token)
        yb, ya = best_bid_ask(ob_yes)
        nb, na = best_bid_ask(ob_no)
        if yb is None or ya is None or nb is None or na is None:
            return None
        return float(yb), float(ya), float(nb), float(na)
    except Exception as e:
        log.warning(f"[ORDERBOOK] fetch failed: {e}")
        return None


def refresh_session(asset: str, sess: AssetSession) -> AssetSession:
    info = resolve_active_market(asset)
    if not info:
        log.info(f"[MARKET] {asset} no active 15m market found (deterministic slug search)")
        return sess

    if info["slug"] != sess.slug:
        sess = AssetSession(
            slug=info["slug"],
            start_ts=info["start_ts"],
            end_ts=info["end_ts"],
            yes_token=info["yes_token"],
            no_token=info["no_token"],
            initial_bought=False,
            eval_index=0,
            next_eval_ts=info["start_ts"] + EVAL_INTERVAL_SEC,
            exit_ts=info["start_ts"] + EXIT_AT_SEC,
            last_yes_ask=None,
            last_no_ask=None,
            yes_shares=0.0,
            no_shares=0.0,
        )
        log.info(
            f"[MARKET] {asset} ACTIVE slug={sess.slug} "
            f"start={datetime.fromtimestamp(sess.start_ts, tz=timezone.utc)} "
            f"end={datetime.fromtimestamp(sess.end_ts, tz=timezone.utc)}"
        )

    return sess


def run_ladder(client: ClobClient, asset: str, sess: AssetSession, rl: Dict[str, Any]) -> AssetSession:
    if not sess.slug:
        return sess

    now = now_ts()
    if now < sess.start_ts:
        return sess

    # window ended -> reset next refresh
    if now > sess.end_ts + 3:
        return sess

    # exit: sell all at ~best bid
    if sess.initial_bought and now >= sess.exit_ts:
        prices = get_prices(client, sess.yes_token, sess.no_token)
        if prices and allow_trade(rl):
            yb, ya, nb, na = prices
            sell_yes = clamp_price(yb * (1.0 - MAX_SLIPPAGE))
            sell_no = clamp_price(nb * (1.0 - MAX_SLIPPAGE))
            if sess.yes_shares > 0:
                place_limit_order(client, sess.yes_token, SELL, sell_yes, sess.yes_shares)
            if sess.no_shares > 0:
                place_limit_order(client, sess.no_token, SELL, sell_no, sess.no_shares)
            log.info(f"[EXIT] {asset} sold YES+NO positions slug={sess.slug}")
        return sess

    # initial: buy both sides immediately once in-window
    if not sess.initial_bought:
        prices = get_prices(client, sess.yes_token, sess.no_token)
        if prices and allow_trade(rl):
            yb, ya, nb, na = prices
            buy_yes = clamp_price(ya * (1.0 + MAX_SLIPPAGE))
            buy_no = clamp_price(na * (1.0 + MAX_SLIPPAGE))
            yes_size = BASE_USDC / buy_yes
            no_size = BASE_USDC / buy_no

            ok1 = place_limit_order(client, sess.yes_token, BUY, buy_yes, yes_size)
            ok2 = place_limit_order(client, sess.no_token, BUY, buy_no, no_size)

            if ok1:
                sess.yes_shares += yes_size
            if ok2:
                sess.no_shares += no_size

            if ok1 and ok2:
                sess.initial_bought = True
                sess.last_yes_ask = ya
                sess.last_no_ask = na
                log.info(
                    f"[INIT] {asset} bought both sides BASE_USDC={BASE_USDC:.2f} "
                    f"(yes_ask={ya:.4f}, no_ask={na:.4f}) slug={sess.slug}"
                )

        time.sleep(COOLDOWN_SEC)
        return sess

    # ladder eval every 2 minutes, 6 times
    if sess.eval_index < EVAL_COUNT and now >= sess.next_eval_ts:
        prices = get_prices(client, sess.yes_token, sess.no_token)
        if not prices:
            return sess

        yb, ya, nb, na = prices

        if sess.last_yes_ask is None or sess.last_no_ask is None:
            sess.last_yes_ask, sess.last_no_ask = ya, na

        dy = ya - sess.last_yes_ask
        dn = na - sess.last_no_ask

        # leader = the side whose *price increased more* since last eval
        leader = "YES" if dy > dn else "NO"
        leader_ask = ya if leader == "YES" else na
        leader_token = sess.yes_token if leader == "YES" else sess.no_token

        log.info(
            f"[EVAL] {asset} {sess.eval_index+1}/{EVAL_COUNT} "
            f"yes_ask={ya:.4f} (d={dy:+.4f}) no_ask={na:.4f} (d={dn:+.4f}) leader={leader}"
        )

        if allow_trade(rl):
            buy_price = clamp_price(leader_ask * (1.0 + MAX_SLIPPAGE))
            size = STEP_USDC / buy_price
            ok = place_limit_order(client, leader_token, BUY, buy_price, size)
            if ok:
                if leader == "YES":
                    sess.yes_shares += size
                else:
                    sess.no_shares += size
                log.info(f"[LADDER] {asset} bought +{STEP_USDC:.2f} on {leader} @ {buy_price:.4f}")

        sess.eval_index += 1
        sess.next_eval_ts = sess.start_ts + (sess.eval_index + 1) * EVAL_INTERVAL_SEC
        sess.last_yes_ask, sess.last_no_ask = ya, na

        time.sleep(COOLDOWN_SEC)

    return sess


# =========================
# Main
# =========================
def main():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC}")
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

        if t - last_resolve >= RESOLVE_EVERY_SEC:
            last_resolve = t
            for a in ASSETS:
                sessions[a] = refresh_session(a, sessions[a])

        for a in ASSETS:
            sessions[a] = run_ladder(client, a, sessions[a], rl)

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
