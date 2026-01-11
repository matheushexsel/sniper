# pump_sniper.py
# Polymarket Copy Trader (public activity -> copy trades with your sizing)
#
# What it does:
# - Polls Polymarket Data API for TARGET_USER public trade activity (type=TRADE)
# - Dedupes events to avoid double-copying across restarts
# - Copies BUY trades (SELL optional) with your own sizing rules (SIZE_MULT, MIN_USDC, MAX_USDC)
# - Places orders on Polymarket CLOB via py-clob-client
# - Uses FOK (fill-or-kill) to avoid hanging exposure
# - Has compatibility fallback: if MarketOrderArgs signature differs, it falls back to a limit-FOK order
#
# Install deps:
#   pip install requests py-clob-client
#
# Required env vars:
#   TARGET_USER         0x... (target wallet address observed by Data API)
#   PM_PRIVATE_KEY      your Polygon private key for signing orders
#   PM_FUNDER           your Polymarket funder/proxy wallet address holding USDC.e
#
# Recommended env vars:
#   MAX_USDC=2
#   COPY_SELLS=false
#   MAX_SIGNAL_AGE_SEC=15

import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

import requests

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL


# ===================== UTIL =====================

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

def env_str(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v not in (None, "") else float(default)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else int(default)

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# ===================== CONFIG =====================

# Target (public)
TARGET_USER = env_str("TARGET_USER")  # 0x... as seen by Polymarket Data API
DATA_API_BASE = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").strip().rstrip("/")
POLL_SEC = env_float("POLL_SEC", 2.0)
FETCH_LIMIT = env_int("FETCH_LIMIT", 50)

# Copy behavior
COPY_BUYS = env_bool("COPY_BUYS", True)
COPY_SELLS = env_bool("COPY_SELLS", False)  # default off

# Position sizing
SIZE_MULT = env_float("SIZE_MULT", 1.0)
MIN_USDC = env_float("MIN_USDC", 1.0)
MAX_USDC = env_float("MAX_USDC", 2.0)  # you requested cap at $2

# Latency / staleness protection
MAX_SIGNAL_AGE_SEC = env_int("MAX_SIGNAL_AGE_SEC", 15)

# Risk controls
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 6)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

# Trading (your account)
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip().rstrip("/")
CHAIN_ID = env_int("CHAIN_ID", 137)  # Polygon
PM_PRIVATE_KEY = env_str("PM_PRIVATE_KEY")
PM_FUNDER = env_str("PM_FUNDER")
PM_SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 1)

# State
STATE_PATH = os.getenv("STATE_PATH", "./copy_state.json").strip()

# Safety / verbosity
LOG_SKIPS = env_bool("LOG_SKIPS", True)


# ===================== HTTP =====================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "polymarket-copytrader/1.0"})

def http_get_json(url: str, timeout: float = 12.0, retries: int = 3) -> Any:
    last = None
    for i in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.35 * (2 ** i))
    raise RuntimeError(f"GET failed after {retries} retries: {url} err={last}")


# ===================== STATE / DEDUPE =====================

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            s = json.load(f)
            if not isinstance(s, dict):
                return {"seen": []}
            if "seen" not in s or not isinstance(s["seen"], list):
                s["seen"] = []
            return s
    except Exception:
        return {"seen": []}

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)

def event_key(e: Dict[str, Any]) -> str:
    # Use transactionHash when present; combine with key fields to be safe
    tx = str(e.get("transactionHash") or "")
    asset = str(e.get("asset") or "")
    side = str(e.get("side") or "")
    t = str(e.get("timestamp") or "")
    usdc = str(e.get("usdcSize") or e.get("size") or "")
    raw = f"{tx}|{asset}|{side}|{t}|{usdc}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def seen_has(state: Dict[str, Any], k: str) -> bool:
    return k in (state.get("seen") or [])

def seen_add(state: Dict[str, Any], k: str, max_keep: int = 3000) -> None:
    seen = state.get("seen") or []
    seen.append(k)
    if len(seen) > max_keep:
        seen = seen[-max_keep:]
    state["seen"] = seen


# ===================== POLYMARKET CLIENT =====================

def init_clob_client() -> ClobClient:
    client = ClobClient(
        CLOB_HOST,
        key=PM_PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FUNDER,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


# ===================== COPY LOGIC =====================

def fetch_target_trades(limit: int) -> List[Dict[str, Any]]:
    url = f"{DATA_API_BASE}/activity?user={TARGET_USER}&limit={limit}&offset=0"
    data = http_get_json(url, timeout=12.0, retries=3)
    if not isinstance(data, list):
        return []
    out = [x for x in data if str(x.get("type") or "").upper() == "TRADE"]
    return out

def should_copy_side(side: str) -> bool:
    s = side.upper().strip()
    if s == "BUY":
        return COPY_BUYS
    if s == "SELL":
        return COPY_SELLS
    return False

def compute_my_usdc(target_usdc: float) -> float:
    my = float(target_usdc) * float(SIZE_MULT)
    my = max(float(MIN_USDC), min(float(MAX_USDC), my))
    return float(f"{my:.6f}")

def normalize_timestamp_to_seconds(tstamp: int) -> int:
    # Heuristic: ms if huge
    return int(tstamp / 1000) if tstamp > 10_000_000_000 else tstamp

def rate_limit_ok(trade_times: List[float]) -> bool:
    now = time.time()
    trade_times[:] = [t for t in trade_times if (now - t) <= 60.0]
    return len(trade_times) < int(MAX_TRADES_PER_MIN)

def clamp_price_01_99(px: float) -> float:
    # Known Polymarket constraints in many endpoints: price in [0.01, 0.99]
    if px <= 0:
        return 0.0
    return max(0.01, min(0.99, float(px)))


def place_copy_trade(client: ClobClient, e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Preferred: MarketOrderArgs FOK
    Fallback: limit order (OrderArgs) with FOK at current price
    """
    asset = str(e.get("asset") or "").strip()
    side = str(e.get("side") or "").strip().upper()

    if not asset or side not in ("BUY", "SELL"):
        if LOG_SKIPS:
            log(f"SKIP: missing asset/side asset={asset} side={side}")
        return None

    if not should_copy_side(side):
        if LOG_SKIPS:
            log(f"SKIP: side not enabled side={side}")
        return None

    # Amount from Data API
    raw_usdc = e.get("usdcSize")
    if raw_usdc is None:
        raw_usdc = e.get("size")

    try:
        target_usdc = float(raw_usdc)
    except Exception:
        if LOG_SKIPS:
            log(f"SKIP: bad usdcSize/size raw={raw_usdc}")
        return None

    if target_usdc <= 0:
        if LOG_SKIPS:
            log("SKIP: target_usdc<=0")
        return None

    # Staleness
    try:
        tstamp = int(e.get("timestamp") or 0)
    except Exception:
        tstamp = 0

    if tstamp > 0:
        seen_sec = normalize_timestamp_to_seconds(tstamp)
        age = int(time.time()) - seen_sec
        if age > int(MAX_SIGNAL_AGE_SEC):
            if LOG_SKIPS:
                log(f"SKIP: stale age={age}s > {MAX_SIGNAL_AGE_SEC}s asset={asset} side={side}")
            return None

    my_usdc = compute_my_usdc(target_usdc)
    if my_usdc < float(MIN_USDC):
        if LOG_SKIPS:
            log(f"SKIP: my_usdc<{MIN_USDC} my_usdc={my_usdc}")
        return None

    order_side = BUY if side == "BUY" else SELL

    # PATH A: Market order (FOK)
    try:
        mo = MarketOrderArgs(
            token_id=asset,
            amount=my_usdc,
            side=order_side,
            order_type=OrderType.FOK,
        )
        signed = client.create_market_order(mo)
        resp = client.post_order(signed, OrderType.FOK)
        return resp

    except TypeError as te:
        # Compatibility fallback (older/newer py-clob-client API)
        log(f"MarketOrderArgs incompatibility: {te} -> fallback to limit FOK")

        # Pull a current price from CLOB
        # The library exposes client.get_price(token_id, side="BUY"/"SELL") in many versions.
        try:
            px_side = "BUY" if side == "BUY" else "SELL"
            px = float(client.get_price(asset, side=px_side) or 0.0)
            px = clamp_price_01_99(px)
            if px <= 0:
                log(f"SKIP: could not fetch valid price for asset={asset}")
                return None
        except Exception as ex:
            log(f"SKIP: get_price failed asset={asset} err={ex}")
            return None

        # Convert USDC -> shares
        size_shares = float(my_usdc) / float(px)
        if size_shares <= 0:
            log(f"SKIP: size_shares<=0 my_usdc={my_usdc} px={px}")
            return None

        # Use OrderArgs limit order with FOK
        try:
            from py_clob_client.clob_types import OrderArgs
            order = OrderArgs(
                token_id=asset,
                price=px,
                size=size_shares,
                side=order_side,
            )
            signed = client.create_order(order)
            resp = client.post_order(signed, OrderType.FOK)
            return resp
        except Exception as ex:
            log(f"ORDER fallback error: {ex}")
            return None

    except Exception as ex:
        log(f"ORDER error: {ex}")
        return None


# ===================== MAIN LOOP =====================

def main() -> None:
    log("BOOT CONFIG:")
    log(f"  TARGET_USER={TARGET_USER}")
    log(f"  DATA_API_BASE={DATA_API_BASE}")
    log(f"  POLL_SEC={POLL_SEC} FETCH_LIMIT={FETCH_LIMIT}")
    log(f"  COPY_BUYS={COPY_BUYS} COPY_SELLS={COPY_SELLS}")
    log(f"  SIZE_MULT={SIZE_MULT} MIN_USDC={MIN_USDC} MAX_USDC={MAX_USDC}")
    log(f"  MAX_SIGNAL_AGE_SEC={MAX_SIGNAL_AGE_SEC}")
    log(f"  MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}")
    log(f"  CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} SIGNATURE_TYPE={PM_SIGNATURE_TYPE}")
    log(f"  FUNDER={PM_FUNDER}")
    log(f"  STATE_PATH={STATE_PATH}")
    log(f"  LOG_SKIPS={LOG_SKIPS}")

    state = load_state()
    client = init_clob_client()
    trade_times: List[float] = []

    while True:
        try:
            events = fetch_target_trades(int(FETCH_LIMIT))
            log(f"Fetched {len(events)} TRADE events for target={TARGET_USER}")

            # Process oldest -> newest so you mirror in order
            def _ts_sort(x: Dict[str, Any]) -> int:
                try:
                    return int(x.get("timestamp") or 0)
                except Exception:
                    return 0

            events_sorted = sorted(events, key=_ts_sort)

            did_work = False

            for e in events_sorted:
                k = event_key(e)
                if seen_has(state, k):
                    continue

                # Mark as seen first (idempotency). If execution fails, we still won't spam.
                seen_add(state, k)
                save_state(state)

                side = str(e.get("side") or "").upper()
                asset = str(e.get("asset") or "")
                title = str(e.get("title") or "")
                price = e.get("price")
                usdc_size = e.get("usdcSize")

                if not should_copy_side(side):
                    if LOG_SKIPS:
                        log(f"SKIP: copying disabled for side={side}")
                    continue

                if not rate_limit_ok(trade_times):
                    log("RISK: rate limit hit, skipping remaining events this cycle")
                    break

                resp = place_copy_trade(client, e)
                if resp is None:
                    if LOG_SKIPS:
                        log(f"NO-EXEC: side={side} asset={asset} usdcSize={usdc_size} title={title[:80]}")
                    continue

                trade_times.append(time.time())
                did_work = True

                try:
                    target_usdc_f = float(usdc_size or 0.0)
                except Exception:
                    target_usdc_f = 0.0

                log(
                    f"COPIED {side} | asset(token_id)={asset} | target_usdc={target_usdc_f:.6f} "
                    f"-> my_usdc={compute_my_usdc(target_usdc_f):.6f} | ref_price={price} | title={title[:80]}"
                )
                log(f"  order_resp={resp}")

                time.sleep(float(COOLDOWN_SEC))

            # backoff
            if not did_work:
                time.sleep(float(POLL_SEC))
            else:
                time.sleep(max(0.25, float(POLL_SEC) / 2.0))

        except Exception as ex:
            log(f"LOOP error: {ex}")
            time.sleep(max(2.0, float(POLL_SEC)))


if __name__ == "__main__":
    main()
