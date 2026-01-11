# polymarket_copytrader.py
# Copy-trades a target Polymarket wallet/profile activity (public) with your own sizing.
#
# Data source: Polymarket Data API /activity (public) for target wallet trades.
# Execution: Polymarket CLOB via py-clob-client.
#
# Requirements:
#   pip install py-clob-client requests
#
# IMPORTANT OP NOTES:
# - You need a Polygon-compatible private key for trading (EOA OR Polymarket/Magic signature type).
# - You must have USDC.e funded in your Polymarket proxy wallet ("funder") and allowances set.
#   Polymarket docs explain funder/proxy wallet and allowances. (See CLOB quickstart / py-clob-client README.)
#
# This bot intentionally uses FOK orders to avoid hanging exposure from stale copy signals.

import os
import time
import json
import math
import hashlib
from typing import Any, Dict, List, Optional, Tuple

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
TARGET_USER = env_str("TARGET_USER")  # target "user profile address" 0x... (proxy wallet or user wallet)
DATA_API_BASE = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").strip().rstrip("/")
POLL_SEC = env_float("POLL_SEC", 2.0)
FETCH_LIMIT = env_int("FETCH_LIMIT", 50)

# Copy behavior
COPY_BUYS = env_bool("COPY_BUYS", True)
COPY_SELLS = env_bool("COPY_SELLS", False)  # off by default (selling requires you hold the same tokens)

SIZE_MULT = env_float("SIZE_MULT", 1.0)     # my_usdc = target_usdc * SIZE_MULT
MIN_USDC = env_float("MIN_USDC", 1.0)       # ignore tiny signals
MAX_USDC = env_float("MAX_USDC", 50.0)      # cap per copied trade

# Latency / staleness protection
MAX_SIGNAL_AGE_SEC = env_int("MAX_SIGNAL_AGE_SEC", 20)  # if we see a trade older than this, skip

# Risk controls
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 6)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)  # throttle between orders

# Trading (your account)
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip().rstrip("/")
CHAIN_ID = env_int("CHAIN_ID", 137)  # Polygon mainnet
PRIVATE_KEY = env_str("PM_PRIVATE_KEY")
FUNDER = env_str("PM_FUNDER")  # your Polymarket proxy wallet address holding USDC.e
SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 1)  # 1 commonly used for email/Magic per py-clob-client examples

# Storage (lightweight local state)
STATE_PATH = os.getenv("STATE_PATH", "./copy_state.json").strip()


# ===================== HTTP =====================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "polymarket-copytrader/1.0"})

def http_get_json(url: str, timeout: float = 10.0, retries: int = 3) -> Any:
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

def _hash_event(e: Dict[str, Any]) -> str:
    """
    Conservative dedupe key. Uses transactionHash when present.
    Data-API activity includes transactionHash (example schema). :contentReference[oaicite:3]{index=3}
    """
    tx = str(e.get("transactionHash") or "")
    asset = str(e.get("asset") or "")
    side = str(e.get("side") or "")
    ts_ = str(e.get("timestamp") or "")
    usdc = str(e.get("usdcSize") or e.get("size") or "")
    raw = f"{tx}|{asset}|{side}|{ts_}|{usdc}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": [], "last_poll_ts": 0}

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)

def seen_add(state: Dict[str, Any], key: str, max_keep: int = 3000) -> None:
    seen = state.get("seen") or []
    seen.append(key)
    if len(seen) > max_keep:
        seen = seen[-max_keep:]
    state["seen"] = seen

def seen_has(state: Dict[str, Any], key: str) -> bool:
    seen = state.get("seen") or []
    return key in seen


# ===================== POLYMARKET CLIENT =====================

def init_clob_client() -> ClobClient:
    """
    Uses py-clob-client standard setup:
      - instantiate with host/key/chain_id/signature_type/funder
      - derive api creds and set them
    """
    client = ClobClient(
        CLOB_HOST,
        key=PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=SIGNATURE_TYPE,
        funder=FUNDER,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


# ===================== COPY LOGIC =====================

def fetch_target_trades(limit: int) -> List[Dict[str, Any]]:
    """
    Pulls target activity and filters to type=TRADE.
    Endpoint + response schema shown in docs. :contentReference[oaicite:4]{index=4}
    """
    url = f"{DATA_API_BASE}/activity?user={TARGET_USER}&limit={limit}&offset=0"
    data = http_get_json(url, timeout=10.0, retries=3)
    if not isinstance(data, list):
        return []
    # Data API returns mixed activity; keep only trades
    out = [x for x in data if str(x.get("type", "")).upper() == "TRADE"]
    return out

def clamp_amount(x: float) -> float:
    return max(0.0, float(x))

def compute_my_usdc(target_usdc: float) -> float:
    my = target_usdc * float(SIZE_MULT)
    my = max(float(MIN_USDC), min(float(MAX_USDC), my))
    # Keep sane precision (USDC has 6 decimals; exchange may accept float but this avoids noise)
    return float(f"{my:.6f}")

def should_copy_side(side: str) -> bool:
    s = side.upper().strip()
    if s == "BUY":
        return COPY_BUYS
    if s == "SELL":
        return COPY_SELLS
    return False

def rate_limit_ok(trade_times: List[float]) -> bool:
    """
    Allows up to MAX_TRADES_PER_MIN in a rolling 60s window.
    """
    now = time.time()
    trade_times[:] = [t for t in trade_times if (now - t) <= 60.0]
    return len(trade_times) < MAX_TRADES_PER_MIN

def place_copy_trade(client: ClobClient, e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Places a FOK market order sized by USDC amount.
    py-clob-client supports MarketOrderArgs(amount=USDC, token_id=..., side=BUY/SELL, order_type=FOK). :contentReference[oaicite:5]{index=5}
    """
    asset = str(e.get("asset") or "").strip()
    side = str(e.get("side") or "").strip().upper()

    # target amount
    target_usdc = e.get("usdcSize")
    if target_usdc is None:
        # fallback to "size" if usdcSize absent (some endpoints show size/price; activity shows usdcSize) :contentReference[oaicite:6]{index=6}
        target_usdc = e.get("size")

    try:
        target_usdc = float(target_usdc)
    except Exception:
        return None

    if target_usdc <= 0:
        return None

    # staleness check (Data API timestamps are integers; docs show 'timestamp'). :contentReference[oaicite:7]{index=7}
    try:
        tstamp = int(e.get("timestamp") or 0)
    except Exception:
        tstamp = 0

    # timestamp unit can vary by API implementations (seconds vs ms). Normalize by heuristic.
    now = int(time.time())
    age = None
    if tstamp > 0:
        if tstamp > 10_000_000_000:  # likely ms
            age = now - int(tstamp / 1000)
        else:  # likely seconds
            age = now - tstamp

    if age is not None and age > MAX_SIGNAL_AGE_SEC:
        return None

    if not asset or side not in ("BUY", "SELL"):
        return None

    if not should_copy_side(side):
        return None

    my_usdc = compute_my_usdc(target_usdc)
    if my_usdc < float(MIN_USDC):
        return None

    order_side = BUY if side == "BUY" else SELL

    mo = MarketOrderArgs(
        token_id=asset,
        amount=my_usdc,
        side=order_side,
        order_type=OrderType.FOK,  # fill-or-kill: do it now or not at all
    )
    signed = client.create_market_order(mo)
    resp = client.post_order(signed, OrderType.FOK)
    return resp


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
    log(f"  CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} SIGNATURE_TYPE={SIGNATURE_TYPE}")
    log(f"  FUNDER={FUNDER}")
    log(f"  STATE_PATH={STATE_PATH}")

    state = load_state()
    client = init_clob_client()

    trade_times: List[float] = []

    while True:
        try:
            events = fetch_target_trades(FETCH_LIMIT)

            # newest first is typical; we process oldest->newest to preserve order
            events_sorted = sorted(
                events,
                key=lambda x: int(x.get("timestamp") or 0)
            )

            did_any = False

            for e in events_sorted:
                k = _hash_event(e)
                if seen_has(state, k):
                    continue

                # Mark as seen FIRST (idempotency): avoids double fire if execution throws
                seen_add(state, k)
                save_state(state)

                side = str(e.get("side") or "").upper()
                asset = str(e.get("asset") or "")
                title = str(e.get("title") or "")
                price = e.get("price")

                if not should_copy_side(side):
                    continue

                if not rate_limit_ok(trade_times):
                    log("RISK: rate limit hit, skipping further trades this minute")
                    break

                # Attempt execution
                resp = place_copy_trade(client, e)
                if resp is None:
                    continue

                trade_times.append(time.time())
                did_any = True

                log(
                    f"COPIED {side} | token_id(asset)={asset} | target_usdc={e.get('usdcSize')} "
                    f"-> my_usdc={compute_my_usdc(float(e.get('usdcSize') or e.get('size') or 0.0)):.6f} "
                    f"| price={price} | title={title[:80]}"
                )
                log(f"  order_resp={resp}")

                time.sleep(COOLDOWN_SEC)

            if not did_any:
                time.sleep(POLL_SEC)
            else:
                # if we did work, do a faster next poll
                time.sleep(max(0.25, POLL_SEC / 2))

        except Exception as ex:
            log(f"LOOP error: {ex}")
            time.sleep(max(2.0, POLL_SEC))


if __name__ == "__main__":
    main()
