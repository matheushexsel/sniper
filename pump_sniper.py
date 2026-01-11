# pump_sniper.py
# Polymarket Copy Trader + Auto-Close (sell before 15-min boundary)
#
# Adds:
# - Track positions (shares) per token_id from order responses
# - Auto-close: at ET boundary - CLOSE_BEFORE_BOUNDARY_SEC, sell all tracked positions (FOK)
# - Retries inside the close window with MAX_CLOSE_ATTEMPTS
# - Marks events as "seen" only after a successful copy order (prevents losing signals on auth errors)

import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
MAX_USDC = env_float("MAX_USDC", 2.0)

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
PM_SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 2)

# Auto-close behavior (new)
AUTO_CLOSE_ON_INTERVAL = env_bool("AUTO_CLOSE_ON_INTERVAL", True)
INTERVAL_MINUTES = env_int("INTERVAL_MINUTES", 15)
CLOSE_BEFORE_BOUNDARY_SEC = env_int("CLOSE_BEFORE_BOUNDARY_SEC", 30)
MAX_CLOSE_ATTEMPTS = env_int("MAX_CLOSE_ATTEMPTS", 20)
CLOSE_LOOP_SEC = env_float("CLOSE_LOOP_SEC", 1.0)

# State
STATE_PATH = os.getenv("STATE_PATH", "./copy_state.json").strip()

# Safety / verbosity
LOG_SKIPS = env_bool("LOG_SKIPS", True)

ET = ZoneInfo("America/New_York")


# ===================== HTTP =====================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "polymarket-copytrader/1.1"})

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
                s = {}
    except Exception:
        s = {}

    if "seen" not in s or not isinstance(s["seen"], list):
        s["seen"] = []

    # Track positions: token_id -> shares (float)
    if "positions" not in s or not isinstance(s["positions"], dict):
        s["positions"] = {}

    # Auto-close bookkeeping
    if "last_close_boundary_id" not in s:
        s["last_close_boundary_id"] = ""

    if "close_attempts" not in s or not isinstance(s["close_attempts"], dict):
        s["close_attempts"] = {}

    return s

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)

def event_key(e: Dict[str, Any]) -> str:
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

def pos_add(state: Dict[str, Any], token_id: str, delta_shares: float) -> None:
    p = state.get("positions") or {}
    cur = float(p.get(token_id, 0.0))
    cur += float(delta_shares)
    if cur <= 1e-9:
        p.pop(token_id, None)
    else:
        p[token_id] = float(f"{cur:.12f}")
    state["positions"] = p

def pos_snapshot(state: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    p = state.get("positions") or {}
    for k, v in p.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    return out


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
    return [x for x in data if str(x.get("type") or "").upper() == "TRADE"]

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
    return int(tstamp / 1000) if tstamp > 10_000_000_000 else tstamp

def rate_limit_ok(trade_times: List[float]) -> bool:
    now = time.time()
    trade_times[:] = [t for t in trade_times if (now - t) <= 60.0]
    return len(trade_times) < int(MAX_TRADES_PER_MIN)

def clamp_price_01_99(px: float) -> float:
    if px <= 0:
        return 0.0
    return max(0.01, min(0.99, float(px)))

def extract_filled_shares_from_resp(resp: Dict[str, Any]) -> float:
    """
    For your BUY logs:
      takingAmount ~ shares
      makingAmount ~ USDC
    We'll use takingAmount when it looks valid.
    """
    try:
        ta = float(resp.get("takingAmount") or 0.0)
        if ta > 0:
            return ta
    except Exception:
        pass
    return 0.0


def place_copy_trade(client: ClobClient, e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

    # PATH A: Market order (FOK) -- this is working in your current setup
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
        log(f"MarketOrderArgs incompatibility: {te} -> fallback to limit FOK")

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

        size_shares = float(my_usdc) / float(px)
        if size_shares <= 0:
            log(f"SKIP: size_shares<=0 my_usdc={my_usdc} px={px}")
            return None

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


# ===================== AUTO CLOSE =====================

def next_boundary_et(now: datetime) -> datetime:
    # boundaries at :00 :15 :30 :45
    minute = now.minute
    next_q = ((minute // 15) + 1) * 15
    if next_q >= 60:
        return (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return now.replace(minute=next_q, second=0, microsecond=0)

def boundary_id(dt: datetime) -> str:
    return dt.strftime("%Y%m%d-%H%M")

def in_close_window(now: datetime, boundary: datetime, close_before_sec: int) -> bool:
    trigger = boundary - timedelta(seconds=int(close_before_sec))
    return now >= trigger and now < boundary

def place_sell_shares_fok(client: ClobClient, token_id: str, shares: float) -> Optional[Dict[str, Any]]:
    """
    Place SELL for 'shares' using limit-FOK at current SELL price (best executable),
    because MarketOrderArgs takes 'amount' in USDC, not shares.
    We'll query price and convert shares->usdc target.
    """
    if shares <= 0:
        return None

    # Fetch a sell-side price reference
    try:
        px = float(client.get_price(token_id, side="SELL") or 0.0)
        px = clamp_price_01_99(px)
        if px <= 0:
            return None
    except Exception as ex:
        log(f"AUTO-CLOSE: get_price failed token={token_id} err={ex}")
        return None

    # Use OrderArgs limit order for SELL shares
    try:
        from py_clob_client.clob_types import OrderArgs
        order = OrderArgs(
            token_id=token_id,
            price=px,
            size=float(shares),
            side=SELL,
        )
        signed = client.create_order(order)
        resp = client.post_order(signed, OrderType.FOK)
        return resp
    except Exception as ex:
        log(f"AUTO-CLOSE: SELL order error token={token_id} err={ex}")
        return None


def maybe_auto_close(client: ClobClient, state: Dict[str, Any]) -> None:
    """
    Called each loop iteration. If we're inside the close window,
    attempt to sell all tracked positions. Retry up to MAX_CLOSE_ATTEMPTS for that boundary.
    """
    if not AUTO_CLOSE_ON_INTERVAL:
        return

    now = datetime.now(tz=ET)
    boundary = next_boundary_et(now)
    bid = boundary_id(boundary)

    if not in_close_window(now, boundary, CLOSE_BEFORE_BOUNDARY_SEC):
        return

    attempts_map = state.get("close_attempts") or {}
    attempts = int(attempts_map.get(bid, 0))
    if attempts >= int(MAX_CLOSE_ATTEMPTS):
        return

    # Increment attempt counter and persist (so restarts keep behavior)
    attempts_map[bid] = attempts + 1
    state["close_attempts"] = attempts_map
    save_state(state)

    positions = pos_snapshot(state)
    if not positions:
        state["last_close_boundary_id"] = bid
        save_state(state)
        return

    # Sell everything we currently track
    for token_id, shares in positions.items():
        if shares <= 0:
            continue

        resp = place_sell_shares_fok(client, token_id, shares)
        if resp is None:
            if LOG_SKIPS:
                log(f"AUTO-CLOSE: NO-EXEC SELL token={token_id} shares={shares:.6f}")
            continue

        ok = bool(resp.get("success"))
        status = str(resp.get("status") or "")

        log(f"AUTO-CLOSE: SELL token={token_id} shares={shares:.6f} resp_success={ok} status={status}")
        log(f"  sell_resp={resp}")

        # If matched, subtract shares (best-effort)
        if ok and status == "matched":
            # Often, for limit SELL, making/taking amounts can vary by API.
            # We assume 'makingAmount' ~ shares when SELLing outcome tokens.
            # If your logs show otherwise, we can swap this.
            sold_shares = 0.0
            try:
                sold_shares = float(resp.get("makingAmount") or 0.0)
            except Exception:
                sold_shares = 0.0

            # Fallback: if makingAmount looks like USDC (~<=2) but shares are larger, use takingAmount.
            if sold_shares <= 3.0:
                try:
                    ta = float(resp.get("takingAmount") or 0.0)
                    if ta > sold_shares:
                        sold_shares = ta
                except Exception:
                    pass

            if sold_shares <= 0:
                # worst-case: assume we sold what we asked (FOK matched usually implies full)
                sold_shares = shares

            pos_add(state, token_id, -sold_shares)
            save_state(state)


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
    log(f"  AUTO_CLOSE_ON_INTERVAL={AUTO_CLOSE_ON_INTERVAL} INTERVAL_MINUTES={INTERVAL_MINUTES} CLOSE_BEFORE_BOUNDARY_SEC={CLOSE_BEFORE_BOUNDARY_SEC}")
    log(f"  MAX_CLOSE_ATTEMPTS={MAX_CLOSE_ATTEMPTS} CLOSE_LOOP_SEC={CLOSE_LOOP_SEC}")
    log(f"  LOG_SKIPS={LOG_SKIPS}")

    state = load_state()
    client = init_clob_client()
    trade_times: List[float] = []

    while True:
        try:
            # Auto-close runs every loop
            maybe_auto_close(client, state)

            events = fetch_target_trades(int(FETCH_LIMIT))
            log(f"Fetched {len(events)} TRADE events for target={TARGET_USER}")

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

                # Only mark as seen after we actually placed an order (prevents losing signals on errors)
                seen_add(state, k)
                save_state(state)

                # Track positions for BUYs so we can close later
                if side == "BUY" and bool(resp.get("success")) and str(resp.get("status") or "") == "matched":
                    filled_shares = extract_filled_shares_from_resp(resp)
                    if filled_shares > 0:
                        pos_add(state, asset, filled_shares)
                        save_state(state)

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

            # pacing
            if not did_work:
                time.sleep(float(POLL_SEC))
            else:
                time.sleep(max(float(CLOSE_LOOP_SEC), 0.25))

        except Exception as ex:
            log(f"LOOP error: {ex}")
            time.sleep(max(2.0, float(POLL_SEC)))


if __name__ == "__main__":
    main()
