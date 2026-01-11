# pump_sniper.py
# Polymarket Copy Trader + Auto-Close Scheduler (sell near 15-min boundary)
#
# What it does:
# - Polls Polymarket Data API for TARGET_USER public trade activity (type=TRADE)
# - Dedupes events to avoid double-copying across restarts
# - Copies BUY trades (SELL optional) with your own sizing rules (SIZE_MULT, MIN_USDC, MAX_USDC)
# - Places orders on Polymarket CLOB via py-clob-client
# - Uses FOK (fill-or-kill) to avoid hanging exposure
# - Tracks your filled shares per token_id in state["positions"]
# - Optional auto-close: sells tracked positions starting exactly CLOSE_BEFORE_BOUNDARY_SEC before each INTERVAL_MINUTES boundary
#   (armed earlier for reliability; execution is still at T-30 if you set CLOSE_BEFORE_BOUNDARY_SEC=30)
#
# Required env vars:
#   TARGET_USER         0x... (target wallet address observed by Data API)
#   PM_PRIVATE_KEY      your Polygon private key for signing orders
#   PM_FUNDER           your Polymarket funder/proxy wallet address holding USDC.e
#
# Your current vars (example):
#   MAX_USDC=2
#   COPY_SELLS=false
#   MAX_SIGNAL_AGE_SEC=35
#
# Auto-close vars (optional):
#   AUTO_CLOSE_ON_INTERVAL=true
#   INTERVAL_MINUTES=15
#   CLOSE_BEFORE_BOUNDARY_SEC=30
#   ARM_BEFORE_BOUNDARY_SEC=120
#   STOP_TRYING_BEFORE_BOUNDARY_SEC=3
#   CLOSE_LOOP_SEC=0.25
#   MAX_CLOSE_ATTEMPTS=200
#   INTERVAL_TZ_OFFSET_SEC=0   # optional: shift boundary schedule to match ET, etc.
#                              # e.g. ET standard = -18000, ET DST = -14400
#
# Install deps:
#   pip install requests py-clob-client

import os
import time
import json
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
MAX_USDC = env_float("MAX_USDC", 2.0)  # cap at $2

# Latency / staleness protection
MAX_SIGNAL_AGE_SEC = env_int("MAX_SIGNAL_AGE_SEC", 35)

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

# Auto-close (optional)
AUTO_CLOSE_ON_INTERVAL = env_bool("AUTO_CLOSE_ON_INTERVAL", False)
INTERVAL_MINUTES = env_int("INTERVAL_MINUTES", 15)
CLOSE_BEFORE_BOUNDARY_SEC = env_int("CLOSE_BEFORE_BOUNDARY_SEC", 30)

ARM_BEFORE_BOUNDARY_SEC = env_int("ARM_BEFORE_BOUNDARY_SEC", 120)   # "armed" window starts here (no selling yet)
STOP_TRYING_BEFORE_BOUNDARY_SEC = env_int("STOP_TRYING_BEFORE_BOUNDARY_SEC", 3)  # stop trying at T-3

MAX_CLOSE_ATTEMPTS = env_int("MAX_CLOSE_ATTEMPTS", 200)
CLOSE_LOOP_SEC = env_float("CLOSE_LOOP_SEC", 0.25)

# Optional: shift boundaries to match a timezone clock (e.g., ET)
INTERVAL_TZ_OFFSET_SEC = env_int("INTERVAL_TZ_OFFSET_SEC", 0)


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
                return {"seen": [], "positions": {}, "close_window": {}}
            if "seen" not in s or not isinstance(s["seen"], list):
                s["seen"] = []
            if "positions" not in s or not isinstance(s["positions"], dict):
                s["positions"] = {}
            if "close_window" not in s or not isinstance(s["close_window"], dict):
                s["close_window"] = {}
            return s
    except Exception:
        return {"seen": [], "positions": {}, "close_window": {}}

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

def positions_get(state: Dict[str, Any]) -> Dict[str, Any]:
    p = state.get("positions")
    if not isinstance(p, dict):
        state["positions"] = {}
        return state["positions"]
    return p


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
    # Heuristic: ms if huge
    return int(tstamp / 1000) if tstamp > 10_000_000_000 else tstamp

def rate_limit_ok(trade_times: List[float]) -> bool:
    now = time.time()
    trade_times[:] = [t for t in trade_times if (now - t) <= 60.0]
    return len(trade_times) < int(MAX_TRADES_PER_MIN)

def clamp_price_01_99(px: float) -> float:
    if px <= 0:
        return 0.0
    return max(0.01, min(0.99, float(px)))

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def place_copy_trade(client: ClobClient, e: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], float]:
    """
    Returns: (order_resp, shares_executed_estimate)

    Preferred: MarketOrderArgs FOK
    Fallback: limit order (OrderArgs) with FOK at current price

    shares_executed_estimate:
      - For BUY market orders, we read resp["takingAmount"] when present.
      - For BUY limit fallback, we use submitted size_shares when resp success.
      - For SELL (if enabled), we cannot reliably infer from resp across versions,
        so we return 0 here and handle SELL share accounting using what we submitted.
    """
    asset = str(e.get("asset") or "").strip()
    side = str(e.get("side") or "").strip().upper()

    if not asset or side not in ("BUY", "SELL"):
        if LOG_SKIPS:
            log(f"SKIP: missing asset/side asset={asset} side={side}")
        return None, 0.0

    if not should_copy_side(side):
        if LOG_SKIPS:
            log(f"SKIP: side not enabled side={side}")
        return None, 0.0

    raw_usdc = e.get("usdcSize")
    if raw_usdc is None:
        raw_usdc = e.get("size")

    target_usdc = safe_float(raw_usdc)
    if target_usdc <= 0:
        if LOG_SKIPS:
            log("SKIP: target_usdc<=0")
        return None, 0.0

    # Staleness
    tstamp = 0
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
            return None, 0.0

    my_usdc = compute_my_usdc(target_usdc)
    if my_usdc < float(MIN_USDC):
        if LOG_SKIPS:
            log(f"SKIP: my_usdc<{MIN_USDC} my_usdc={my_usdc}")
        return None, 0.0

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

        shares_exec = 0.0
        if isinstance(resp, dict) and resp.get("success"):
            # For BUY this is typically the shares you received
            if side == "BUY":
                shares_exec = safe_float(resp.get("takingAmount"))
        return resp, shares_exec

    except TypeError as te:
        log(f"MarketOrderArgs incompatibility: {te} -> fallback to limit FOK")

        # Pull current price
        try:
            px_side = "BUY" if side == "BUY" else "SELL"
            px = safe_float(client.get_price(asset, side=px_side))
            px = clamp_price_01_99(px)
            if px <= 0:
                log(f"SKIP: could not fetch valid price for asset={asset}")
                return None, 0.0
        except Exception as ex:
            log(f"SKIP: get_price failed asset={asset} err={ex}")
            return None, 0.0

        # Convert USDC -> shares
        size_shares = float(my_usdc) / float(px)
        if size_shares <= 0:
            log(f"SKIP: size_shares<=0 my_usdc={my_usdc} px={px}")
            return None, 0.0

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

            shares_exec = 0.0
            if isinstance(resp, dict) and resp.get("success") and side == "BUY":
                # FOK means full fill or kill; treat submitted shares as filled
                shares_exec = float(size_shares)

            return resp, shares_exec

        except Exception as ex:
            log(f"ORDER fallback error: {ex}")
            return None, 0.0

    except Exception as ex:
        log(f"ORDER error: {ex}")
        return None, 0.0


# ===================== POSITION TRACKING =====================

def pos_add_shares(state: Dict[str, Any], token_id: str, shares_delta: float, title: str = "") -> None:
    if shares_delta == 0:
        return
    positions = positions_get(state)
    p = positions.get(token_id) or {}
    cur = safe_float(p.get("shares"))
    new = cur + float(shares_delta)

    if new <= 0.0000001:
        positions.pop(token_id, None)
    else:
        p["shares"] = float(f"{new:.12f}")
        if title:
            p["title"] = title
        p["updated_ts"] = int(time.time())
        positions[token_id] = p

    state["positions"] = positions
    save_state(state)

def pos_snapshot(state: Dict[str, Any]) -> List[Tuple[str, float, str]]:
    positions = positions_get(state)
    out: List[Tuple[str, float, str]] = []
    for token_id, p in positions.items():
        shares = safe_float((p or {}).get("shares"))
        title = str((p or {}).get("title") or "")
        if shares > 0:
            out.append((str(token_id), float(shares), title))
    return out


# ===================== AUTO-CLOSE =====================

def next_interval_boundary(now_ts: float, minutes: int, tz_offset_sec: int) -> int:
    # Build boundaries relative to a shifted clock (tz_offset_sec), then shift back.
    step = minutes * 60
    shifted = int(now_ts) + int(tz_offset_sec)
    next_shifted = ((shifted // step) + 1) * step
    return int(next_shifted - int(tz_offset_sec))

def close_position_limit_fok(client: ClobClient, token_id: str, shares: float) -> Optional[Dict[str, Any]]:
    # Sell hits bid: use BUY side to get best bid price
    if shares <= 0:
        return None

    try:
        px = safe_float(client.get_price(token_id, side="BUY"))  # bid
        px = clamp_price_01_99(px)
        if px <= 0:
            return None
    except Exception as ex:
        log(f"CLOSE get_price failed token_id={token_id} err={ex}")
        return None

    try:
        from py_clob_client.clob_types import OrderArgs
        order = OrderArgs(
            token_id=token_id,
            price=px,
            size=float(shares),
            side=SELL,
        )
        signed = client.create_order(order)
        return client.post_order(signed, OrderType.FOK)
    except Exception as ex:
        log(f"CLOSE order failed token_id={token_id} err={ex}")
        return None

def maybe_auto_close(client: ClobClient, state: Dict[str, Any]) -> None:
    if not AUTO_CLOSE_ON_INTERVAL:
        return

    now = time.time()
    boundary = next_interval_boundary(now, INTERVAL_MINUTES, INTERVAL_TZ_OFFSET_SEC)

    arm_ts = boundary - int(ARM_BEFORE_BOUNDARY_SEC)
    fire_ts = boundary - int(CLOSE_BEFORE_BOUNDARY_SEC)
    stop_ts = boundary - int(STOP_TRYING_BEFORE_BOUNDARY_SEC)

    # Track per-boundary window to avoid spam
    w = state.get("close_window") or {}
    last_boundary = int(w.get("boundary") or 0)

    if last_boundary != boundary:
        w = {"boundary": boundary, "armed_logged": False, "fired_logged": False, "attempts": 0}
        state["close_window"] = w
        save_state(state)

    # Too early
    if now < arm_ts:
        return

    if not w.get("armed_logged"):
        log(
            "AUTO-CLOSE ARMED: "
            f"boundary_utc={time.strftime('%H:%M:%S', time.gmtime(boundary))} "
            f"fire_in={max(0, int(fire_ts - now))}s stop_in={max(0, int(stop_ts - now))}s "
            f"(CLOSE_BEFORE_BOUNDARY_SEC={CLOSE_BEFORE_BOUNDARY_SEC})"
        )
        w["armed_logged"] = True
        state["close_window"] = w
        save_state(state)

    # Not time to fire yet (this is the key: you still execute only at T-30)
    if now < fire_ts:
        return

    # Past stop time
    if now > stop_ts:
        return

    if not w.get("fired_logged"):
        log(f"AUTO-CLOSE FIRING: now_utc={time.strftime('%H:%M:%S', time.gmtime(int(now)))} T-{int(boundary-now)}s")
        w["fired_logged"] = True
        state["close_window"] = w
        save_state(state)

    positions = pos_snapshot(state)
    if not positions:
        return

    # Try closing everything we think we still hold
    for token_id, shares, title in positions:
        if int(w.get("attempts") or 0) >= int(MAX_CLOSE_ATTEMPTS):
            return

        resp = close_position_limit_fok(client, token_id, shares)
        w["attempts"] = int(w.get("attempts") or 0) + 1
        state["close_window"] = w
        save_state(state)

        if isinstance(resp, dict) and resp.get("success"):
            log(f"AUTO-CLOSE SOLD token_id={token_id} shares={shares:.12f} title={title[:70]}")
            # We submitted an FOK sell of all shares; treat as closed.
            pos_add_shares(state, token_id, -shares, title=title)
        else:
            if resp is not None:
                log(f"AUTO-CLOSE RETRY token_id={token_id} shares={shares:.12f} resp={resp}")

        time.sleep(float(CLOSE_LOOP_SEC))


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

    log("AUTO-CLOSE CONFIG:")
    log(f"  AUTO_CLOSE_ON_INTERVAL={AUTO_CLOSE_ON_INTERVAL}")
    log(f"  INTERVAL_MINUTES={INTERVAL_MINUTES} CLOSE_BEFORE_BOUNDARY_SEC={CLOSE_BEFORE_BOUNDARY_SEC}")
    log(f"  ARM_BEFORE_BOUNDARY_SEC={ARM_BEFORE_BOUNDARY_SEC} STOP_TRYING_BEFORE_BOUNDARY_SEC={STOP_TRYING_BEFORE_BOUNDARY_SEC}")
    log(f"  CLOSE_LOOP_SEC={CLOSE_LOOP_SEC} MAX_CLOSE_ATTEMPTS={MAX_CLOSE_ATTEMPTS}")
    log(f"  INTERVAL_TZ_OFFSET_SEC={INTERVAL_TZ_OFFSET_SEC}")

    state = load_state()
    client = init_clob_client()
    trade_times: List[float] = []

    # One-time position snapshot
    ps = pos_snapshot(state)
    if ps:
        log(f"STATE positions loaded: {len(ps)} open tokens")
        for token_id, shares, title in ps[:8]:
            log(f"  token_id={token_id} shares={shares:.12f} title={title[:70]}")

    while True:
        try:
            # Auto-close checks even if no new trades appear
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
                # Keep auto-close responsive during heavy event bursts
                maybe_auto_close(client, state)

                k = event_key(e)
                if seen_has(state, k):
                    continue

                # Mark seen first (idempotency)
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

                resp, shares_exec = place_copy_trade(client, e)
                if resp is None:
                    if LOG_SKIPS:
                        log(f"NO-EXEC: side={side} asset={asset} usdcSize={usdc_size} title={title[:80]}")
                    continue

                if isinstance(resp, dict) and not resp.get("success"):
                    if LOG_SKIPS:
                        log(f"NO-EXEC: side={side} asset={asset} title={title[:80]} resp={resp}")
                    continue

                trade_times.append(time.time())
                did_work = True

                target_usdc_f = safe_float(usdc_size)
                my_usdc = compute_my_usdc(target_usdc_f)

                log(
                    f"COPIED {side} | asset(token_id)={asset} | target_usdc={target_usdc_f:.6f} "
                    f"-> my_usdc={my_usdc:.6f} | ref_price={price} | title={title[:80]}"
                )
                log(f"  order_resp={resp}")

                # Update positions (only reliable for BUY unless you enable SELL + add explicit logic)
                if side == "BUY" and shares_exec > 0:
                    pos_add_shares(state, asset, +shares_exec, title=title)
                    log(f"  POSITION +shares={shares_exec:.12f} token_id={asset}")

                time.sleep(float(COOLDOWN_SEC))

            # Auto-close again after processing
            maybe_auto_close(client, state)

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
