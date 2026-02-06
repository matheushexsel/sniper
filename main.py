import os
import time
import json
import math
import logging
import requests
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timezone

from py_clob_client.client import ClobClient
from py_clob_client.order_builder.constants import BUY, SELL  # correct location

# ----------------------------
# Config helpers
# ----------------------------

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None or v == "" else v

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return int(v)

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return float(v)

def parse_assets(raw: str) -> List[str]:
    raw = raw.strip()
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            return [str(x).strip().upper() for x in arr]
        except Exception:
            pass
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

# ----------------------------
# Logging
# ----------------------------

LOG_LEVEL = env_str("LOG_LEVEL", env_str("LOG_LEVEL", "INFO")).upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("updown15m_ladder")

# ----------------------------
# Env / Strategy config
# ----------------------------

CLOB_HOST   = env_str("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST  = env_str("GAMMA_HOST", "https://gamma-api.polymarket.com")

CHAIN_ID    = env_int("CHAIN_ID", 137)

PM_FUNDER       = env_str("PM_FUNDER")          # your address (0x...)
PM_PRIVATE_KEY  = env_str("PM_PRIVATE_KEY")     # hex private key
PM_SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 1)

ASSETS = parse_assets(env_str("ASSETS", '["ETH","BTC","SOL","XRP"]'))

POLL_SEC          = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)
MAX_SLUG_LOOKUPS  = env_int("MAX_SLUG_LOOKUPS", 50)

# Ladder parameters
BASE_USDC          = env_float("BASE_USDC", 1.0)         # initial buy each side
STEP_USDC          = env_float("STEP_USDC", 1.0)         # add-on buy
EVAL_INTERVAL_SEC  = env_int("EVAL_INTERVAL_SEC", 120)   # 2 minutes
EVAL_COUNT         = env_int("EVAL_COUNT", 6)            # 6 evaluations = 12 minutes
EXIT_AT_SEC        = env_int("EXIT_AT_SEC", 780)         # 13 minutes into 15m

SELL_OPPOSITE      = env_bool("SELL_OPPOSITE", True)     # per your example
DRY_RUN            = env_bool("DRY_RUN", False)

# Execution controls
MAX_SLIPPAGE       = env_float("MAX_SLIPPAGE", 0.02)     # 2%
MAX_RETRIES        = env_int("MAX_RETRIES", 3)
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC       = env_float("COOLDOWN_SEC", 0.25)

# ----------------------------
# HTTP helpers (Gamma)
# ----------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "updown15m-ladder-bot/1.0"})

def http_get_json(url: str, params: Optional[dict] = None, timeout: float = 10.0) -> Optional[dict]:
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"[HTTP] GET failed: {r.url if 'r' in locals() else url} ({e})")
        return None

def gamma_market_by_slug(slug: str) -> Optional[dict]:
    # Correct way per docs: /markets?slug=<slug> :contentReference[oaicite:2]{index=2}
    url = f"{GAMMA_HOST}/markets"
    data = http_get_json(url, params={"slug": slug}, timeout=10.0)
    if not data:
        return None
    # Gamma typically returns a list
    if isinstance(data, list):
        return data[0] if data else None
    # Sometimes wrapped
    if isinstance(data, dict) and "markets" in data and isinstance(data["markets"], list):
        return data["markets"][0] if data["markets"] else None
    return None

def gamma_events_recent(limit: int = 50, offset: int = 0) -> List[dict]:
    url = f"{GAMMA_HOST}/events"
    # Keep params conservative; avoid invalid sort/order combos that caused your 422s
    params = {"limit": limit, "offset": offset, "closed": "false"}
    data = http_get_json(url, params=params, timeout=10.0)
    if not data:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "events" in data:
        return data["events"] if isinstance(data["events"], list) else []
    return []

# ----------------------------
# Time helpers
# ----------------------------

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # '2026-02-06T19:45:00Z'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ----------------------------
# CLOB helpers
# ----------------------------

def init_clob() -> ClobClient:
    if not PM_PRIVATE_KEY or not PM_FUNDER:
        raise RuntimeError("Missing PM_PRIVATE_KEY or PM_FUNDER in env")

    # Per Polymarket docs: ClobClient(host, chain_id, key=..., signature_type=..., funder=...) :contentReference[oaicite:3]{index=3}
    client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PM_PRIVATE_KEY,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FUNDER,
    )

    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    log.info("[AUTH] derived API creds via create_or_derive_api_creds()")
    return client

def best_bid_ask(clob: ClobClient, token_id: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        ob = clob.get_order_book(token_id)
        # order book fields vary; normalize cautiously
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return best_bid, best_ask
    except Exception as e:
        log.warning(f"[CLOB] order book failed token={token_id}: {e}")
        return None, None

def place_usdc_order(
    clob: ClobClient,
    token_id: str,
    side: str,                 # BUY or SELL
    usdc_amount: float,        # used for BUY sizing; for SELL we treat as "size" in tokens if sell_tokens=True
    max_slippage: float,
    sell_tokens: bool = False  # if True: usdc_amount is token size to sell
) -> Optional[dict]:
    """
    BUY: size = usdc / limit_price
    SELL: size = token_size (sell_tokens=True)
    """
    bid, ask = best_bid_ask(clob, token_id)
    if bid is None and ask is None:
        return None

    if side == BUY:
        if ask is None:
            return None
        limit_price = min(0.99, ask * (1.0 + max_slippage))
        size = usdc_amount / max(limit_price, 1e-6)
    else:
        if bid is None:
            return None
        limit_price = max(0.01, bid * (1.0 - max_slippage))
        size = usdc_amount if sell_tokens else usdc_amount / max(limit_price, 1e-6)

    size = float(size)
    if size <= 0:
        return None

    if DRY_RUN:
        log.info(f"[DRY] {side} token={token_id} price={limit_price:.4f} size={size:.6f}")
        return {"dry_run": True, "side": side, "token_id": token_id, "price": limit_price, "size": size}

    # create_and_post_order exists in py-clob-client :contentReference[oaicite:4]{index=4}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = clob.create_and_post_order({
                "token_id": token_id,
                "side": side,
                "price": str(limit_price),
                "size": str(size),
            })
            return resp
        except Exception as e:
            log.warning(f"[ORDER] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(0.2 * attempt)
    return None

# ----------------------------
# Market resolution
# ----------------------------

@dataclass
class Market:
    asset: str
    slug: str
    condition_id: str
    yes_token: str
    no_token: str
    end: datetime

def looks_like_updown_15m_slug(asset: str, slug: str) -> bool:
    a = asset.lower()
    # ETH uses eth-updown-15m-..., BTC uses btc-updown-15m-..., etc.
    return slug.startswith(f"{a}-updown-15m-")

def resolve_active_15m_market(asset: str) -> Optional[Market]:
    # Pull recent events and extract slugs; then fetch market details by slug
    events = gamma_events_recent(limit=MAX_SLUG_LOOKUPS, offset=0)
    slugs: List[str] = []
    for ev in events:
        s = ev.get("slug")
        if isinstance(s, str) and looks_like_updown_15m_slug(asset, s):
            slugs.append(s)

    # If Gamma /events doesn't include these (rare), just bail; logs will show it.
    if not slugs:
        return None

    # Choose the earliest-ending market that is still open (endDate in future)
    candidates: List[Market] = []
    for slug in slugs[:MAX_SLUG_LOOKUPS]:
        m = gamma_market_by_slug(slug)
        if not m:
            continue

        if str(m.get("closed", "")).lower() == "true":
            continue
        if str(m.get("enableOrderBook", "")).lower() != "true":
            continue

        end_dt = parse_iso(m.get("endDate"))
        if not end_dt:
            continue
        if end_dt <= now_utc():
            continue

        toks = m.get("clobTokenIds") or []
        if not isinstance(toks, list) or len(toks) < 2:
            continue

        condition_id = m.get("conditionId") or ""
        if not isinstance(condition_id, str) or not condition_id.startswith("0x"):
            continue

        # Convention: clobTokenIds[0]=YES, [1]=NO (what you were printing earlier)
        yes_token = str(toks[0])
        no_token = str(toks[1])

        candidates.append(Market(
            asset=asset,
            slug=slug,
            condition_id=condition_id,
            yes_token=yes_token,
            no_token=no_token,
            end=end_dt
        ))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x.end)
    return candidates[0]

# ----------------------------
# Ladder state machine
# ----------------------------

@dataclass
class RunState:
    market: Market
    started_at: datetime
    last_eval_at: datetime
    eval_idx: int
    last_yes_ask: float
    last_no_ask: float
    inv_yes: float  # token units
    inv_no: float   # token units

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def main():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(f"LADDER: BASE_USDC={BASE_USDC:.2f} STEP_USDC={STEP_USDC:.2f} EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}")
    log.info(f"EXEC: MAX_SLIPPAGE={MAX_SLIPPAGE:.3f} MAX_RETRIES={MAX_RETRIES} MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}")
    log.info(f"DRY_RUN={DRY_RUN}")

    clob = init_clob()

    # rate limiter
    trade_timestamps: List[float] = []
    def can_trade() -> bool:
        nonlocal trade_timestamps
        now = time.time()
        trade_timestamps = [t for t in trade_timestamps if now - t < 60.0]
        return len(trade_timestamps) < MAX_TRADES_PER_MIN

    def mark_trade():
        trade_timestamps.append(time.time())

    states: Dict[str, Optional[RunState]] = {a: None for a in ASSETS}
    last_resolve = 0.0

    while True:
        t0 = time.time()

        # periodically resolve markets
        if (t0 - last_resolve) >= RESOLVE_EVERY_SEC:
            last_resolve = t0
            for asset in ASSETS:
                m = resolve_active_15m_market(asset)
                if not m:
                    log.info(f"[MARKET] {asset} no active 15m market found")
                    continue

                st = states.get(asset)
                if st is None or st.market.condition_id != m.condition_id:
                    # New market run: initialize and place initial buys
                    bid_y, ask_y = best_bid_ask(clob, m.yes_token)
                    bid_n, ask_n = best_bid_ask(clob, m.no_token)
                    if ask_y is None or ask_n is None:
                        log.info(f"[MARKET] {asset} slug={m.slug} missing asks yet; waiting")
                        continue

                    log.info(f"[MARKET] {asset} slug={m.slug} end={m.end.isoformat()} yes={m.yes_token} no={m.no_token}")

                    # Initial buy both sides
                    if can_trade():
                        resp1 = place_usdc_order(clob, m.yes_token, BUY, BASE_USDC, MAX_SLIPPAGE)
                        if resp1:
                            mark_trade()
                            # approximate token inventory added:
                            yes_tokens = BASE_USDC / ask_y
                        else:
                            yes_tokens = 0.0

                        time.sleep(COOLDOWN_SEC)

                    else:
                        yes_tokens = 0.0

                    if can_trade():
                        resp2 = place_usdc_order(clob, m.no_token, BUY, BASE_USDC, MAX_SLIPPAGE)
                        if resp2:
                            mark_trade()
                            no_tokens = BASE_USDC / ask_n
                        else:
                            no_tokens = 0.0

                        time.sleep(COOLDOWN_SEC)
                    else:
                        no_tokens = 0.0

                    states[asset] = RunState(
                        market=m,
                        started_at=now_utc(),
                        last_eval_at=now_utc(),
                        eval_idx=0,
                        last_yes_ask=float(ask_y),
                        last_no_ask=float(ask_n),
                        inv_yes=float(yes_tokens),
                        inv_no=float(no_tokens),
                    )
                    log.info(f"[INIT] {asset} bought both sides: yes_tokens~{yes_tokens:.6f} no_tokens~{no_tokens:.6f} (asks y={ask_y:.4f} n={ask_n:.4f})")

        # process each active run
        for asset in ASSETS:
            st = states.get(asset)
            if not st:
                continue

            elapsed = (now_utc() - st.started_at).total_seconds()

            # Exit
            if elapsed >= EXIT_AT_SEC:
                log.info(f"[EXIT] {asset} elapsed={elapsed:.1f}s selling inventory yes={st.inv_yes:.6f} no={st.inv_no:.6f}")

                # Sell YES
                if st.inv_yes > 0 and can_trade():
                    r = place_usdc_order(clob, st.market.yes_token, SELL, st.inv_yes, MAX_SLIPPAGE, sell_tokens=True)
                    if r:
                        mark_trade()
                        st.inv_yes = 0.0
                    time.sleep(COOLDOWN_SEC)

                # Sell NO
                if st.inv_no > 0 and can_trade():
                    r = place_usdc_order(clob, st.market.no_token, SELL, st.inv_no, MAX_SLIPPAGE, sell_tokens=True)
                    if r:
                        mark_trade()
                        st.inv_no = 0.0
                    time.sleep(COOLDOWN_SEC)

                states[asset] = None
                continue

            # Ladder evaluations
            if st.eval_idx < EVAL_COUNT:
                since_eval = (now_utc() - st.last_eval_at).total_seconds()
                if since_eval >= EVAL_INTERVAL_SEC:
                    bid_y, ask_y = best_bid_ask(clob, st.market.yes_token)
                    bid_n, ask_n = best_bid_ask(clob, st.market.no_token)
                    if ask_y is None or ask_n is None:
                        log.info(f"[EVAL] {asset} missing asks; waiting")
                        st.last_eval_at = now_utc()
                        continue

                    dy = float(ask_y) - st.last_yes_ask
                    dn = float(ask_n) - st.last_no_ask

                    # Choose side whose ask increased more (momentum proxy)
                    pick_yes = dy > dn
                    pick = "YES" if pick_yes else "NO"

                    log.info(
                        f"[EVAL] {asset} idx={st.eval_idx+1}/{EVAL_COUNT} "
                        f"ask_yes={ask_y:.4f} ask_no={ask_n:.4f} "
                        f"dy={dy:+.4f} dn={dn:+.4f} pick={pick}"
                    )

                    # Buy STEP_USDC on chosen side
                    if can_trade():
                        token = st.market.yes_token if pick_yes else st.market.no_token
                        r = place_usdc_order(clob, token, BUY, STEP_USDC, MAX_SLIPPAGE)
                        if r:
                            mark_trade()
                            added = STEP_USDC / float(ask_y if pick_yes else ask_n)
                            if pick_yes:
                                st.inv_yes += added
                            else:
                                st.inv_no += added
                            log.info(f"[BUY] {asset} +{STEP_USDC:.2f}USDC on {pick} tokens~{added:.6f}")
                        time.sleep(COOLDOWN_SEC)

                    # Optional: sell opposite side inventory (your “sells the opposite side” behavior)
                    if SELL_OPPOSITE:
                        if pick_yes and st.inv_no > 0 and can_trade():
                            r = place_usdc_order(clob, st.market.no_token, SELL, st.inv_no, MAX_SLIPPAGE, sell_tokens=True)
                            if r:
                                mark_trade()
                                log.info(f"[SELL] {asset} sold ALL NO inventory={st.inv_no:.6f}")
                                st.inv_no = 0.0
                            time.sleep(COOLDOWN_SEC)

                        if (not pick_yes) and st.inv_yes > 0 and can_trade():
                            r = place_usdc_order(clob, st.market.yes_token, SELL, st.inv_yes, MAX_SLIPPAGE, sell_tokens=True)
                            if r:
                                mark_trade()
                                log.info(f"[SELL] {asset} sold ALL YES inventory={st.inv_yes:.6f}")
                                st.inv_yes = 0.0
                            time.sleep(COOLDOWN_SEC)

                    # Advance eval
                    st.last_yes_ask = float(ask_y)
                    st.last_no_ask = float(ask_n)
                    st.last_eval_at = now_utc()
                    st.eval_idx += 1

        # Sleep
        dt = time.time() - t0
        time.sleep(max(0.05, POLL_SEC - dt))

if __name__ == "__main__":
    main()
