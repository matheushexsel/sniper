import os
import time
import json
import logging
import requests
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timezone

from py_clob_client.client import ClobClient
from py_clob_client.order_builder.constants import BUY, SELL

# ----------------------------
# Env helpers
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

LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("updown15m_ladder")

# ----------------------------
# Config
# ----------------------------

CLOB_HOST  = env_str("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = env_str("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID   = env_int("CHAIN_ID", 137)

PM_FUNDER        = env_str("PM_FUNDER")
PM_PRIVATE_KEY   = env_str("PM_PRIVATE_KEY")
PM_SIGNATURE_TYPE = env_int("PM_SIGNATURE_TYPE", 1)

ASSETS = parse_assets(env_str("ASSETS", '["ETH","BTC","SOL","XRP"]'))

POLL_SEC          = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)
MAX_SLUG_LOOKUPS  = env_int("MAX_SLUG_LOOKUPS", 50)

BASE_USDC         = env_float("BASE_USDC", 1.0)
STEP_USDC         = env_float("STEP_USDC", 1.0)
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120)
EVAL_COUNT        = env_int("EVAL_COUNT", 6)
EXIT_AT_SEC       = env_int("EXIT_AT_SEC", 780)

SELL_OPPOSITE     = env_bool("SELL_OPPOSITE", True)
DRY_RUN           = env_bool("DRY_RUN", False)

MAX_SLIPPAGE       = env_float("MAX_SLIPPAGE", 0.02)
MAX_RETRIES        = env_int("MAX_RETRIES", 3)
MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC       = env_float("COOLDOWN_SEC", 0.25)

# Deterministic slug scan window
# We’ll try current 15m bucket +/- N buckets (each bucket = 900s)
SLUG_SCAN_BUCKETS_BEHIND = env_int("SLUG_SCAN_BUCKETS_BEHIND", 6)   # 6*15m = 90m back
SLUG_SCAN_BUCKETS_AHEAD  = env_int("SLUG_SCAN_BUCKETS_AHEAD", 6)    # 90m forward

# ----------------------------
# HTTP (Gamma)
# ----------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "updown15m-ladder-bot/2.0"})

def http_get_json(url: str, params: Optional[dict] = None, timeout: float = 10.0) -> Optional[object]:
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        # show URL+params for immediate diagnosis
        full = r.url if "r" in locals() else url
        log.warning(f"[HTTP] GET failed: {full} ({e})")
        return None

def gamma_market_by_slug(slug: str) -> Optional[dict]:
    # Correct lookup: /markets?slug=<slug>
    url = f"{GAMMA_HOST}/markets"
    data = http_get_json(url, params={"slug": slug}, timeout=10.0)
    if not data:
        return None
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        if "markets" in data and isinstance(data["markets"], list):
            return data["markets"][0] if data["markets"] else None
        # Sometimes API returns a single market object
        if "slug" in data:
            return data
    return None

# ----------------------------
# Time helpers
# ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

def floor_to_15m_epoch(ts: int) -> int:
    return (ts // 900) * 900

# ----------------------------
# CLOB client
# ----------------------------

def init_clob() -> ClobClient:
    if not PM_PRIVATE_KEY or not PM_FUNDER:
        raise RuntimeError("Missing PM_PRIVATE_KEY or PM_FUNDER")

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
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return best_bid, best_ask
    except Exception as e:
        log.warning(f"[CLOB] order book failed token={token_id}: {e}")
        return None, None

def place_order(
    clob: ClobClient,
    token_id: str,
    side: str,
    usdc_amount: float,
    max_slippage: float,
    sell_tokens: bool = False
) -> Optional[dict]:
    bid, ask = best_bid_ask(clob, token_id)
    if bid is None and ask is None:
        return None

    if side == BUY:
        if ask is None:
            return None
        limit_price = min(0.99, ask * (1.0 + max_slippage))
        size = usdc_amount / max(limit_price, 1e-9)
    else:
        if bid is None:
            return None
        limit_price = max(0.01, bid * (1.0 - max_slippage))
        size = usdc_amount if sell_tokens else usdc_amount / max(limit_price, 1e-9)

    size = float(size)
    if size <= 0:
        return None

    if DRY_RUN:
        log.info(f"[DRY] {side} token={token_id} price={limit_price:.4f} size={size:.6f}")
        return {"dry_run": True, "side": side, "token_id": token_id, "price": limit_price, "size": size}

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
            time.sleep(0.25 * attempt)
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

def resolve_active_15m_market(asset: str) -> Optional[Market]:
    """
    Deterministic scan:
    - Build candidate slugs on 15m grid around now
    - Query /markets?slug=<slug>
    - Take the nearest future/open market with orderbook enabled
    """
    asset_l = asset.lower()
    now_ts = int(time.time())
    base = floor_to_15m_epoch(now_ts)

    candidates: List[Market] = []

    # Scan behind -> ahead so we pick the currently active one first if it exists
    for k in range(-SLUG_SCAN_BUCKETS_BEHIND, SLUG_SCAN_BUCKETS_AHEAD + 1):
        t = base + k * 900
        slug = f"{asset_l}-updown-15m-{t}"

        m = gamma_market_by_slug(slug)
        if not m:
            continue

        # Basic sanity checks
        closed = str(m.get("closed", "")).lower() == "true"
        eob = str(m.get("enableOrderBook", "")).lower() == "true"
        if closed or not eob:
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

        candidates.append(Market(
            asset=asset,
            slug=slug,
            condition_id=condition_id,
            yes_token=str(toks[0]),
            no_token=str(toks[1]),
            end=end_dt,
        ))

    if not candidates:
        return None

    # Choose the earliest-ending still-open market (closest in time)
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
    inv_yes: float
    inv_no: float

def main():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(f"LADDER: BASE_USDC={BASE_USDC:.2f} STEP_USDC={STEP_USDC:.2f} EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}")
    log.info(f"EXEC: MAX_SLIPPAGE={MAX_SLIPPAGE:.3f} MAX_RETRIES={MAX_RETRIES} MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}")
    log.info(f"DRY_RUN={DRY_RUN}")
    log.info(f"SLUG_SCAN: behind={SLUG_SCAN_BUCKETS_BEHIND} ahead={SLUG_SCAN_BUCKETS_AHEAD}")

    clob = init_clob()

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
        loop_start = time.time()

        # Resolve markets periodically
        if (loop_start - last_resolve) >= RESOLVE_EVERY_SEC:
            last_resolve = loop_start

            for asset in ASSETS:
                m = resolve_active_15m_market(asset)
                if not m:
                    log.info(f"[MARKET] {asset} no active 15m market found")
                    continue

                st = states.get(asset)
                if st is None or st.market.condition_id != m.condition_id:
                    by, ay = best_bid_ask(clob, m.yes_token)
                    bn, an = best_bid_ask(clob, m.no_token)
                    if ay is None or an is None:
                        log.info(f"[MARKET] {asset} slug={m.slug} asks not ready yet; waiting")
                        continue

                    log.info(f"[MARKET] {asset} slug={m.slug} end={m.end.isoformat()} yes={m.yes_token} no={m.no_token}")

                    yes_tokens = 0.0
                    no_tokens = 0.0

                    # Initial buy both sides
                    if can_trade():
                        r1 = place_order(clob, m.yes_token, BUY, BASE_USDC, MAX_SLIPPAGE)
                        if r1:
                            mark_trade()
                            yes_tokens = BASE_USDC / float(ay)
                        time.sleep(COOLDOWN_SEC)

                    if can_trade():
                        r2 = place_order(clob, m.no_token, BUY, BASE_USDC, MAX_SLIPPAGE)
                        if r2:
                            mark_trade()
                            no_tokens = BASE_USDC / float(an)
                        time.sleep(COOLDOWN_SEC)

                    states[asset] = RunState(
                        market=m,
                        started_at=now_utc(),
                        last_eval_at=now_utc(),
                        eval_idx=0,
                        last_yes_ask=float(ay),
                        last_no_ask=float(an),
                        inv_yes=float(yes_tokens),
                        inv_no=float(no_tokens),
                    )

                    log.info(f"[INIT] {asset} bought both: yes~{yes_tokens:.6f} no~{no_tokens:.6f} (asks y={ay:.4f} n={an:.4f})")

        # Run ladder logic
        for asset in ASSETS:
            st = states.get(asset)
            if not st:
                continue

            elapsed = (now_utc() - st.started_at).total_seconds()

            # Exit at 13 minutes
            if elapsed >= EXIT_AT_SEC:
                log.info(f"[EXIT] {asset} selling inventory yes={st.inv_yes:.6f} no={st.inv_no:.6f}")

                if st.inv_yes > 0 and can_trade():
                    r = place_order(clob, st.market.yes_token, SELL, st.inv_yes, MAX_SLIPPAGE, sell_tokens=True)
                    if r:
                        mark_trade()
                        st.inv_yes = 0.0
                    time.sleep(COOLDOWN_SEC)

                if st.inv_no > 0 and can_trade():
                    r = place_order(clob, st.market.no_token, SELL, st.inv_no, MAX_SLIPPAGE, sell_tokens=True)
                    if r:
                        mark_trade()
                        st.inv_no = 0.0
                    time.sleep(COOLDOWN_SEC)

                states[asset] = None
                continue

            # Evaluations every 2 minutes, 6 times
            if st.eval_idx < EVAL_COUNT:
                since_eval = (now_utc() - st.last_eval_at).total_seconds()
                if since_eval >= EVAL_INTERVAL_SEC:
                    by, ay = best_bid_ask(clob, st.market.yes_token)
                    bn, an = best_bid_ask(clob, st.market.no_token)
                    if ay is None or an is None:
                        log.info(f"[EVAL] {asset} asks missing; waiting")
                        st.last_eval_at = now_utc()
                        continue

                    dy = float(ay) - st.last_yes_ask
                    dn = float(an) - st.last_no_ask
                    pick_yes = dy > dn
                    pick = "YES" if pick_yes else "NO"

                    log.info(
                        f"[EVAL] {asset} {st.eval_idx+1}/{EVAL_COUNT} "
                        f"ask_yes={ay:.4f} ask_no={an:.4f} dy={dy:+.4f} dn={dn:+.4f} pick={pick}"
                    )

                    # Buy STEP_USDC on chosen side
                    if can_trade():
                        token = st.market.yes_token if pick_yes else st.market.no_token
                        r = place_order(clob, token, BUY, STEP_USDC, MAX_SLIPPAGE)
                        if r:
                            mark_trade()
                            added = STEP_USDC / float(ay if pick_yes else an)
                            if pick_yes:
                                st.inv_yes += added
                            else:
                                st.inv_no += added
                            log.info(f"[BUY] {asset} +{STEP_USDC:.2f}USDC on {pick} tokens~{added:.6f}")
                        time.sleep(COOLDOWN_SEC)

                    # Optional: sell the opposite side immediately
                    if SELL_OPPOSITE:
                        if pick_yes and st.inv_no > 0 and can_trade():
                            r = place_order(clob, st.market.no_token, SELL, st.inv_no, MAX_SLIPPAGE, sell_tokens=True)
                            if r:
                                mark_trade()
                                log.info(f"[SELL] {asset} sold ALL NO inv={st.inv_no:.6f}")
                                st.inv_no = 0.0
                            time.sleep(COOLDOWN_SEC)

                        if (not pick_yes) and st.inv_yes > 0 and can_trade():
                            r = place_order(clob, st.market.yes_token, SELL, st.inv_yes, MAX_SLIPPAGE, sell_tokens=True)
                            if r:
                                mark_trade()
                                log.info(f"[SELL] {asset} sold ALL YES inv={st.inv_yes:.6f}")
                                st.inv_yes = 0.0
                            time.sleep(COOLDOWN_SEC)

                    # Advance
                    st.last_yes_ask = float(ay)
                    st.last_no_ask = float(an)
                    st.last_eval_at = now_utc()
                    st.eval_idx += 1

        # sleep
        dt = time.time() - loop_start
        time.sleep(max(0.05, POLL_SEC - dt))

if __name__ == "__main__":
    main()
