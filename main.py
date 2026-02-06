import os
import time
import json
import logging
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, ApiCreds


# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("updown15m-ladder")


# -------------------------
# Env helpers
# -------------------------
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

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


# -------------------------
# Config
# -------------------------
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")
CHAIN_ID = env_int("CHAIN_ID", 137)

ASSETS = [a.upper() for a in env_list("ASSETS", ["ETH", "BTC", "SOL", "XRP"])]

POLL_SEC = env_float("POLL_SEC", 2.0)
RESOLVE_EVERY_SEC = env_float("RESOLVE_EVERY_SEC", 15.0)

# Ladder strategy
EVAL_INTERVAL_SEC = env_int("EVAL_INTERVAL_SEC", 120)  # every 2 minutes
EVAL_COUNT = env_int("EVAL_COUNT", 6)                  # 6 times => 12 minutes
EXIT_AT_SEC = env_int("EXIT_AT_SEC", 780)              # sell at t=13m
WINDOW_SEC = env_int("WINDOW_SEC", 900)                # 15m slug lifetime
CLOSE_HARD_SEC = env_int("CLOSE_HARD_SEC", 60)         # if <=60s left, force exit

BASE_USDC = env_float("BASE_USDC", 1.0)                # buy both sides at start
STEP_USDC = env_float("STEP_USDC", 1.0)                # add to winner each eval

# Execution controls
MAX_SLIPPAGE = env_float("MAX_SLIPPAGE", 0.02)
FILL_TIMEOUT_SEC = env_float("FILL_TIMEOUT_SEC", 2.0)
MAX_RETRIES = env_int("MAX_RETRIES", 3)

MAX_TRADES_PER_MIN = env_int("MAX_TRADES_PER_MIN", 20)
COOLDOWN_SEC = env_float("COOLDOWN_SEC", 0.25)

# Market selection
MAX_SLUG_LOOKUPS = env_int("MAX_SLUG_LOOKUPS", 40)
MIN_TTE_SELECT_SEC = env_int("MIN_TTE_SELECT_SEC", 15)
MAX_TTE_SELECT_SEC = env_int("MAX_TTE_SELECT_SEC", 1200)  # pick markets ending within 20 minutes

# Credentials
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY") or os.getenv("PRIVATE_KEY")
PM_FUNDER = os.getenv("PM_FUNDER") or os.getenv("PM_FOUNDER")

PM_API_KEY = os.getenv("PM_API_KEY")
PM_API_SECRET = os.getenv("PM_API_SECRET")
PM_API_PASSPHRASE = os.getenv("PM_API_PASSPHRASE")

if not PM_PRIVATE_KEY:
    raise SystemExit("Missing PM_PRIVATE_KEY")
if not PM_FUNDER:
    raise SystemExit("Missing PM_FUNDER (wallet address)")

SLUG_PREFIX = {
    "ETH": "eth-updown-15m-",
    "BTC": "btc-updown-15m-",
    "SOL": "sol-updown-15m-",
    "XRP": "xrp-updown-15m-",
}


# -------------------------
# Rate limiter
# -------------------------
class TradeLimiter:
    def __init__(self):
        self.window_start = time.time()
        self.count = 0
        self.last_trade_ts = 0.0

    def allow(self) -> bool:
        now = time.time()
        if now - self.window_start >= 60:
            self.window_start = now
            self.count = 0
        if self.count >= MAX_TRADES_PER_MIN:
            return False
        if now - self.last_trade_ts < COOLDOWN_SEC:
            return False
        self.count += 1
        self.last_trade_ts = now
        return True

limiter = TradeLimiter()


# -------------------------
# Gamma API
# -------------------------
sess = requests.Session()
sess.headers.update({"User-Agent": "updown15m-ladder/2.1"})

def gamma_get(path: str, params: Optional[dict] = None) -> Optional[Any]:
    url = f"{GAMMA_HOST}{path}"
    try:
        r = sess.get(url, params=params, timeout=12)
        if r.status_code == 429:
            # Gamma rate limit; back off a bit
            log.warning(f"[GAMMA] 429 rate-limited: {url}")
            return None
        if r.status_code != 200:
            log.warning(f"[GAMMA] {r.status_code} {url} {r.text[:200]}")
            return None
        return r.json()
    except Exception as e:
        log.warning(f"[GAMMA] error {url}: {e}")
        return None

def gamma_markets(limit: int = 200, offset: int = 0) -> List[dict]:
    """
    Primary discovery. Gamma is much more consistent listing these markets under /markets.
    We try a few param shapes because Gamma occasionally changes.
    """
    param_variants = [
        {"limit": limit, "offset": offset, "active": True},
        {"limit": limit, "offset": offset, "closed": False},
        {"limit": limit, "offset": offset},
    ]
    for params in param_variants:
        data = gamma_get("/markets", params=params)
        if isinstance(data, dict) and isinstance(data.get("markets"), list):
            return data["markets"]
        if isinstance(data, list):
            return data
    return []

def gamma_market_by_slug(slug: str) -> Optional[dict]:
    # Try /markets/{slug} first
    data = gamma_get(f"/markets/{slug}")
    if isinstance(data, dict) and data.get("slug"):
        return data

    # Fallback: /markets?slug=...
    data = gamma_get("/markets", params={"slug": slug})
    if isinstance(data, dict) and isinstance(data.get("markets"), list) and data["markets"]:
        return data["markets"][0]
    if isinstance(data, list) and data:
        return data[0]
    return None

def gamma_events_fallback(asset: str, limit: int = 50) -> List[dict]:
    """
    Last resort. Sometimes markets still appear in /events search. Keep as fallback only.
    """
    for params in (
        {"limit": limit, "offset": 0, "search": asset},
        {"limit": limit, "offset": 0, "q": asset},
        {"limit": limit, "offset": 0},
    ):
        data = gamma_get("/events", params=params)
        if isinstance(data, dict) and isinstance(data.get("events"), list):
            return data["events"]
        if isinstance(data, list):
            return data
    return []


# -------------------------
# py-clob-client adaptive init
# -------------------------
def init_clob_client() -> ClobClient:
    sig = inspect.signature(ClobClient.__init__)
    ctor_params = set(sig.parameters.keys())

    kwargs = {}
    if "chain_id" in ctor_params:
        kwargs["chain_id"] = CHAIN_ID
    elif "chainId" in ctor_params:
        kwargs["chainId"] = CHAIN_ID

    if "funder" in ctor_params:
        kwargs["funder"] = PM_FUNDER
    elif "address" in ctor_params:
        kwargs["address"] = PM_FUNDER

    for key_name in ("private_key", "key", "signer", "wallet_private_key"):
        if key_name in ctor_params:
            kwargs[key_name] = PM_PRIVATE_KEY
            break

    if PM_API_KEY and PM_API_SECRET and PM_API_PASSPHRASE:
        for name in ("api_creds", "apiCreds", "creds"):
            if name in ctor_params:
                kwargs[name] = ApiCreds(
                    api_key=PM_API_KEY,
                    api_secret=PM_API_SECRET,
                    api_passphrase=PM_API_PASSPHRASE,
                )
                break

    try:
        client = ClobClient(CLOB_HOST, **kwargs)
    except TypeError:
        if "host" in ctor_params:
            kwargs["host"] = CLOB_HOST
            client = ClobClient(**kwargs)
        else:
            client = ClobClient(CLOB_HOST)

    # Attach wallet/signer if ctor didn’t accept it
    for m in ("set_private_key", "set_signer", "set_wallet", "set_key"):
        fn = getattr(client, m, None)
        if callable(fn):
            try:
                fn(PM_PRIVATE_KEY, PM_FUNDER)
            except TypeError:
                try:
                    fn(PM_PRIVATE_KEY)
                except Exception:
                    pass
            break

    # Attach api creds if provided
    if PM_API_KEY and PM_API_SECRET and PM_API_PASSPHRASE:
        creds = ApiCreds(
            api_key=PM_API_KEY,
            api_secret=PM_API_SECRET,
            api_passphrase=PM_API_PASSPHRASE,
        )
        for m in ("set_api_creds", "set_api_credentials", "set_creds"):
            fn = getattr(client, m, None)
            if callable(fn):
                try:
                    fn(creds)
                except Exception:
                    pass
                break

    return client


# -------------------------
# Order book helpers
# -------------------------
def get_order_book(client: ClobClient, token_id: str) -> Optional[dict]:
    for m in ("get_order_book", "get_orderbook", "get_book"):
        fn = getattr(client, m, None)
        if callable(fn):
            try:
                return fn(token_id)
            except Exception:
                continue
    return None

def best_prices(book: dict) -> Tuple[Optional[float], Optional[float]]:
    if not book:
        return None, None
    bids = book.get("bids") or book.get("buy") or []
    asks = book.get("asks") or book.get("sell") or []

    def parse_level(x):
        if isinstance(x, dict):
            p = x.get("price") or x.get("p")
        elif isinstance(x, (list, tuple)) and len(x) >= 1:
            p = x[0]
        else:
            return None
        try:
            return float(p)
        except Exception:
            return None

    best_bid = parse_level(bids[0]) if bids else None
    best_ask = parse_level(asks[0]) if asks else None
    return best_bid, best_ask


# -------------------------
# Trading wrappers
# -------------------------
def create_signed_order(client: ClobClient, args: OrderArgs) -> Any:
    for m in ("create_order", "build_order", "create_signed_order"):
        fn = getattr(client, m, None)
        if callable(fn):
            try:
                return fn(args)
            except TypeError:
                try:
                    return fn(**args.__dict__)
                except Exception:
                    pass
            except Exception:
                pass
    raise RuntimeError("No supported order creation method found on ClobClient")

def post_order(client: ClobClient, signed_order: Any) -> Any:
    for m in ("post_order", "place_order", "submit_order"):
        fn = getattr(client, m, None)
        if callable(fn):
            return fn(signed_order)
    raise RuntimeError("No supported order posting method found on ClobClient")

def place_limit(client: ClobClient, token_id: str, side: str, price: float, size: float) -> Optional[str]:
    if not limiter.allow():
        log.info("[RATE] blocked by limiter")
        return None

    price = max(0.001, min(0.999, price))
    size = max(0.0001, size)

    try:
        args = OrderArgs(token_id=token_id, price=price, size=size, side=side)
    except TypeError:
        args = OrderArgs(tokenId=token_id, price=price, size=size, side=side)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            signed = create_signed_order(client, args)
            res = post_order(client, signed)
            oid = None
            if isinstance(res, dict):
                oid = res.get("orderID") or res.get("orderId") or res.get("id")
            log.info(f"[ORDER] {side.upper()} token={token_id} px={price:.4f} sz={size:.4f} attempt={attempt} id={oid}")
            return oid or "ok"
        except Exception as e:
            log.warning(f"[ORDER] failed attempt={attempt} side={side} token={token_id}: {e}")
            time.sleep(0.25 * attempt)

    return None


# -------------------------
# Market selection
# -------------------------
@dataclass
class UpDownMarket:
    asset: str
    slug: str
    end: datetime
    yes_token: str
    no_token: str

def select_active_15m_market(asset: str) -> Optional[UpDownMarket]:
    """
    AIRTIGHT discovery:
      1) list /markets (limit=200)
      2) filter by slug prefix (eth-updown-15m-, etc)
      3) require: closed != True, enableOrderBook == True, endDate present, clobTokenIds length >= 2
      4) choose nearest endDate within [MIN_TTE_SELECT_SEC, MAX_TTE_SELECT_SEC]
    """
    prefix = SLUG_PREFIX.get(asset)
    if not prefix:
        return None

    now = utc_now()
    markets = gamma_markets(limit=200, offset=0)

    if not markets:
        log.warning(f"[DISCOVERY] {asset} /markets returned 0 items, falling back to /events")
        # Fallback: events -> try to map slugs into markets
        evs = gamma_events_fallback(asset, limit=80)
        slugs = [e.get("slug") for e in evs if isinstance(e, dict) and e.get("slug")]
        markets = []
        for s in slugs[:MAX_SLUG_LOOKUPS]:
            m = gamma_market_by_slug(str(s))
            if m:
                markets.append(m)

    if not markets:
        return None

    candidates: List[Tuple[float, dict]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        slug = (m.get("slug") or "").strip()
        if not slug.startswith(prefix):
            continue

        if m.get("closed") is True:
            continue
        if m.get("enableOrderBook") is False:
            continue

        end = parse_iso(m.get("endDate"))
        if not end:
            continue

        tte = (end - now).total_seconds()
        if tte < MIN_TTE_SELECT_SEC or tte > MAX_TTE_SELECT_SEC:
            continue

        clob_ids = m.get("clobTokenIds") or []
        if not (isinstance(clob_ids, list) and len(clob_ids) >= 2):
            continue

        candidates.append((tte, m))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    best = candidates[0][1]
    end = parse_iso(best.get("endDate"))
    clob_ids = best.get("clobTokenIds") or []
    yes_token = str(clob_ids[0])
    no_token = str(clob_ids[1])

    return UpDownMarket(asset=asset, slug=str(best.get("slug")), end=end, yes_token=yes_token, no_token=no_token)


# -------------------------
# Ladder state
# -------------------------
@dataclass
class MarketRunState:
    market: UpDownMarket
    start_ts: float
    eval_done: int
    opened: bool
    last_eval_ts: float
    bias_side: Optional[str]  # "YES" or "NO"

def usdc_to_shares(usdc: float, price: float) -> float:
    return 0.0 if price <= 0 else usdc / price

def mid_price(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if best_bid is None and best_ask is None:
        return None
    if best_bid is None:
        return best_ask
    if best_ask is None:
        return best_bid
    return (best_bid + best_ask) / 2.0

def choose_bias(yes_mid: float, no_mid: float) -> str:
    return "YES" if yes_mid >= no_mid else "NO"

def should_exit(elapsed: float, tte: float) -> bool:
    if elapsed >= EXIT_AT_SEC:
        return True
    if tte <= CLOSE_HARD_SEC:
        return True
    return False


# -------------------------
# Main loop
# -------------------------
def print_banner():
    log.info("=== UPDOWN 15M LADDER BOT START ===")
    log.info(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST}")
    log.info(f"ASSETS={ASSETS}")
    log.info(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}")
    log.info(f"LADDER: BASE_USDC={BASE_USDC} STEP_USDC={STEP_USDC} EVAL_INTERVAL_SEC={EVAL_INTERVAL_SEC} EVAL_COUNT={EVAL_COUNT} EXIT_AT_SEC={EXIT_AT_SEC}")
    log.info(f"EXEC: MAX_SLIPPAGE={MAX_SLIPPAGE} MAX_RETRIES={MAX_RETRIES} MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}")

def main():
    clob = init_clob_client()
    print_banner()

    runs: Dict[str, Optional[MarketRunState]] = {a: None for a in ASSETS}
    last_resolve_ts = 0.0

    while True:
        now = utc_now()
        now_ts = time.time()

        if now_ts - last_resolve_ts >= RESOLVE_EVERY_SEC:
            last_resolve_ts = now_ts
            for asset in ASSETS:
                st = runs.get(asset)
                if st is None or (st.market.end <= now):
                    m = select_active_15m_market(asset)
                    if m:
                        tte = (m.end - now).total_seconds()
                        runs[asset] = MarketRunState(
                            market=m,
                            start_ts=now_ts,
                            eval_done=0,
                            opened=False,
                            last_eval_ts=now_ts,
                            bias_side=None,
                        )
                        log.info(f"[MARKET] {asset} slug={m.slug} tte={tte:.1f}s yes={m.yes_token} no={m.no_token}")
                    else:
                        log.info(f"[MARKET] {asset} no active 15m market found")

        for asset, st in list(runs.items()):
            if st is None:
                continue

            tty = (st.market.end - utc_now()).total_seconds()
            elapsed = time.time() - st.start_ts

            ybook = get_order_book(clob, st.market.yes_token)
            nbook = get_order_book(clob, st.market.no_token)
            ybid, yask = best_prices(ybook or {})
            nbid, nask = best_prices(nbook or {})

            if yask is None or nask is None:
                log.info(f"[WAIT] {asset} missing asks yask={yask} nask={nask}")
                continue

            ymid = mid_price(ybid, yask) or yask
            nmid = mid_price(nbid, nask) or nask

            if not st.opened:
                y_sh = usdc_to_shares(BASE_USDC, yask)
                n_sh = usdc_to_shares(BASE_USDC, nask)

                place_limit(clob, st.market.yes_token, "buy", yask, y_sh)
                place_limit(clob, st.market.no_token, "buy", nask, n_sh)

                st.opened = True
                st.last_eval_ts = time.time()
                st.bias_side = choose_bias(ymid, nmid)
                log.info(f"[OPEN] {asset} yask={yask:.4f} nask={nask:.4f} bias={st.bias_side}")

            if st.opened and st.eval_done < EVAL_COUNT:
                if time.time() - st.last_eval_ts >= EVAL_INTERVAL_SEC:
                    st.last_eval_ts = time.time()
                    st.eval_done += 1
                    bias = choose_bias(ymid, nmid)
                    st.bias_side = bias

                    if bias == "YES":
                        sh = usdc_to_shares(STEP_USDC, yask)
                        place_limit(clob, st.market.yes_token, "buy", yask, sh)
                        log.info(f"[ADD] {asset} eval={st.eval_done}/{EVAL_COUNT} bias=YES yask={yask:.4f} nmid={nmid:.4f}")
                    else:
                        sh = usdc_to_shares(STEP_USDC, nask)
                        place_limit(clob, st.market.no_token, "buy", nask, sh)
                        log.info(f"[ADD] {asset} eval={st.eval_done}/{EVAL_COUNT} bias=NO  nask={nask:.4f} ymid={ymid:.4f}")

            if should_exit(elapsed, tty):
                sell_y_px = ybid if ybid is not None else 0.01
                sell_n_px = nbid if nbid is not None else 0.01

                # best-effort close based on what we bought
                approx_yes = usdc_to_shares(BASE_USDC, max(yask, 0.01)) + st.eval_done * usdc_to_shares(STEP_USDC, max(yask, 0.01))
                approx_no  = usdc_to_shares(BASE_USDC, max(nask, 0.01)) + st.eval_done * usdc_to_shares(STEP_USDC, max(nask, 0.01))

                place_limit(clob, st.market.yes_token, "sell", sell_y_px, approx_yes)
                place_limit(clob, st.market.no_token, "sell", sell_n_px, approx_no)

                log.info(f"[EXIT] {asset} elapsed={elapsed:.1f}s tte={tty:.1f}s sell_y={sell_y_px:.4f} sell_n={sell_n_px:.4f}")
                runs[asset] = None

            else:
                if int(time.time()) % 10 == 0:
                    log.info(f"[PRICES] {asset} ybid={ybid} yask={yask} nbid={nbid} nask={nask} ymid={ymid:.4f} nmid={nmid:.4f} eval={st.eval_done}/{EVAL_COUNT} tte={tty:.1f}s")

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
