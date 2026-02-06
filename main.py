import os
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

# =========================
# ENV (reusing your names)
# =========================
PM_FUNDER = os.getenv("PM_FUNDER", "").strip()
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY", "").strip()

CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip()
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))
PM_SIGNATURE_TYPE = int(os.getenv("PM_SIGNATURE_TYPE", "0"))

POLL_SEC = float(os.getenv("POLL_SEC", "0.50"))
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "50"))

MIN_USDC = float(os.getenv("MIN_USDC", "5"))
MAX_USDC = float(os.getenv("MAX_USDC", "50"))
SIZE_MULT = float(os.getenv("SIZE_MULT", "1.0"))

MAX_TRADES_PER_MIN = int(os.getenv("MAX_TRADES_PER_MIN", "6"))
COOLDOWN_SEC = float(os.getenv("COOLDOWN_SEC", "2.0"))

ARM_BEFORE_BOUNDARY_SEC = float(os.getenv("ARM_BEFORE_BOUNDARY_SEC", "999999"))  # default: always armed
CLOSE_BEFORE_BOUNDARY_SEC = float(os.getenv("CLOSE_BEFORE_BOUNDARY_SEC", "90"))
STOP_TRYING_BEFORE_BOUNDARY_SEC = float(os.getenv("STOP_TRYING_BEFORE_BOUNDARY_SEC", "30"))

# =========================
# New env vars
# =========================
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
DATA_API_BASE = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").strip()

ASSETS = os.getenv("ASSETS", "ETH,BTC,SOL,XRP").strip()
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))
MAX_SLIPPAGE = float(os.getenv("MAX_SLIPPAGE", "0.02"))
RESOLVE_EVERY_SEC = float(os.getenv("RESOLVE_EVERY_SEC", "10"))

# Debug knobs
LOG_EVERY_SEC = float(os.getenv("LOG_EVERY_SEC", "15"))
DEBUG = os.getenv("DEBUG", "1").strip() == "1"

# Resolver knobs
MAX_SLUG_LOOKUPS = int(os.getenv("MAX_SLUG_LOOKUPS", "12"))  # how many slugs per asset we expand via /markets?slug=
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))


# =========================
# Models
# =========================
@dataclass
class Level:
    price: float
    size: float


@dataclass
class ResolvedMarket:
    asset: str
    slug: str
    condition_id: str
    end_dt: datetime
    yes_token: str
    no_token: str
    question: str


# =========================
# Time helpers
# =========================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def seconds_to_end(end_dt: datetime) -> float:
    return (end_dt - now_utc()).total_seconds()


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =========================
# Rate limit helper
# =========================
def rate_limit_ok(trades_ts: List[float]) -> bool:
    cutoff = time.time() - 60.0
    while trades_ts and trades_ts[0] < cutoff:
        trades_ts.pop(0)
    return len(trades_ts) < MAX_TRADES_PER_MIN


# =========================
# Asset naming / matching
# =========================
def asset_query_name(asset: str) -> str:
    mapping = {
        "ETH": "Ethereum Up or Down",
        "BTC": "Bitcoin Up or Down",
        "SOL": "Solana Up or Down",
        "XRP": "XRP Up or Down",
    }
    return mapping.get(asset.upper(), f"{asset.upper()} Up or Down")


def slug_family_prefix(asset: str) -> str:
    # Based on what you showed: btc-updown-15m-..., eth-updown-15m-...
    return f"{asset.lower()}-updown-15m-"


def is_candidate_slug(asset: str, slug: str) -> bool:
    s = (slug or "").lower()
    # Must be the 15m family
    if "updown-15m" not in s:
        return False

    a = asset.upper()
    if a == "BTC":
        return s.startswith("btc-updown-15m-") or s.startswith("bitcoin-updown-15m-")
    if a == "ETH":
        return s.startswith("eth-updown-15m-") or s.startswith("ethereum-updown-15m-")
    if a == "SOL":
        return s.startswith("sol-updown-15m-") or s.startswith("solana-updown-15m-")
    if a == "XRP":
        return s.startswith("xrp-updown-15m-")

    # fallback
    return s.startswith(slug_family_prefix(asset))


def extract_end_dt(market_obj: dict) -> Optional[datetime]:
    # Gamma fields vary across endpoints/versions; handle common ones
    for k in ("endDate", "endDateIso", "end_date", "end_date_iso"):
        v = market_obj.get(k)
        if isinstance(v, str) and v.strip():
            try:
                return parse_iso(v.strip())
            except Exception:
                pass
    return None


def extract_enable_orderbook(market_obj: dict) -> bool:
    # Some responses use enableOrderBook, some enable_order_book
    v = market_obj.get("enableOrderBook")
    if isinstance(v, bool):
        return v
    v2 = market_obj.get("enable_order_book")
    if isinstance(v2, bool):
        return v2
    # If missing, treat as False (we’ll only trade if explicitly enabled)
    return False


def extract_condition_id(market_obj: dict) -> str:
    for k in ("conditionId", "condition_id"):
        v = market_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def extract_clob_tokens(market_obj: dict) -> Optional[Tuple[str, str]]:
    tokens = market_obj.get("clobTokenIds") or market_obj.get("clob_token_ids")
    if isinstance(tokens, list) and len(tokens) == 2:
        return str(tokens[0]).strip(), str(tokens[1]).strip()
    return None


# =========================
# HTTP calls
# =========================
async def http_get_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> dict:
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        return await r.json()


async def gamma_public_search(session: aiohttp.ClientSession, asset: str) -> dict:
    # IMPORTANT: do NOT set optimized=true here, because you are getting incomplete market fields.
    params = {
        "q": asset_query_name(asset),
        "events_status": "active",
        "limit_per_type": FETCH_LIMIT,
        "sort": "endDate",
        "ascending": "true",
        # no optimized flag
    }
    return await http_get_json(session, f"{GAMMA_HOST}/public-search", params=params)


async def gamma_market_by_slug(session: aiohttp.ClientSession, slug: str) -> Optional[dict]:
    # Gamma markets endpoint: /markets?slug=...
    # Some deployments return a list; some return a dict with "markets"
    data = await http_get_json(session, f"{GAMMA_HOST}/markets", params={"slug": slug})
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        mkts = data.get("markets")
        if isinstance(mkts, list) and mkts:
            return mkts[0]
        # sometimes the market object is directly returned
        if "slug" in data:
            return data
    return None


async def clob_fetch_book(session: aiohttp.ClientSession, token_id: str) -> Tuple[List[Level], List[Level]]:
    data = await http_get_json(session, f"{CLOB_HOST}/book", params={"token_id": token_id})
    bids = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("bids", [])], key=lambda x: x.price, reverse=True)
    asks = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("asks", [])], key=lambda x: x.price)
    return bids, asks


async def dataapi_positions(session: aiohttp.ClientSession, user: str, condition_id: str) -> List[dict]:
    params = {"user": user, "market": condition_id, "sizeThreshold": 0}
    data = await http_get_json(session, f"{DATA_API_BASE}/positions", params=params)
    return data if isinstance(data, list) else []


# =========================
# Inventory parsing
# =========================
def parse_inventory_updown(positions: List[dict]) -> Tuple[float, float]:
    up = 0.0
    down = 0.0
    for p in positions or []:
        outcome = (p.get("outcome") or "").strip().lower()
        size = float(p.get("size", 0) or 0)
        if size <= 0:
            continue
        if outcome == "up":
            up += size
        elif outcome == "down":
            down += size
    return up, down


# =========================
# Strategy math
# =========================
def tob(bids: List[Level], asks: List[Level]) -> Tuple[Optional[float], Optional[float]]:
    bid = bids[0].price if bids else None
    ask = asks[0].price if asks else None
    return bid, ask


def max_buy_shares(asks: List[Level], price_cap: float, budget: float) -> float:
    shares = 0.0
    spent = 0.0
    for lvl in asks:
        if lvl.price > price_cap:
            break
        if lvl.size <= 0:
            continue
        rem = budget - spent
        if rem <= 0:
            break
        take = min(lvl.size, rem / lvl.price)
        if take <= 0:
            break
        shares += take
        spent += take * lvl.price
    return shares


def max_sell_shares(bids: List[Level], price_floor: float, cap_shares: float) -> float:
    shares = 0.0
    for lvl in bids:
        if lvl.price < price_floor:
            break
        if lvl.size <= 0:
            continue
        take = min(lvl.size, cap_shares - shares)
        if take <= 0:
            break
        shares += take
        if shares >= cap_shares:
            break
    return shares


# =========================
# Trading client
# =========================
def make_client() -> ClobClient:
    if not PM_FUNDER or not PM_PRIVATE_KEY:
        raise RuntimeError("Missing PM_FUNDER or PM_PRIVATE_KEY.")
    c = ClobClient(
        CLOB_HOST,
        key=PM_PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FUNDER,
    )
    c.set_api_creds(c.create_or_derive_api_creds())
    return c


def place_fok(client: ClobClient, token_id: str, side: str, price: float, size: float) -> dict:
    args = OrderArgs(
        token_id=token_id,
        price=round(price, 4),
        size=round(size, 4),
        side=side,
    )
    signed = client.create_order(args)
    return client.post_order(signed, OrderType.FOK)


# =========================
# Resolver (fixed)
# =========================
async def gamma_resolve_current_15m(session: aiohttp.ClientSession, asset: str) -> Optional[ResolvedMarket]:
    """
    Two-step resolver:
      1) public-search to get candidate slugs
      2) expand a small number of updown-15m slugs via /markets?slug=... to get full fields
    Then pick the soonest-ending valid market.
    """
    data = await gamma_public_search(session, asset)

    # candidates only from nested events[].markets (your logs show top_markets=0)
    candidates = []
    for ev in (data.get("events") or []):
        for m in (ev.get("markets") or []):
            slug = (m.get("slug") or "").strip()
            if slug:
                candidates.append(slug)

    # filter to the 15m family
    slugs = [s for s in candidates if is_candidate_slug(asset, s)]

    if DEBUG:
        print(f"[RESOLVE] {asset}: events={len(data.get('events') or [])} market_slugs={len(candidates)} updown15m_slugs={len(slugs)}", flush=True)
        if slugs[:5]:
            print(f"  [SLUGS] {asset} sample={slugs[:5]}", flush=True)

    if not slugs:
        return None

    # Expand only the first N slugs (already sorted by endDate order in search, typically)
    slugs = slugs[:MAX_SLUG_LOOKUPS]

    # Fetch full market details concurrently
    tasks = [asyncio.create_task(gamma_market_by_slug(session, s)) for s in slugs]
    full = await asyncio.gather(*tasks, return_exceptions=True)

    now = now_utc()
    best: Optional[ResolvedMarket] = None

    for idx, mk in enumerate(full):
        if isinstance(mk, Exception) or mk is None:
            continue

        slug = (mk.get("slug") or slugs[idx]).strip()
        question = (mk.get("question") or "").strip()

        # Must be open + orderbook enabled
        if mk.get("closed", False):
            continue
        if not extract_enable_orderbook(mk):
            continue

        end_dt = extract_end_dt(mk)
        if end_dt is None or end_dt <= now:
            continue

        condition_id = extract_condition_id(mk)
        if not condition_id or not condition_id.startswith("0x"):
            continue

        tokens = extract_clob_tokens(mk)
        if not tokens:
            continue
        yes_token, no_token = tokens

        cand = ResolvedMarket(
            asset=asset.upper(),
            slug=slug,
            condition_id=condition_id,
            end_dt=end_dt,
            yes_token=yes_token,
            no_token=no_token,
            question=question,
        )

        if best is None or cand.end_dt < best.end_dt:
            best = cand

    return best


# =========================
# Main loop
# =========================
async def run():
    assets = [a.strip().upper() for a in ASSETS.split(",") if a.strip()]

    print("=== UPDOWN 15M ARB BOT START ===", flush=True)
    print(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST} DATA_API_BASE={DATA_API_BASE}", flush=True)
    print(f"ASSETS={assets}", flush=True)
    print(f"MIN_EDGE={MIN_EDGE} MAX_SLIPPAGE={MAX_SLIPPAGE} MIN_USDC={MIN_USDC} MAX_USDC={MAX_USDC} SIZE_MULT={SIZE_MULT}", flush=True)
    print(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_SLUG_LOOKUPS={MAX_SLUG_LOOKUPS}", flush=True)
    print(f"RATE: MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}", flush=True)
    print(f"BOUNDARY: ARM_BEFORE={ARM_BEFORE_BOUNDARY_SEC} CLOSE_BEFORE={CLOSE_BEFORE_BOUNDARY_SEC} STOP_TRYING_BEFORE={STOP_TRYING_BEFORE_BOUNDARY_SEC}", flush=True)

    if not assets:
        raise RuntimeError("ASSETS is empty. Set ASSETS=ETH,BTC,SOL,XRP")
    if not PM_FUNDER or not PM_PRIVATE_KEY:
        raise RuntimeError("PM_FUNDER / PM_PRIVATE_KEY missing")

    client = make_client()

    resolved: Dict[str, ResolvedMarket] = {}
    last_resolve: Dict[str, float] = {a: 0.0 for a in assets}

    trades_ts: List[float] = []
    last_trade = 0.0
    last_heartbeat = 0.0

    async with aiohttp.ClientSession() as session:
        while True:
            t0 = time.time()

            # Heartbeat
            if t0 - last_heartbeat >= LOG_EVERY_SEC:
                print(f"[HEARTBEAT] running. resolved={list(resolved.keys())}", flush=True)
                last_heartbeat = t0

            for asset in assets:
                # Resolve rotating market periodically
                if (t0 - last_resolve.get(asset, 0.0)) >= RESOLVE_EVERY_SEC or asset not in resolved:
                    try:
                        m = await gamma_resolve_current_15m(session, asset)
                        last_resolve[asset] = time.time()
                        if m:
                            resolved[asset] = m
                            tte = seconds_to_end(m.end_dt)
                            print(f"[MARKET] {asset} slug={m.slug} tte={tte:.1f}s yes={m.yes_token} no={m.no_token}", flush=True)
                        else:
                            print(f"[MARKET] {asset} no active 15m market found after slug expansion", flush=True)
                    except Exception as e:
                        print(f"[RESOLVE-ERR] {asset}: {e}", flush=True)
                        continue

                if asset not in resolved:
                    continue

                mkt = resolved[asset]
                tte = seconds_to_end(mkt.end_dt)

                # Stop opening new positions too close to end
                if tte <= CLOSE_BEFORE_BOUNDARY_SEC:
                    if DEBUG:
                        print(f"[SKIP] {asset} too close to end tte={tte:.1f}s", flush=True)
                    continue

                # Only trade within ARM window before end (set ARM huge to be always-on)
                if tte > ARM_BEFORE_BOUNDARY_SEC:
                    if DEBUG:
                        print(f"[SKIP] {asset} not armed yet tte={tte:.1f}s (> ARM_BEFORE)", flush=True)
                    continue

                # Throttles
                if not rate_limit_ok(trades_ts):
                    continue
                if (time.time() - last_trade) < COOLDOWN_SEC:
                    continue

                try:
                    yes_bids, yes_asks = await clob_fetch_book(session, mkt.yes_token)
                    no_bids, no_asks = await clob_fetch_book(session, mkt.no_token)

                    yb, ya = tob(yes_bids, yes_asks)
                    nb, na = tob(no_bids, no_asks)
                    if yb is None or ya is None or nb is None or na is None:
                        continue

                    buy_edge = 1.0 - (ya + na)
                    sell_edge = (yb + nb) - 1.0

                    if DEBUG:
                        print(f"[PRICES] {asset} ya={ya:.4f} na={na:.4f} yb={yb:.4f} nb={nb:.4f} buy_edge={buy_edge:.4f} sell_edge={sell_edge:.4f}", flush=True)

                    # =======================
                    # BUY BOTH (YES+NO < 1)
                    # =======================
                    if buy_edge >= MIN_EDGE:
                        total_budget = clip(MAX_USDC * SIZE_MULT, MIN_USDC, MAX_USDC * SIZE_MULT)
                        leg_budget = total_budget / 2.0

                        y_cap = clip(ya + MAX_SLIPPAGE, 0.0001, 0.9999)
                        n_cap = clip(na + MAX_SLIPPAGE, 0.0001, 0.9999)

                        y_sh = max_buy_shares(yes_asks, y_cap, leg_budget)
                        n_sh = max_buy_shares(no_asks, n_cap, leg_budget)
                        sh = min(y_sh, n_sh)

                        if sh <= 0:
                            continue
                        if sh * (ya + na) < MIN_USDC:
                            continue

                        print(f"[TRADE BUY-BOTH] {asset} edge={buy_edge:.4f} shares={sh:.4f} y_cap={y_cap:.4f} n_cap={n_cap:.4f}", flush=True)
                        r1 = place_fok(client, mkt.yes_token, BUY, y_cap, sh)
                        r2 = place_fok(client, mkt.no_token, BUY, n_cap, sh)
                        print(f"  YES: {r1}", flush=True)
                        print(f"  NO : {r2}", flush=True)

                        trades_ts.append(time.time())
                        last_trade = time.time()
                        continue

                    # =======================
                    # SELL BOTH (YES+NO > 1)
                    # Requires inventory in BOTH outcomes
                    # =======================
                    if sell_edge >= MIN_EDGE:
                        pos = await dataapi_positions(session, PM_FUNDER, mkt.condition_id)
                        up, down = parse_inventory_updown(pos)
                        inv = min(up, down)
                        if inv <= 0:
                            if DEBUG:
                                print(f"[SKIP] {asset} sell_edge but no inventory (up={up:.4f} down={down:.4f})", flush=True)
                            continue

                        y_floor = clip(yb - MAX_SLIPPAGE, 0.0001, 0.9999)
                        n_floor = clip(nb - MAX_SLIPPAGE, 0.0001, 0.9999)

                        cap_sh = (MAX_USDC * SIZE_MULT) / max((yb + nb), 1e-9)
                        desired = min(inv, cap_sh)

                        y_ok = max_sell_shares(yes_bids, y_floor, desired)
                        n_ok = max_sell_shares(no_bids, n_floor, desired)
                        sh = min(y_ok, n_ok)

                        if sh <= 0:
                            continue
                        if sh * (yb + nb) < MIN_USDC:
                            continue

                        print(f"[TRADE SELL-BOTH] {asset} edge={sell_edge:.4f} shares={sh:.4f} y_floor={y_floor:.4f} n_floor={n_floor:.4f} inv={inv:.4f}", flush=True)
                        r1 = place_fok(client, mkt.yes_token, SELL, y_floor, sh)
                        r2 = place_fok(client, mkt.no_token, SELL, n_floor, sh)
                        print(f"  YES: {r1}", flush=True)
                        print(f"  NO : {r2}", flush=True)

                        trades_ts.append(time.time())
                        last_trade = time.time()
                        continue

                except Exception as e:
                    print(f"[ERR] {asset}: {e}", flush=True)
                    continue

            # sleep
            elapsed = time.time() - t0
            await asyncio.sleep(max(0.0, POLL_SEC - elapsed))


if __name__ == "__main__":
    asyncio.run(run())
