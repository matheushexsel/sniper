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
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "25"))

MIN_USDC = float(os.getenv("MIN_USDC", "5"))
MAX_USDC = float(os.getenv("MAX_USDC", "50"))
SIZE_MULT = float(os.getenv("SIZE_MULT", "1.0"))

MAX_TRADES_PER_MIN = int(os.getenv("MAX_TRADES_PER_MIN", "6"))
COOLDOWN_SEC = float(os.getenv("COOLDOWN_SEC", "2.0"))

ARM_BEFORE_BOUNDARY_SEC = float(os.getenv("ARM_BEFORE_BOUNDARY_SEC", "999999"))  # default: always armed
CLOSE_BEFORE_BOUNDARY_SEC = float(os.getenv("CLOSE_BEFORE_BOUNDARY_SEC", "90"))
STOP_TRYING_BEFORE_BOUNDARY_SEC = float(os.getenv("STOP_TRYING_BEFORE_BOUNDARY_SEC", "30"))

# =========================
# NEW env vars
# =========================
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
DATA_API_BASE = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").strip()

ASSETS = os.getenv("ASSETS", "ETH,BTC,SOL,XRP").strip()
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))
MAX_SLIPPAGE = float(os.getenv("MAX_SLIPPAGE", "0.02"))
RESOLVE_EVERY_SEC = float(os.getenv("RESOLVE_EVERY_SEC", "10"))

# Debug knobs
LOG_EVERY_SEC = float(os.getenv("LOG_EVERY_SEC", "15"))  # heartbeat
DEBUG = os.getenv("DEBUG", "1").strip() == "1"


# =========================
# Helpers
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


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def seconds_to_end(end_dt: datetime) -> float:
    return (end_dt - now_utc()).total_seconds()


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def rate_limit_ok(trades_ts: List[float]) -> bool:
    cutoff = time.time() - 60.0
    while trades_ts and trades_ts[0] < cutoff:
        trades_ts.pop(0)
    return len(trades_ts) < MAX_TRADES_PER_MIN


def asset_query_name(asset: str) -> str:
    mapping = {
        "ETH": "Ethereum Up or Down",
        "BTC": "Bitcoin Up or Down",
        "SOL": "Solana Up or Down",
        "XRP": "XRP Up or Down",
    }
    return mapping.get(asset.upper(), f"{asset.upper()} Up or Down")


def is_updown_15m_market(asset: str, slug: str, question: str) -> bool:
    """
    Robust match:
    - Must look like the 15m Up/Down family
    - Must correspond to the asset (by name or ticker)
    """
    s = (slug or "").lower()
    q = (question or "").lower()

    looks_15m = ("updown-15m" in s) or (("up or down" in q) and ("15m" in q or "15 m" in q))
    if not looks_15m:
        return False

    a = asset.upper()
    if a == "ETH":
        return ("ethereum" in q) or ("eth" in s) or ("ethereum" in s)
    if a == "BTC":
        return ("bitcoin" in q) or ("btc" in s) or ("bitcoin" in s)
    if a == "SOL":
        return ("solana" in q) or ("sol" in s) or ("solana" in s)
    if a == "XRP":
        return ("xrp" in q) or ("xrp" in s)

    return (a.lower() in q) or (a.lower() in s)


# =========================
# Polymarket API calls
# =========================
async def gamma_resolve_current(session: aiohttp.ClientSession, asset: str) -> Optional[ResolvedMarket]:
    """
    IMPORTANT: Gamma public-search can return matches in BOTH:
      - data["markets"] (top-level)
      - data["events"][..]["markets"] (nested)
    We scan both.
    """
    params = {
        "q": asset_query_name(asset),
        "events_status": "active",
        "limit_per_type": FETCH_LIMIT,
        "sort": "endDate",
        "ascending": "true",
        "optimized": "true",
    }
    url = f"{GAMMA_HOST}/public-search"

    async with session.get(url, params=params, timeout=10) as r:
        r.raise_for_status()
        data = await r.json()

    now = now_utc()
    best: Optional[ResolvedMarket] = None

    # Collect candidate markets from BOTH places
    candidates = []
    for m in (data.get("markets") or []):
        candidates.append(m)
    for ev in (data.get("events") or []):
        for m in (ev.get("markets") or []):
            candidates.append(m)

    if DEBUG:
        print(
            f"[RESOLVE] {asset}: events={len(data.get('events') or [])} "
            f"top_markets={len(data.get('markets') or [])} total_candidates={len(candidates)}",
            flush=True,
        )
        for m in candidates[:5]:
            print(
                f"  [SAMPLE] slug={m.get('slug')} | q={m.get('question')} | end={m.get('endDate')} "
                f"| OBook={m.get('enableOrderBook')} | closed={m.get('closed')}",
                flush=True,
            )

    for m in candidates:
        if not m.get("enableOrderBook", False):
            continue
        if m.get("closed", False):
            continue

        slug = (m.get("slug") or "").strip()
        question = (m.get("question") or "").strip()
        if not is_updown_15m_market(asset, slug, question):
            continue

        end_str = (m.get("endDate") or "").strip()
        if not end_str:
            continue
        end_dt = parse_iso(end_str)
        if end_dt <= now:
            continue

        condition_id = (m.get("conditionId") or "").strip()
        tokens = m.get("clobTokenIds")

        if not condition_id or not condition_id.startswith("0x"):
            continue
        if not (isinstance(tokens, list) and len(tokens) == 2):
            continue

        cand = ResolvedMarket(
            asset=asset.upper(),
            slug=slug,
            condition_id=condition_id,
            end_dt=end_dt,
            yes_token=str(tokens[0]).strip(),
            no_token=str(tokens[1]).strip(),
            question=question,
        )

        if best is None or cand.end_dt < best.end_dt:
            best = cand

    return best


async def clob_fetch_book(session: aiohttp.ClientSession, token_id: str) -> Tuple[List[Level], List[Level]]:
    url = f"{CLOB_HOST}/book"
    async with session.get(url, params={"token_id": token_id}, timeout=10) as r:
        r.raise_for_status()
        data = await r.json()

    bids = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("bids", [])], key=lambda x: x.price, reverse=True)
    asks = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("asks", [])], key=lambda x: x.price)
    return bids, asks


async def dataapi_positions(session: aiohttp.ClientSession, user: str, condition_id: str) -> List[dict]:
    url = f"{DATA_API_BASE}/positions"
    params = {"user": user, "market": condition_id, "sizeThreshold": 0}
    async with session.get(url, params=params, timeout=10) as r:
        r.raise_for_status()
        return await r.json()


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
# Execution
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
# Main
# =========================
async def run():
    assets = [a.strip().upper() for a in ASSETS.split(",") if a.strip()]

    print("=== UPDOWN ARB BOT START ===", flush=True)
    print(f"CLOB_HOST={CLOB_HOST} CHAIN_ID={CHAIN_ID} GAMMA_HOST={GAMMA_HOST} DATA_API_BASE={DATA_API_BASE}", flush=True)
    print(f"ASSETS={assets}", flush=True)
    print(f"MIN_EDGE={MIN_EDGE} MAX_SLIPPAGE={MAX_SLIPPAGE} MIN_USDC={MIN_USDC} MAX_USDC={MAX_USDC} SIZE_MULT={SIZE_MULT}", flush=True)
    print(f"POLL_SEC={POLL_SEC} RESOLVE_EVERY_SEC={RESOLVE_EVERY_SEC} MAX_TRADES_PER_MIN={MAX_TRADES_PER_MIN} COOLDOWN_SEC={COOLDOWN_SEC}", flush=True)
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
            now = time.time()

            # Heartbeat
            if now - last_heartbeat >= float(os.getenv("LOG_EVERY_SEC", "15")):
                print(f"[HEARTBEAT] running. resolved={list(resolved.keys())}", flush=True)
                last_heartbeat = now

            for asset in assets:
                # Re-resolve rotating market periodically
                if (now - last_resolve.get(asset, 0.0)) >= RESOLVE_EVERY_SEC or asset not in resolved:
                    try:
                        m = await gamma_resolve_current(session, asset)
                        last_resolve[asset] = time.time()
                        if m:
                            resolved[asset] = m
                            tte = seconds_to_end(m.end_dt)
                            print(f"[MARKET] {asset} slug={m.slug} tte={tte:.1f}s yes={m.yes_token} no={m.no_token}", flush=True)
                        else:
                            print(f"[MARKET] {asset} no active market found via Gamma", flush=True)
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

                # Optional: only trade within ARM window before end
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
                        print(f"[PRICES] {asset} ya={ya:.3f} na={na:.3f} yb={yb:.3f} nb={nb:.3f} buy_edge={buy_edge:.3f} sell_edge={sell_edge:.3f}", flush=True)

                    # BUY BOTH
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

                    # SELL BOTH (requires inventory)
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

            await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(run())
