import os
import re
import time
import math
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

# You already have this; keep it (0=EOA, 1=Magic/email, 2=proxy).
PM_SIGNATURE_TYPE = int(os.getenv("PM_SIGNATURE_TYPE", "0"))

# You already have these
POLL_SEC = float(os.getenv("POLL_SEC", "0.50"))
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "25"))

MIN_USDC = float(os.getenv("MIN_USDC", "5"))
MAX_USDC = float(os.getenv("MAX_USDC", "50"))
SIZE_MULT = float(os.getenv("SIZE_MULT", "1.0"))

MAX_TRADES_PER_MIN = int(os.getenv("MAX_TRADES_PER_MIN", "6"))
COOLDOWN_SEC = float(os.getenv("COOLDOWN_SEC", "2.0"))

# Boundary safety (reusing your names)
ARM_BEFORE_BOUNDARY_SEC = float(os.getenv("ARM_BEFORE_BOUNDARY_SEC", "120"))
CLOSE_BEFORE_BOUNDARY_SEC = float(os.getenv("CLOSE_BEFORE_BOUNDARY_SEC", "90"))
STOP_TRYING_BEFORE_BOUNDARY_SEC = float(os.getenv("STOP_TRYING_BEFORE_BOUNDARY_SEC", "30"))

# =========================
# NEW (optional) env vars
# =========================
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
DATA_API_BASE = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").strip()

# Assets: defaults to what you asked for
ASSETS = os.getenv("ASSETS", "ETH,BTC,SOL,XRP").strip()

# Strategy thresholds
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))       # edge per $1 payout (in dollars)
MAX_SLIPPAGE = float(os.getenv("MAX_SLIPPAGE", "0.02"))  # price cushion vs top-of-book

# How often to re-resolve the rotating 15m market per asset
RESOLVE_EVERY_SEC = float(os.getenv("RESOLVE_EVERY_SEC", "10"))

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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt_str: str) -> datetime:
    # Handles Z suffix
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def _asset_query_name(asset: str) -> str:
    # Polymarket UI uses full names for these markets.
    # If they ever change naming, adjust here.
    mapping = {
        "ETH": "Ethereum Up or Down",
        "BTC": "Bitcoin Up or Down",
        "SOL": "Solana Up or Down",
        "XRP": "XRP Up or Down",
    }
    return mapping.get(asset.upper(), f"{asset.upper()} Up or Down")


def _slug_prefix(asset: str) -> str:
    # Matches what you showed: eth-updown-15m-<timestamp>
    return f"{asset.lower()}-updown-15m-"


def _seconds_to_end(end_dt: datetime) -> float:
    return (end_dt - _now_utc()).total_seconds()


def _rate_limit_guard(trades_timestamps: List[float]) -> bool:
    """True if allowed to trade now under MAX_TRADES_PER_MIN."""
    cutoff = time.time() - 60.0
    while trades_timestamps and trades_timestamps[0] < cutoff:
        trades_timestamps.pop(0)
    return len(trades_timestamps) < MAX_TRADES_PER_MIN


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =========================
# Polymarket API calls
# =========================
async def gamma_resolve_current_15m_market(session: aiohttp.ClientSession, asset: str) -> Optional[ResolvedMarket]:
    """
    Finds the active 15m Up/Down market for an asset by:
    - Gamma public-search with q="<Asset> Up or Down"
    - filter: enableOrderBook, not closed, slug prefix match
    - choose: soonest endDate in the future
    """
    params = {
        "q": _asset_query_name(asset),
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

    now = _now_utc()
    best: Optional[ResolvedMarket] = None

    slug_pref = _slug_prefix(asset)

    for ev in data.get("events", []) or []:
        for m in ev.get("markets", []) or []:
            if not m.get("enableOrderBook", False):
                continue
            if m.get("closed", False):
                continue

            slug = (m.get("slug") or "").strip()
            if not slug.startswith(slug_pref):
                continue

            end_str = (m.get("endDate") or "").strip()
            if not end_str:
                continue
            end_dt = _parse_iso(end_str)
            if end_dt <= now:
                continue

            condition_id = (m.get("conditionId") or "").strip()
            if not condition_id or not condition_id.startswith("0x"):
                continue

            tokens = m.get("clobTokenIds")
            if not (isinstance(tokens, list) and len(tokens) == 2):
                continue

            yes_token, no_token = str(tokens[0]).strip(), str(tokens[1]).strip()
            if not yes_token or not no_token:
                continue

            cand = ResolvedMarket(
                asset=asset.upper(),
                slug=slug,
                condition_id=condition_id,
                end_dt=end_dt,
                yes_token=yes_token,
                no_token=no_token,
            )

            if best is None or cand.end_dt < best.end_dt:
                best = cand

    return best


async def clob_fetch_book(session: aiohttp.ClientSession, token_id: str) -> Tuple[List[Level], List[Level]]:
    """
    Uses the public CLOB /book endpoint (no auth) for depth.
    """
    url = f"{CLOB_HOST}/book"
    async with session.get(url, params={"token_id": token_id}, timeout=10) as r:
        r.raise_for_status()
        data = await r.json()

    bids = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("bids", [])], key=lambda x: x.price, reverse=True)
    asks = sorted([Level(float(x["price"]), float(x["size"])) for x in data.get("asks", [])], key=lambda x: x.price)
    return bids, asks


async def dataapi_get_positions_for_market(session: aiohttp.ClientSession, user: str, condition_id: str) -> List[dict]:
    """
    Data API positions endpoint supports ?user=...&market=...  (market = conditionId).
    """
    url = f"{DATA_API_BASE}/positions"
    params = {
        "user": user,
        "market": condition_id,
        "sizeThreshold": 0,  # include tiny positions
    }
    async with session.get(url, params=params, timeout=10) as r:
        r.raise_for_status()
        return await r.json()


# =========================
# Strategy math
# =========================
def top_of_book_prices(bids: List[Level], asks: List[Level]) -> Tuple[Optional[float], Optional[float]]:
    bid = bids[0].price if bids else None
    ask = asks[0].price if asks else None
    return bid, ask


def max_shares_buyable_under_price_cap(asks: List[Level], price_cap: float, max_cost: float) -> float:
    """
    Conservative: buy shares walking asks, but only take levels <= price_cap,
    and stop once max_cost is reached.
    """
    shares = 0.0
    cost = 0.0
    for lvl in asks:
        if lvl.price > price_cap:
            break
        if lvl.size <= 0:
            continue
        # how many shares can we take from this level given remaining budget?
        remaining_budget = max_cost - cost
        if remaining_budget <= 0:
            break
        take = min(lvl.size, remaining_budget / lvl.price)
        if take <= 0:
            break
        shares += take
        cost += take * lvl.price
    return shares


def max_shares_sellable_over_price_floor(bids: List[Level], price_floor: float, max_shares: float) -> float:
    """
    Conservative: sell shares walking bids, only levels >= price_floor.
    """
    shares = 0.0
    for lvl in bids:
        if lvl.price < price_floor:
            break
        if lvl.size <= 0:
            continue
        take = min(lvl.size, max_shares - shares)
        if take <= 0:
            break
        shares += take
        if shares >= max_shares:
            break
    return shares


def parse_updown_inventory(positions: List[dict]) -> Tuple[float, float]:
    """
    Returns (up_size, down_size) based on Data API positions "outcome" field.
    Outcome names for these markets are typically "Up" and "Down".
    """
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
# Execution
# =========================
def make_trading_client() -> ClobClient:
    if not PM_FUNDER or not PM_PRIVATE_KEY:
        raise RuntimeError("Missing PM_FUNDER or PM_PRIVATE_KEY.")

    client = ClobClient(
        CLOB_HOST,
        key=PM_PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=PM_SIGNATURE_TYPE,
        funder=PM_FUNDER,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def place_fok_order(client: ClobClient, token_id: str, side: str, price: float, size: float) -> dict:
    # FOK: either fills immediately or cancels (reduces partial fill pain per leg)
    order = OrderArgs(
        token_id=token_id,
        price=round(price, 4),
        size=round(size, 4),
        side=side,
    )
    signed = client.create_order(order)
    return client.post_order(signed, OrderType.FOK)


# =========================
# Main loop
# =========================
async def run():
    assets = [a.strip().upper() for a in ASSETS.split(",") if a.strip()]
    if not assets:
        raise RuntimeError("ASSETS is empty. Set ASSETS=ETH,BTC,SOL,XRP")

    client = make_trading_client()

    # Cache resolved rotating markets per asset
    resolved: Dict[str, ResolvedMarket] = {}
    last_resolve: Dict[str, float] = {a: 0.0 for a in assets}

    trades_timestamps: List[float] = []
    last_trade_time = 0.0

    async with aiohttp.ClientSession() as session:
        while True:
            loop_start = time.time()

            for asset in assets:
                # Throttle global trade rate
                if not _rate_limit_guard(trades_timestamps):
                    continue
                if (time.time() - last_trade_time) < COOLDOWN_SEC:
                    continue

                # Resolve current market periodically (slug rotates)
                if (time.time() - last_resolve.get(asset, 0.0)) >= RESOLVE_EVERY_SEC or asset not in resolved:
                    try:
                        m = await gamma_resolve_current_15m_market(session, asset)
                        last_resolve[asset] = time.time()
                        if m:
                            resolved[asset] = m
                    except Exception as e:
                        print(f"[RESOLVE-ERR] {asset}: {e}")
                        continue

                if asset not in resolved:
                    continue

                mkt = resolved[asset]
                secs_to_end = _seconds_to_end(mkt.end_dt)

                # Safety: don't open new positions too close to end
                if secs_to_end <= CLOSE_BEFORE_BOUNDARY_SEC:
                    continue
                # Optional: only arm within a window before end (keeps it focused)
                if secs_to_end > ARM_BEFORE_BOUNDARY_SEC:
                    # This line means you only trade closer to settlement.
                    # If you want ALWAYS on, comment it out.
                    pass

                try:
                    yes_bids, yes_asks = await clob_fetch_book(session, mkt.yes_token)
                    no_bids, no_asks = await clob_fetch_book(session, mkt.no_token)

                    yes_bid, yes_ask = top_of_book_prices(yes_bids, yes_asks)
                    no_bid, no_ask = top_of_book_prices(no_bids, no_asks)

                    if yes_bid is None or yes_ask is None or no_bid is None or no_ask is None:
                        continue

                    # =======================
                    # BUY-BOTH arbitrage
                    # =======================
                    # Edge estimate using top-of-book (fast filter)
                    buy_edge = 1.0 - (yes_ask + no_ask)
                    if buy_edge >= MIN_EDGE:
                        # Conservative price caps
                        yes_price_cap = _clip(yes_ask + MAX_SLIPPAGE, 0.0001, 0.9999)
                        no_price_cap = _clip(no_ask + MAX_SLIPPAGE, 0.0001, 0.9999)

                        # Budget (USDC) for BOTH legs total
                        total_budget = _clip(MAX_USDC * SIZE_MULT, MIN_USDC, MAX_USDC * SIZE_MULT)

                        # Split budget across legs (simple + robust)
                        leg_budget = total_budget / 2.0

                        # Compute max shares we can buy under price caps and per-leg budget
                        yes_shares = max_shares_buyable_under_price_cap(yes_asks, yes_price_cap, leg_budget)
                        no_shares = max_shares_buyable_under_price_cap(no_asks, no_price_cap, leg_budget)

                        shares = min(yes_shares, no_shares)
                        if shares <= 0:
                            continue

                        # Enforce min notional: approx cost ~ shares*(ask_yes+ask_no)
                        approx_cost = shares * (yes_ask + no_ask)
                        if approx_cost < MIN_USDC:
                            continue

                        # Execute both legs (FOK) back-to-back
                        print(f"[BUY-BOTH] {asset} slug={mkt.slug} tte={secs_to_end:.1f}s edge={buy_edge:.4f} shares={shares:.4f}")
                        r1 = place_fok_order(client, mkt.yes_token, BUY, yes_price_cap, shares)
                        r2 = place_fok_order(client, mkt.no_token, BUY, no_price_cap, shares)

                        print(f"  YES buy resp: {r1}")
                        print(f"  NO  buy resp: {r2}")

                        trades_timestamps.append(time.time())
                        last_trade_time = time.time()
                        continue  # avoid stacking buy+sell same tick

                    # =======================
                    # SELL-BOTH arbitrage
                    # =======================
                    sell_edge = (yes_bid + no_bid) - 1.0
                    if sell_edge >= MIN_EDGE:
                        # Need inventory (Up and Down) to sell both.
                        # Use Data API positions for this conditionId.
                        try:
                            positions = await dataapi_get_positions_for_market(session, PM_FUNDER, mkt.condition_id)
                        except Exception as e:
                            print(f"[POS-ERR] {asset}: {e}")
                            continue

                        up_size, down_size = parse_updown_inventory(positions)
                        inv_shares = min(up_size, down_size)

                        if inv_shares <= 0:
                            continue

                        # Conservative price floors (sell at/near best bids)
                        yes_price_floor = _clip(yes_bid - MAX_SLIPPAGE, 0.0001, 0.9999)
                        no_price_floor = _clip(no_bid - MAX_SLIPPAGE, 0.0001, 0.9999)

                        # Cap by MAX_USDC equivalent (proceeds proxy)
                        # shares cap: approximate proceeds ~ shares*(bid_yes+bid_no)
                        max_shares_by_cap = (MAX_USDC * SIZE_MULT) / max((yes_bid + no_bid), 1e-9)
                        desired_shares = min(inv_shares, max_shares_by_cap)

                        # Also ensure liquidity exists above price floor
                        yes_sellable = max_shares_sellable_over_price_floor(yes_bids, yes_price_floor, desired_shares)
                        no_sellable = max_shares_sellable_over_price_floor(no_bids, no_price_floor, desired_shares)
                        shares = min(yes_sellable, no_sellable)

                        if shares <= 0:
                            continue

                        # min notional check (proceeds proxy)
                        approx_proceeds = shares * (yes_bid + no_bid)
                        if approx_proceeds < MIN_USDC:
                            continue

                        print(f"[SELL-BOTH] {asset} slug={mkt.slug} tte={secs_to_end:.1f}s edge={sell_edge:.4f} shares={shares:.4f} inv={inv_shares:.4f}")
                        r1 = place_fok_order(client, mkt.yes_token, SELL, yes_price_floor, shares)
                        r2 = place_fok_order(client, mkt.no_token, SELL, no_price_floor, shares)

                        print(f"  YES sell resp: {r1}")
                        print(f"  NO  sell resp: {r2}")

                        trades_timestamps.append(time.time())
                        last_trade_time = time.time()
                        continue

                except Exception as e:
                    print(f"[LOOP-ERR] {asset}: {e}")
                    continue

            # Sleep until next tick
            elapsed = time.time() - loop_start
            delay = max(0.0, POLL_SEC - elapsed)
            await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
