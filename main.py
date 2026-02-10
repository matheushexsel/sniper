# momentum_strategy.py
# 15-Minute Momentum Ladder Strategy
# Buy both sides at start, then ladder into the winning side, exit before close

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL


# ----------------------------
# Logging
# ----------------------------

def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("momentum15m")


# ----------------------------
# Env helpers
# ----------------------------

def _env(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _env_bool(*keys: str, default: bool = False) -> bool:
    v = _env(*keys, default=str(default))
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(*keys: str, default: int) -> int:
    v = _env(*keys, default=str(default))
    try:
        return int(float(v))
    except Exception:
        return default


def _env_float(*keys: str, default: float) -> float:
    v = _env(*keys, default=str(default))
    try:
        return float(v)
    except Exception:
        return default


def _env_json_list(*keys: str, default: List[str]) -> List[str]:
    raw = _env(*keys, default="")
    if not raw:
        return default
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [x.strip() for x in raw.split(",") if x.strip()]


# ----------------------------
# Settings
# ----------------------------

@dataclass
class Settings:
    # APIs
    clob_host: str

    # auth
    chain_id: int
    signature_type: int
    private_key: str
    funder: str

    api_key: str
    api_secret: str
    api_passphrase: str

    # Timing constants (seconds)
    window_sec: int  # 900 (15 minutes)
    eval_interval_sec: int  # 120 (2 minutes)
    num_evals: int  # 6 evaluations
    exit_time_sec: int  # 780 (13 minutes) - when to force exit
    
    # Asset
    asset: str  # "BTC"

    # loop
    poll_sec: float
    log_every_sec: float

    # Sizing (all in USDC)
    base_usdc_per_side: float  # $1 per side at start
    step_usdc: float  # $1 per add

    # Risk
    max_slippage: float  # 0.05 (5%)
    min_price_delta: float  # 0.0001 - minimum price change to trigger add

    # Execution
    dry_run: bool
    fill_timeout_sec: float
    max_retries: int

    # Market resolution
    max_slug_lookups: int
    slug_scan_buckets_ahead: int
    slug_scan_buckets_behind: int
    resolve_cache_sec: float

    @staticmethod
    def load() -> "Settings":
        return Settings(
            clob_host=_env("CLOB_HOST", default="https://clob.polymarket.com"),

            chain_id=_env_int("CHAIN_ID", default=137),
            signature_type=_env_int("PM_SIGNATURE_TYPE", default=2),
            private_key=_env("PM_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY", default=""),
            funder=_env("PM_FUNDER", default=""),

            api_key=_env("PM_API_KEY", default=""),
            api_secret=_env("PM_API_SECRET", default=""),
            api_passphrase=_env("PM_API_PASSPHRASE", default=""),

            window_sec=_env_int("WINDOW_SEC", default=900),
            eval_interval_sec=_env_int("EVAL_INTERVAL_SEC", default=120),
            num_evals=_env_int("NUM_EVALS", default=6),
            exit_time_sec=_env_int("EXIT_TIME_SEC", default=780),

            asset=_env("ASSET", default="BTC"),

            poll_sec=_env_float("POLL_SEC", default=1.0),
            log_every_sec=_env_float("LOG_EVERY_SEC", default=30.0),

            base_usdc_per_side=_env_float("BASE_USDC_PER_SIDE", default=1.0),
            step_usdc=_env_float("STEP_USDC", default=1.0),

            max_slippage=_env_float("MAX_SLIPPAGE", default=0.05),
            min_price_delta=_env_float("MIN_PRICE_DELTA", default=0.0001),

            dry_run=_env_bool("DRY_RUN", default=True),
            fill_timeout_sec=_env_float("FILL_TIMEOUT_SEC", default=2.0),
            max_retries=_env_int("MAX_RETRIES", default=3),

            max_slug_lookups=_env_int("MAX_SLUG_LOOKUPS", default=50),
            slug_scan_buckets_ahead=_env_int("SLUG_SCAN_BUCKETS_AHEAD", default=3),
            slug_scan_buckets_behind=_env_int("SLUG_SCAN_BUCKETS_BEHIND", default=5),
            resolve_cache_sec=_env_float("RESOLVE_CACHE_SEC", default=10.0),
        )


# ----------------------------
# Time + slug helpers
# ----------------------------

def _bucket_start(ts: int, window_sec: int) -> int:
    return (ts // window_sec) * window_sec


def _compute_candidate_slugs(asset: str, now_ts: int, window_sec: int, ahead: int, behind: int) -> List[str]:
    """
    Generate candidate slugs for UP/DOWN markets.
    
    CRITICAL: The slug timestamp is the MARKET START time.
    We prioritize CURRENT and FUTURE markets over past ones.
    """
    asset_l = asset.lower()
    minutes = int(window_sec / 60)
    
    # Current bucket start time (this IS the slug timestamp for the current market)
    current_bucket_start = _bucket_start(now_ts, window_sec)
    
    out: List[str] = []
    
    # Generate current market FIRST
    out.append(f"{asset_l}-updown-{minutes}m-{current_bucket_start}")
    
    # Then future markets
    for off in range(1, ahead + 1):
        future_start = current_bucket_start + off * window_sec
        out.append(f"{asset_l}-updown-{minutes}m-{future_start}")
    
    # Then past markets (in case we're slightly late to a window)
    for off in range(1, behind + 1):
        past_start = current_bucket_start - off * window_sec
        out.append(f"{asset_l}-updown-{minutes}m-{past_start}")
    
    return out


# ----------------------------
# Polymarket event page resolver
# ----------------------------

_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)


async def fetch_market_from_event_page(slug: str) -> Dict[str, Any]:
    slug = slug.split("?")[0].strip()
    url = f"https://polymarket.com/event/{slug}"
    headers = {"User-Agent": "Mozilla/5.0"}

    async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        html = r.text

    m = _NEXT_DATA_RE.search(html)
    if not m:
        raise RuntimeError("__NEXT_DATA__ payload not found on event page")

    payload = json.loads(m.group(1))
    queries = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("dehydratedState", {})
        .get("queries", [])
    )

    market_obj: Optional[Dict[str, Any]] = None

    for q in queries:
        data = q.get("state", {}).get("data")
        if isinstance(data, dict) and "markets" in data and isinstance(data["markets"], list):
            for mk in data["markets"]:
                if isinstance(mk, dict) and mk.get("slug") == slug:
                    market_obj = mk
                    break
        if market_obj:
            break

    if not market_obj:
        raise RuntimeError("Market slug not found in dehydrated state")

    # Also extract end_date for t_end calculation
    end_date = market_obj.get("endDate") or market_obj.get("end_date")
    if end_date:
        # endDate is typically ISO timestamp or unix timestamp
        try:
            if isinstance(end_date, str):
                import dateutil.parser
                end_date = int(dateutil.parser.parse(end_date).timestamp())
            else:
                end_date = int(end_date)
        except Exception:
            end_date = None

    return {"market": market_obj, "end_date": end_date}


def extract_yes_no_tokens_from_market(market_obj: Dict[str, Any], slug: str) -> Dict[str, str]:
    clob_tokens = market_obj.get("clobTokenIds") or market_obj.get("clob_token_ids") or []
    outcomes = market_obj.get("outcomes") or market_obj.get("outcomeNames") or []

    if isinstance(clob_tokens, str):
        try:
            clob_tokens = json.loads(clob_tokens)
        except Exception:
            clob_tokens = []

    if not isinstance(clob_tokens, list) or len(clob_tokens) < 2:
        raise RuntimeError(f"event-page missing usable clobTokenIds for slug={slug}")

    if not isinstance(outcomes, list) or len(outcomes) < 2:
        outcomes = ["Up", "Down"]

    # UP is index 0, DOWN is index 1 (typically)
    up_idx, down_idx = 0, 1
    l0 = str(outcomes[0]).strip().lower()
    l1 = str(outcomes[1]).strip().lower()
    if "down" in l0 and "up" in l1:
        up_idx, down_idx = 1, 0

    return {
        "slug": slug,
        "market_id": str(market_obj.get("id") or market_obj.get("marketId") or ""),
        "up_token_id": str(clob_tokens[up_idx]),
        "down_token_id": str(clob_tokens[down_idx]),
        "up_label": str(outcomes[up_idx]),
        "down_label": str(outcomes[down_idx]),
    }


# ----------------------------
# Orderbook helpers
# ----------------------------

def _book_to_dict(book: Any) -> Dict[str, Any]:
    if book is None:
        return {"asks": [], "bids": []}

    if isinstance(book, dict):
        return {"asks": book.get("asks") or [], "bids": book.get("bids") or []}

    for m in ("model_dump", "dict"):
        if hasattr(book, m):
            try:
                d = getattr(book, m)()
                if isinstance(d, dict):
                    return {"asks": d.get("asks") or [], "bids": d.get("bids") or []}
            except Exception:
                pass

    asks = getattr(book, "asks", None) or []
    bids = getattr(book, "bids", None) or []

    def _lvls(x: Any) -> List[Dict[str, float]]:
        out: List[Dict[str, float]] = []
        for lvl in (x or []):
            if isinstance(lvl, dict):
                try:
                    out.append({"price": float(lvl["price"]), "size": float(lvl["size"])})
                except Exception:
                    continue
            else:
                p = getattr(lvl, "price", None)
                s = getattr(lvl, "size", None)
                if p is None or s is None:
                    continue
                try:
                    out.append({"price": float(p), "size": float(s)})
                except Exception:
                    continue
        return out

    return {"asks": _lvls(asks), "bids": _lvls(bids)}


def _best_ask(book: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    asks = book.get("asks") or []
    if not asks:
        return None
    try:
        a0 = asks[0]
        return float(a0["price"]), float(a0["size"])
    except Exception:
        return None


def _best_bid(book: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    bids = book.get("bids") or []
    if not bids:
        return None
    try:
        b0 = bids[0]
        return float(b0["price"]), float(b0["size"])
    except Exception:
        return None


def _walk_asks_for_size(book: Dict[str, Any], size_needed: float) -> Optional[float]:
    asks = book.get("asks") or []
    remaining = float(size_needed)
    worst = None
    for lvl in asks:
        try:
            p = float(lvl["price"])
            sz = float(lvl["size"])
        except Exception:
            continue
        if sz <= 0:
            continue
        take = min(sz, remaining)
        remaining -= take
        worst = p
        if remaining <= 1e-9:
            return worst
    return None


def _slippage_ok(best: float, worst: float, max_slip: float) -> bool:
    if best <= 0:
        return False
    return ((worst - best) / best) <= max_slip


# ----------------------------
# CLOB wrapper
# ----------------------------

class PolymarketTrader:
    def __init__(self, s: Settings):
        self.s = s
        self.client = self._make_client()

    def _make_client(self) -> ClobClient:
        if not self.s.private_key:
            raise RuntimeError("Missing PM_PRIVATE_KEY / POLYMARKET_PRIVATE_KEY")

        funder = self.s.funder.strip() if self.s.funder else None

        c = ClobClient(
            self.s.clob_host,
            key=self.s.private_key.strip(),
            chain_id=self.s.chain_id,
            signature_type=self.s.signature_type,
            funder=funder,
        )

        if self.s.api_key and self.s.api_secret and self.s.api_passphrase:
            c.set_api_creds({"apiKey": self.s.api_key, "secret": self.s.api_secret, "passphrase": self.s.api_passphrase})
            logger.info("Configured API creds from env.")
        else:
            derived = c.create_or_derive_api_creds()
            c.set_api_creds(derived)
            logger.info("Derived API creds via private key.")

        logger.info(f"Wallet address: {c.get_address()}")
        if funder:
            logger.info(f"Funder: {funder}")
        return c

    def get_order_book(self, token_id: str) -> Any:
        return self.client.get_order_book(token_id)

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return self.client.get_order(order_id)

    def cancel_order(self, order_id: str) -> None:
        try:
            self.client.cancel(order_id)
        except Exception:
            pass

    def place_limit_buy(self, token_id: str, size: float, price: float, ioc: bool = True) -> Dict[str, Any]:
        args = OrderArgs(price=float(price), size=float(size), side=BUY, token_id=str(token_id))
        opts = PartialCreateOrderOptions(tif=OrderType.FOK if ioc else OrderType.GTC)
        return self.client.create_and_post_order(args, opts)

    def place_limit_sell(self, token_id: str, size: float, price: float) -> Dict[str, Any]:
        # Use GTC (Good-Til-Cancel) for sells to earn maker rebates
        args = OrderArgs(price=float(price), size=float(size), side=SELL, token_id=str(token_id))
        opts = PartialCreateOrderOptions(tif=OrderType.GTC)
        return self.client.create_and_post_order(args, opts)


def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    for k in ("orderID", "orderId", "id"):
        if isinstance(resp, dict) and resp.get(k):
            return str(resp[k])
    d = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(d, dict):
        for k in ("orderID", "orderId", "id"):
            if d.get(k):
                return str(d[k])
    return None


def _order_status(o: Dict[str, Any]) -> str:
    for k in ("status", "state"):
        if isinstance(o, dict) and o.get(k):
            return str(o[k]).upper()
    return ""


def _filled_size(o: Dict[str, Any]) -> float:
    for k in ("filledSize", "filled_size", "sizeFilled", "filled"):
        if isinstance(o, dict) and o.get(k) is not None:
            try:
                return float(o[k])
            except Exception:
                pass
    d = o.get("data") if isinstance(o, dict) else None
    if isinstance(d, dict):
        for k in ("filledSize", "filled_size", "sizeFilled", "filled"):
            if d.get(k) is not None:
                try:
                    return float(d[k])
                except Exception:
                    pass
    return 0.0


# ----------------------------
# Market State Tracker
# ----------------------------

@dataclass
class MarketState:
    slug: str
    up_token_id: str
    down_token_id: str
    t_start: int  # Calculated as t_end - 900
    t_end: int
    
    # Execution state
    base_done: bool = False
    eval_index: int = 0  # How many 2-min evals completed (0-6)
    exit_done: bool = False
    
    # Positions (shares held)
    pos_up_qty: float = 0.0
    pos_down_qty: float = 0.0
    
    # Last evaluation prices (for delta calc)
    last_eval_ask_up: Optional[float] = None
    last_eval_ask_down: Optional[float] = None


# ----------------------------
# Momentum Strategy Bot
# ----------------------------

class MomentumBot:
    def __init__(self, s: Settings):
        self.s = s
        self.trader = PolymarketTrader(s)
        
        self._current_state: Optional[MarketState] = None
        self._invalid_until: float = 0.0
        self._last_heartbeat = 0.0

    def _elapsed(self, state: MarketState) -> int:
        """Seconds elapsed since market start"""
        return int(time.time()) - state.t_start

    def _tte(self, state: MarketState) -> int:
        """Time to expiry in seconds"""
        return state.t_end - int(time.time())

    async def _validate_updown_market(self, up_token: str, down_token: str, slug: str) -> bool:
        """Validate this is actually an UP/DOWN market by checking price distribution"""
        try:
            up_book, down_book = await self._books(up_token, down_token)
            
            up_best = _best_ask(up_book)
            down_best = _best_ask(down_book)
            
            if not up_best or not down_best:
                return False
            
            up_price, _ = up_best
            down_price, _ = down_best
            
            # UP/DOWN markets should have competitive pricing
            if up_price > 0.90 or down_price > 0.90:
                logger.warning(f"Validation failed for {slug}: prices too high (UP={up_price:.4f} DOWN={down_price:.4f})")
                return False
            
            if up_price < 0.10 or down_price < 0.10:
                logger.warning(f"Validation failed for {slug}: prices too low (UP={up_price:.4f} DOWN={down_price:.4f})")
                return False
            
            total = up_price + down_price
            if total < 0.50 or total > 1.50:
                logger.warning(f"Validation failed for {slug}: total price {total:.4f} out of range")
                return False
            
            logger.info(f"✅ Validated UP/DOWN market {slug}: UP={up_price:.4f} DOWN={down_price:.4f} total={total:.4f}")
            return True
            
        except Exception as e:
            logger.warning(f"Validation error for {slug}: {e}")
            return False

    async def _resolve_current_market(self) -> Optional[MarketState]:
        """Find and validate the current 15-minute market"""
        if time.time() < self._invalid_until:
            return self._current_state

        now_ts = int(time.time())
        cands = _compute_candidate_slugs(
            self.s.asset, now_ts, self.s.window_sec, 
            self.s.slug_scan_buckets_ahead, self.s.slug_scan_buckets_behind
        )[: self.s.max_slug_lookups]

        logger.info(f"🔍 Scanning {len(cands)} candidate slugs (now={now_ts}): {cands[:3]}...")

        for slug in cands:
            try:
                result = await fetch_market_from_event_page(slug)
                market_obj = result["market"]
                end_date = result["end_date"]
                
                info = extract_yes_no_tokens_from_market(market_obj, slug)
                
                # Validate it's an active UP/DOWN market
                ok = await self._validate_updown_market(info["up_token_id"], info["down_token_id"], slug)
                if not ok:
                    continue
                
                # Calculate t_start and t_end
                if end_date:
                    t_end = end_date
                else:
                    # Fallback: parse from slug timestamp
                    try:
                        ts_from_slug = int(slug.split("-")[-1])
                        t_end = ts_from_slug + self.s.window_sec
                    except Exception:
                        logger.warning(f"Could not parse end time from slug {slug}")
                        continue
                
                t_start = t_end - self.s.window_sec
                
                # Check if this is a new market or same as current
                if self._current_state and self._current_state.slug == slug:
                    # Same market, just return existing state
                    self._invalid_until = time.time() + self.s.resolve_cache_sec
                    return self._current_state
                
                # New market found
                state = MarketState(
                    slug=slug,
                    up_token_id=info["up_token_id"],
                    down_token_id=info["down_token_id"],
                    t_start=t_start,
                    t_end=t_end,
                )
                
                logger.info(f"🎯 NEW MARKET: {slug} | UP={info['up_token_id'][:16]}... DOWN={info['down_token_id'][:16]}...")
                logger.info(f"   Start: {t_start} | End: {t_end} | Window: {self.s.window_sec}s")
                
                self._current_state = state
                self._invalid_until = time.time() + self.s.resolve_cache_sec
                return state

            except Exception as e:
                continue

        logger.warning(f"[{self.s.asset}] Could not resolve any active UP/DOWN market")
        self._invalid_until = time.time() + max(5.0, self.s.poll_sec)
        return None

    async def _books(self, up_token: str, down_token: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        t1 = asyncio.to_thread(self.trader.get_order_book, up_token)
        t2 = asyncio.to_thread(self.trader.get_order_book, down_token)
        up_raw, down_raw = await asyncio.gather(t1, t2)
        return _book_to_dict(up_raw), _book_to_dict(down_raw)

    async def _wait_terminal(self, oid: str, timeout: float) -> Dict[str, Any]:
        deadline = time.time() + timeout
        last: Dict[str, Any] = {}
        while time.time() < deadline:
            try:
                o = await asyncio.to_thread(self.trader.get_order, oid)
                last = o
                st = _order_status(o)
                if st in ("FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
                    return o
            except Exception:
                pass
            await asyncio.sleep(0.15)
        return last

    async def _buy_side(self, token_id: str, usdc_amount: float, side_name: str) -> float:
        """Buy a side with USDC amount, return shares filled"""
        up_book, down_book = await self._books(self._current_state.up_token_id, self._current_state.down_token_id)
        book = up_book if side_name == "UP" else down_book
        
        ask = _best_ask(book)
        if not ask:
            logger.warning(f"❌ No ask for {side_name}, skipping buy")
            return 0.0
        
        ask_price, _ = ask
        shares = usdc_amount / max(ask_price, 0.01)
        
        # Check slippage
        worst = _walk_asks_for_size(book, shares)
        if worst is None or not _slippage_ok(ask_price, worst, self.s.max_slippage):
            logger.warning(f"❌ {side_name} slippage too high, skipping")
            return 0.0
        
        if self.s.dry_run:
            logger.info(f"[DRY_RUN] Would BUY {shares:.2f} {side_name} @ {worst:.4f} (${usdc_amount:.2f})")
            return shares
        
        try:
            resp = await asyncio.to_thread(self.trader.place_limit_buy, token_id, shares, worst, ioc=True)
            oid = _extract_order_id(resp)
            if not oid:
                logger.warning(f"❌ No order ID from {side_name} buy")
                return 0.0
            
            order = await self._wait_terminal(oid, self.s.fill_timeout_sec)
            filled = _filled_size(order)
            
            if filled > 0:
                logger.info(f"✅ BOUGHT {filled:.2f} {side_name} @ {worst:.4f}")
            else:
                logger.warning(f"⚠️ {side_name} buy not filled")
            
            return filled
            
        except Exception as e:
            logger.warning(f"❌ {side_name} buy failed: {e}")
            return 0.0

    async def _sell_all(self, state: MarketState) -> None:
        """Sell all positions at T=13min to go flat and earn maker rebates"""
        up_book, down_book = await self._books(state.up_token_id, state.down_token_id)
        
        tasks = []
        
        # Sell UP position if any
        if state.pos_up_qty > 0:
            up_bid = _best_bid(up_book)
            if up_bid:
                bid_price, _ = up_bid
                # Place limit slightly below bid to ensure fill while earning maker rebate
                sell_price = bid_price * 0.999  # 0.1% below bid
                tasks.append(self._sell_side(state.up_token_id, state.pos_up_qty, sell_price, "UP"))
        
        # Sell DOWN position if any
        if state.pos_down_qty > 0:
            down_bid = _best_bid(down_book)
            if down_bid:
                bid_price, _ = down_bid
                sell_price = bid_price * 0.999
                tasks.append(self._sell_side(state.down_token_id, state.pos_down_qty, sell_price, "DOWN"))
        
        if tasks:
            await asyncio.gather(*tasks)
        
        logger.info(f"💰 EXIT COMPLETE | UP_sold={state.pos_up_qty:.2f} DOWN_sold={state.pos_down_qty:.2f}")

    async def _sell_side(self, token_id: str, qty: float, price: float, side_name: str) -> None:
        """Sell a side at limit price (GTC for maker rebates)"""
        if self.s.dry_run:
            logger.info(f"[DRY_RUN] Would SELL {qty:.2f} {side_name} @ {price:.4f}")
            return
        
        try:
            resp = await asyncio.to_thread(self.trader.place_limit_sell, token_id, qty, price)
            oid = _extract_order_id(resp)
            logger.info(f"📤 SELL order placed: {side_name} {qty:.2f} @ {price:.4f} (order_id={oid})")
        except Exception as e:
            logger.warning(f"❌ SELL {side_name} failed: {e}")

    async def step_base_entry(self, state: MarketState) -> None:
        """Step 1: Base entry at market start - buy both sides equally"""
        elapsed = self._elapsed(state)
        
        # Only execute in first 60 seconds
        if state.base_done or elapsed > 60:
            return
        
        logger.info(f"🎬 BASE ENTRY | Buying ${self.s.base_usdc_per_side:.2f} each side")
        
        # Buy both sides simultaneously
        up_task = self._buy_side(state.up_token_id, self.s.base_usdc_per_side, "UP")
        down_task = self._buy_side(state.down_token_id, self.s.base_usdc_per_side, "DOWN")
        
        up_filled, down_filled = await asyncio.gather(up_task, down_task)
        
        state.pos_up_qty += up_filled
        state.pos_down_qty += down_filled
        
        # Record prices for next evaluation
        up_book, down_book = await self._books(state.up_token_id, state.down_token_id)
        up_ask = _best_ask(up_book)
        down_ask = _best_ask(down_book)
        
        if up_ask:
            state.last_eval_ask_up, _ = up_ask
        if down_ask:
            state.last_eval_ask_down, _ = down_ask
        
        state.base_done = True
        logger.info(f"✅ BASE DONE | UP={state.pos_up_qty:.2f} DOWN={state.pos_down_qty:.2f}")

    async def step_momentum_add(self, state: MarketState) -> None:
        """Step 2: Momentum adds every 2 minutes - buy the winning side"""
        elapsed = self._elapsed(state)
        
        if not state.base_done:
            return
        
        if state.eval_index >= self.s.num_evals:
            return
        
        # Check if it's time for next evaluation
        target_time = (state.eval_index + 1) * self.s.eval_interval_sec
        if elapsed < target_time:
            return
        
        # Prevent evaluations after exit time
        if elapsed >= self.s.exit_time_sec:
            return
        
        # Fetch current prices
        up_book, down_book = await self._books(state.up_token_id, state.down_token_id)
        up_ask = _best_ask(up_book)
        down_ask = _best_ask(down_book)
        
        if not up_ask or not down_ask:
            logger.warning(f"⚠️ Eval #{state.eval_index + 1}: Missing asks, skipping")
            state.eval_index += 1
            return
        
        up_price_now, _ = up_ask
        down_price_now, _ = down_ask
        
        # Calculate deltas
        if state.last_eval_ask_up is None or state.last_eval_ask_down is None:
            # First eval, use current prices
            delta_up = 0.0
            delta_down = 0.0
        else:
            delta_up = up_price_now - state.last_eval_ask_up
            delta_down = down_price_now - state.last_eval_ask_down
        
        # Determine winner
        if abs(delta_up - delta_down) < self.s.min_price_delta:
            # Tie or tiny diff - use higher current price
            winner = "UP" if up_price_now >= down_price_now else "DOWN"
        elif delta_up > delta_down:
            winner = "UP"
        else:
            winner = "DOWN"
        
        logger.info(
            f"📊 EVAL #{state.eval_index + 1} @ T={elapsed}s | "
            f"UP: {state.last_eval_ask_up:.4f}→{up_price_now:.4f} (Δ={delta_up:+.4f}) | "
            f"DOWN: {state.last_eval_ask_down:.4f}→{down_price_now:.4f} (Δ={delta_down:+.4f}) | "
            f"WINNER: {winner}"
        )
        
        # Buy winner
        winner_token = state.up_token_id if winner == "UP" else state.down_token_id
        filled = await self._buy_side(winner_token, self.s.step_usdc, winner)
        
        if winner == "UP":
            state.pos_up_qty += filled
        else:
            state.pos_down_qty += filled
        
        # Update last eval prices
        state.last_eval_ask_up = up_price_now
        state.last_eval_ask_down = down_price_now
        state.eval_index += 1

    async def step_forced_exit(self, state: MarketState) -> None:
        """Step 3: Forced exit at minute 13"""
        elapsed = self._elapsed(state)
        tte = self._tte(state)
        
        # Exit if we hit exit time OR if time is running out
        should_exit = (elapsed >= self.s.exit_time_sec) or (tte <= 60)
        
        if not should_exit or state.exit_done:
            return
        
        logger.info(f"🚪 FORCED EXIT @ T={elapsed}s (tte={tte}s)")
        await self._sell_all(state)
        state.exit_done = True

    async def run_cycle(self) -> None:
        """Main execution cycle"""
        # Resolve current market
        state = await self._resolve_current_market()
        if not state:
            return
        
        # Execute strategy steps in order
        await self.step_base_entry(state)
        await self.step_momentum_add(state)
        await self.step_forced_exit(state)
        
        # Check if we need to move to next market
        if state.exit_done:
            tte = self._tte(state)
            if tte <= 0:
                logger.info(f"✅ Market {state.slug} expired, waiting for next...")
                self._current_state = None  # Force refresh next cycle

    async def run_forever(self) -> None:
        logger.info("=" * 80)
        logger.info("15-MINUTE MOMENTUM LADDER STRATEGY")
        logger.info("=" * 80)
        logger.info(f"Asset: {self.s.asset}")
        logger.info(f"Base: ${self.s.base_usdc_per_side:.2f}/side | Step: ${self.s.step_usdc:.2f}")
        logger.info(f"Evals: {self.s.num_evals} every {self.s.eval_interval_sec}s | Exit: {self.s.exit_time_sec}s")
        logger.info(f"DRY_RUN: {self.s.dry_run}")
        logger.info("=" * 80)
        
        while True:
            now = time.time()
            
            # Heartbeat
            if now - self._last_heartbeat >= self.s.log_every_sec:
                self._last_heartbeat = now
                if self._current_state:
                    elapsed = self._elapsed(self._current_state)
                    tte = self._tte(self._current_state)
                    logger.info(
                        f"💓 HEARTBEAT | Market: {self._current_state.slug} | "
                        f"T={elapsed}s | TTE={tte}s | "
                        f"Evals: {self._current_state.eval_index}/{self.s.num_evals} | "
                        f"Pos: UP={self._current_state.pos_up_qty:.2f} DOWN={self._current_state.pos_down_qty:.2f}"
                    )
                else:
                    logger.info("💓 HEARTBEAT | No active market")
            
            # Main cycle
            t0 = time.time()
            try:
                await self.run_cycle()
            except Exception as e:
                logger.error(f"❌ Cycle error: {e}")
            
            # Sleep
            dt = time.time() - t0
            await asyncio.sleep(max(0.0, self.s.poll_sec - dt))


# ----------------------------
# Entrypoint
# ----------------------------

def _sanity(s: Settings) -> None:
    if not s.private_key:
        raise RuntimeError("Missing PM_PRIVATE_KEY / POLYMARKET_PRIVATE_KEY")
    if s.signature_type not in (0, 1, 2):
        raise RuntimeError("PM_SIGNATURE_TYPE must be 0/1/2")
    if s.window_sec <= 0:
        raise RuntimeError("WINDOW_SEC must be > 0")
    if s.exit_time_sec >= s.window_sec:
        raise RuntimeError("EXIT_TIME_SEC must be < WINDOW_SEC")
    if s.eval_interval_sec * s.num_evals >= s.exit_time_sec:
        raise RuntimeError("Total eval time exceeds exit time")


async def main() -> None:
    _setup_logging()
    s = Settings.load()
    _sanity(s)

    bot = MomentumBot(s)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
