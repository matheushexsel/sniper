# main.py
# Polymarket 15-minute CLOB arbitrage bot
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
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


logger = logging.getLogger("arb15m")


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
    data_api_base: str

    # auth
    chain_id: int
    signature_type: int
    private_key: str
    funder: str

    api_key: str
    api_secret: str
    api_passphrase: str

    # strategy timing
    window_sec: int
    arm_before_boundary_sec: int
    stop_trying_before_boundary_sec: int
    min_tte_select_sec: int
    max_tte_select_sec: int

    assets: List[str]

    # loop
    poll_sec: float
    log_every_sec: float
    debug: bool

    # sizing
    min_usdc: float
    max_usdc: float
    base_usdc: float
    step_usdc: float
    size_mult: float

    # risk
    min_edge: float
    max_slippage: float

    # execution
    dry_run: bool
    max_trades_per_min: int
    cooldown_sec: float
    fill_timeout_sec: float
    max_retries: int
    sell_opposite: bool

    # slug scanning
    max_slug_lookups: int
    slug_scan_buckets_ahead: int
    slug_scan_buckets_behind: int

    # resolver
    resolve_validate_orderbook: bool
    resolve_cache_sec: float

    @staticmethod
    def load() -> "Settings":
        return Settings(
            clob_host=_env("CLOB_HOST", default="https://clob.polymarket.com"),
            data_api_base=_env("DATA_API_BASE", default="https://data-api.polymarket.com"),

            chain_id=_env_int("CHAIN_ID", default=137),
            signature_type=_env_int("PM_SIGNATURE_TYPE", default=2),
            private_key=_env("PM_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY", default=""),
            funder=_env("PM_FUNDER", default=""),

            api_key=_env("PM_API_KEY", default=""),
            api_secret=_env("PM_API_SECRET", default=""),
            api_passphrase=_env("PM_API_PASSPHRASE", default=""),

            window_sec=_env_int("WINDOW_SEC", default=900),
            arm_before_boundary_sec=_env_int("ARM_BEFORE_BOUNDARY_SEC", default=900),
            stop_trying_before_boundary_sec=_env_int("STOP_TRYING_BEFORE_boundary_SEC", default=3),
            min_tte_select_sec=_env_int("MIN_TTE_SELECT_SEC", default=10),
            max_tte_select_sec=_env_int("MAX_TTE_SELECT_SEC", default=900),

            assets=_env_json_list("ASSETS", default=["BTC"]),

            poll_sec=_env_float("POLL_SEC", default=2.0),
            log_every_sec=_env_float("LOG_EVERY_SEC", default=15.0),
            debug=_env_bool("DEBUG", default=False),

            min_usdc=_env_float("MIN_USDC", default=1.0),
            max_usdc=_env_float("MAX_USDC", default=3.0),
            base_usdc=_env_float("BASE_USDC", default=1.0),
            step_usdc=_env_float("STEP_USDC", default=1.0),
            size_mult=_env_float("SIZE_MULT", default=0.25),

            min_edge=_env_float("MIN_EDGE", default=0.02),
            max_slippage=_env_float("MAX_SLIPPAGE", default=0.02),

            dry_run=_env_bool("DRY_RUN", default=True),
            max_trades_per_min=_env_int("MAX_TRADES_PER_MIN", default=20),
            cooldown_sec=_env_float("COOLDOWN_SEC", default=0.25),
            fill_timeout_sec=_env_float("FILL_TIMEOUT_SEC", default=2.0),
            max_retries=_env_int("MAX_RETRIES", default=3),
            sell_opposite=_env_bool("SELL_OPPOSITE", default=True),

            max_slug_lookups=_env_int("MAX_SLUG_LOOKUPS", default=50),
            slug_scan_buckets_ahead=_env_int("SLUG_SCAN_BUCKETS_AHEAD", default=12),
            slug_scan_buckets_behind=_env_int("SLUG_SCAN_BUCKETS_BEHIND", default=12),

            resolve_validate_orderbook=_env_bool("RESOLVE_VALIDATE_ORDERBOOK", default=True),
            resolve_cache_sec=_env_float("RESOLVE_CACHE_SEC", default=10.0),
        )


# ----------------------------
# Time + slug helpers
# ----------------------------

def _bucket_start(ts: int, window_sec: int) -> int:
    return (ts // window_sec) * window_sec


def _compute_candidate_slugs(asset: str, now_ts: int, window_sec: int, ahead: int, behind: int) -> List[str]:
    asset_l = asset.lower()
    minutes = int(window_sec / 60)
    base = _bucket_start(now_ts, window_sec)

    out: List[str] = []
    for off in range(-behind, ahead + 1):
        start_ts = base + off * window_sec
        out.append(f"{asset_l}-updown-{minutes}m-{start_ts}")
        out.append(f"{asset_l}-updown-{minutes}m-{start_ts + window_sec}")

    seen = set()
    dedup: List[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            dedup.append(s)
    return dedup


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

    return market_obj


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
        outcomes = ["Yes", "No"]

    yes_idx, no_idx = 0, 1
    l0 = str(outcomes[0]).strip().lower()
    l1 = str(outcomes[1]).strip().lower()
    if l0 == "no" and l1 == "yes":
        yes_idx, no_idx = 1, 0

    return {
        "slug": slug,
        "market_id": str(market_obj.get("id") or market_obj.get("marketId") or ""),
        "yes_token_id": str(clob_tokens[yes_idx]),
        "no_token_id": str(clob_tokens[no_idx]),
        "yes_label": str(outcomes[yes_idx]),
        "no_label": str(outcomes[no_idx]),
    }


# ----------------------------
# Orderbook normalization + helpers
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

    def place_limit_buy_fok(self, token_id: str, size: float, price: float) -> Dict[str, Any]:
        args = OrderArgs(price=float(price), size=float(size), side=BUY, token_id=str(token_id))
        opts = PartialCreateOrderOptions(tif=OrderType.FOK)
        return self.client.create_and_post_order(args, opts)

    def place_limit_sell_fak(self, token_id: str, size: float, price: float) -> Dict[str, Any]:
        args = OrderArgs(price=float(price), size=float(size), side=SELL, token_id=str(token_id))
        opts = PartialCreateOrderOptions(tif=OrderType.FAK)
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
# Bot - DIAGNOSTIC VERSION
# ----------------------------

class ArbBot:
    def __init__(self, s: Settings):
        self.s = s
        self.trader = PolymarketTrader(s)

        self._active: Dict[str, Dict[str, str]] = {}
        self._invalid_until: Dict[str, float] = {}
        self._last_trade = 0.0
        self._trade_ts: List[float] = []
        self._last_heartbeat = 0.0
        self._last_debug_log: Dict[str, float] = {}

    def _trades_per_min_ok(self) -> bool:
        now = time.time()
        cutoff = now - 60.0
        self._trade_ts = [t for t in self._trade_ts if t >= cutoff]
        return len(self._trade_ts) < self.s.max_trades_per_min

    def _cooldown_ok(self) -> bool:
        return (time.time() - self._last_trade) >= self.s.cooldown_sec

    def _tte(self, now_ts: int) -> int:
        window_start = _bucket_start(now_ts, self.s.window_sec)
        boundary = window_start + self.s.window_sec
        return boundary - now_ts

    def _armed(self, now_ts: int) -> bool:
        return self._tte(now_ts) <= self.s.arm_before_boundary_sec

    def _tradeable(self, now_ts: int) -> bool:
        tte = self._tte(now_ts)
        if tte <= self.s.stop_trying_before_boundary_sec:
            return False
        return self.s.min_tte_select_sec <= tte <= self.s.max_tte_select_sec

    def _debug_throttle_ok(self, asset: str, every_sec: float = 5.0) -> bool:
        now = time.time()
        last = self._last_debug_log.get(asset, 0.0)
        if now - last >= every_sec:
            self._last_debug_log[asset] = now
            return True
        return False

    async def _validate_orderbooks(self, yes_token: str, no_token: str) -> bool:
        try:
            y = await asyncio.to_thread(self.trader.get_order_book, yes_token)
            n = await asyncio.to_thread(self.trader.get_order_book, no_token)
            _book_to_dict(y)
            _book_to_dict(n)
            return True
        except Exception as e:
            msg = str(e)
            if "No orderbook exists" in msg or "status_code=404" in msg:
                return False
            raise

    async def _resolve_market(self, asset: str) -> Optional[Dict[str, str]]:
        until = self._invalid_until.get(asset, 0.0)
        if time.time() < until:
            return self._active.get(asset)

        now_ts = int(time.time())
        cands = _compute_candidate_slugs(
            asset, now_ts, self.s.window_sec, self.s.slug_scan_buckets_ahead, self.s.slug_scan_buckets_behind
        )[: self.s.max_slug_lookups]

        last_err: Optional[str] = None
        for slug in cands:
            try:
                mk = await fetch_market_from_event_page(slug)
                info = extract_yes_no_tokens_from_market(mk, slug)

                if self.s.resolve_validate_orderbook:
                    ok = await self._validate_orderbooks(info["yes_token_id"], info["no_token_id"])
                    if not ok:
                        if self.s.debug:
                            logger.warning(f"[{asset}] slug={slug} has clobTokenIds but NO CLOB orderbooks (skipping).")
                        continue

                prev = self._active.get(asset)
                self._active[asset] = info
                if not prev or prev.get("slug") != info.get("slug"):
                    logger.info(f"[{asset}] Active: {info['slug']} | YES={info['yes_token_id']} NO={info['no_token_id']}")
                self._invalid_until[asset] = time.time() + self.s.resolve_cache_sec
                return info

            except Exception as e:
                last_err = str(e)
                continue

        logger.warning(f"[{asset}] could not resolve any active market via event-page resolver (last_err={last_err}).")
        self._invalid_until[asset] = time.time() + max(3.0, self.s.poll_sec)
        return None

    def _pick_notional(self) -> float:
        if self.s.step_usdc <= 0:
            raw = float(max(self.s.min_usdc, min(self.s.base_usdc, self.s.max_usdc)))
        else:
            steps = int(max(0, math.floor((self.s.max_usdc - self.s.min_usdc) / self.s.step_usdc)))
            k = random.randint(0, max(0, steps))
            raw = self.s.min_usdc + k * self.s.step_usdc

        scaled = raw * max(0.0, self.s.size_mult)
        return float(max(self.s.min_usdc, min(scaled, self.s.max_usdc)))

    async def _books(self, yes_token: str, no_token: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        t1 = asyncio.to_thread(self.trader.get_order_book, yes_token)
        t2 = asyncio.to_thread(self.trader.get_order_book, no_token)
        y_raw, n_raw = await asyncio.gather(t1, t2)
        return _book_to_dict(y_raw), _book_to_dict(n_raw)

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

    async def _cancel(self, oid: Optional[str]) -> None:
        if oid:
            await asyncio.to_thread(self.trader.cancel_order, oid)

    async def _unwind(self, token_id: str, qty: float, book: Dict[str, Any], why: str) -> None:
        if qty <= 0:
            return
        bid = _best_bid(book)
        if not bid:
            logger.warning(f"Unwind skipped (no bids) token={token_id} qty={qty} | {why}")
            return
        bid_price, _ = bid
        if self.s.dry_run:
            logger.warning(f"[DRY_RUN] Would unwind SELL {qty} @ {bid_price} | {why}")
            return
        try:
            resp = await asyncio.to_thread(self.trader.place_limit_sell_fak, token_id, qty, bid_price)
            logger.warning(f"Unwind SELL placed order={_extract_order_id(resp)} | {why}")
        except Exception as e:
            logger.warning(f"Unwind SELL failed | {why} | err={e}")

    async def try_trade(self, asset: str) -> None:
        now_ts = int(time.time())
        tte = self._tte(now_ts)

        if not self._armed(now_ts):
            logger.info(f"[{asset}] EXIT: not armed (tte={tte}s > {self.s.arm_before_boundary_sec}s)")
            return
        if not self._tradeable(now_ts):
            logger.info(f"[{asset}] EXIT: not tradeable (tte={tte}s, window=[{self.s.min_tte_select_sec},{self.s.max_tte_select_sec}])")
            return
        if not self._trades_per_min_ok():
            logger.info(f"[{asset}] EXIT: rate limit ({len(self._trade_ts)}/{self.s.max_trades_per_min})")
            return
        if not self._cooldown_ok():
            logger.info(f"[{asset}] EXIT: cooldown ({time.time()-self._last_trade:.2f}s < {self.s.cooldown_sec}s)")
            return

        market = await self._resolve_market(asset)
        if not market:
            logger.info(f"[{asset}] EXIT: market resolution failed")
            return

        yes_token = market["yes_token_id"]
        no_token = market["no_token_id"]

        try:
            yes_book, no_book = await self._books(yes_token, no_token)
        except Exception as e:
            logger.warning(f"[{asset}] EXIT: book fetch failed: {e}")
            return

        yes_best = _best_ask(yes_book)
        no_best = _best_ask(no_book)
        if not yes_best or not no_best:
            yb = "none" if not yes_best else f"{yes_best[0]:.4f}@{yes_best[1]:.2f}"
            nb = "none" if not no_best else f"{no_best[0]:.4f}@{no_best[1]:.2f}"
            logger.info(f"[{asset}] EXIT: missing asks (YES={yb}, NO={nb})")
            return

        yes_best_p, _ = yes_best
        no_best_p, _ = no_best

        notional = self._pick_notional()
        est_price = max(yes_best_p, no_best_p, 0.01)
        shares = max(1.0, math.floor(notional / est_price))

        yes_worst = _walk_asks_for_size(yes_book, shares)
        no_worst = _walk_asks_for_size(no_book, shares)
        if yes_worst is None or no_worst is None:
            logger.info(f"[{asset}] EXIT: insufficient depth (shares={shares:.0f}, YES_worst={yes_worst}, NO_worst={no_worst})")
            return

        if not _slippage_ok(yes_best_p, yes_worst, self.s.max_slippage):
            slip = (yes_worst - yes_best_p) / max(yes_best_p, 1e-9)
            logger.info(f"[{asset}] EXIT: YES slippage {slip:.4f} > {self.s.max_slippage} (best={yes_best_p:.4f} worst={yes_worst:.4f})")
            return

        if not _slippage_ok(no_best_p, no_worst, self.s.max_slippage):
            slip = (no_worst - no_best_p) / max(no_best_p, 1e-9)
            logger.info(f"[{asset}] EXIT: NO slippage {slip:.4f} > {self.s.max_slippage} (best={no_best_p:.4f} worst={no_worst:.4f})")
            return

        total_cost = yes_worst + no_worst
        edge = 1.0 - total_cost

        # Always log evaluation metrics - this is critical for understanding bot behavior
        logger.info(
            f"[{asset}] eval | notional={notional:.2f} shares={shares:.0f} "
            f"YES best={yes_best_p:.4f} worst={yes_worst:.4f} | "
            f"NO best={no_best_p:.4f} worst={no_worst:.4f} | "
            f"total={total_cost:.4f} edge={edge:.4f} min_edge={self.s.min_edge:.4f}"
        )

        if edge < self.s.min_edge:
            logger.info(f"[{asset}] EXIT: edge {edge:.4f} < min_edge {self.s.min_edge:.4f}")
            return

        logger.info(
            f"[{asset}] TRADE_SIGNAL | edge={edge:.4f} total={total_cost:.4f} shares={shares:.0f} "
            f"YES@{yes_worst:.4f} NO@{no_worst:.4f} tte={tte}s slug={market.get('slug')}"
        )

        await self._execute_paired(asset, yes_token, no_token, shares, yes_worst, no_worst)

    async def _execute_paired(self, asset: str, yes_token: str, no_token: str, shares: float, yes_price: float, no_price: float) -> None:
        if self.s.dry_run:
            logger.warning(
                f"[DRY_RUN] [{asset}] Would BUY paired shares={shares:.0f} YES@{yes_price:.4f} NO@{no_price:.4f} total={yes_price+no_price:.4f}"
            )
            return

        for attempt in range(1, self.s.max_retries + 1):
            yes_oid = None
            no_oid = None
            try:
                r_yes_task = asyncio.to_thread(self.trader.place_limit_buy_fok, yes_token, shares, yes_price)
                r_no_task = asyncio.to_thread(self.trader.place_limit_buy_fok, no_token, shares, no_price)
                r_yes, r_no = await asyncio.gather(r_yes_task, r_no_task)

                yes_oid = _extract_order_id(r_yes)
                no_oid = _extract_order_id(r_no)
                if not yes_oid or not no_oid:
                    raise RuntimeError("Failed to obtain order IDs")

                y = await self._wait_terminal(yes_oid, self.s.fill_timeout_sec)
                n = await self._wait_terminal(no_oid, self.s.fill_timeout_sec)

                y_st = _order_status(y)
                n_st = _order_status(n)
                y_fill = _filled_size(y)
                n_fill = _filled_size(n)

                if y_st == "FILLED" and n_st == "FILLED":
                    self._last_trade = time.time()
                    self._trade_ts.append(self._last_trade)
                    logger.info(f"[{asset}] ✅ Paired fill | YES={yes_oid} NO={no_oid} shares={shares:.0f}")
                    return

                await asyncio.gather(self._cancel(yes_oid), self._cancel(no_oid))

                if self.s.sell_opposite:
                    yes_book, no_book = await self._books(yes_token, no_token)
                    if y_fill > 0 and n_fill <= 0:
                        await self._unwind(yes_token, y_fill, yes_book, f"one-leg fill YES attempt={attempt}")
                    elif n_fill > 0 and y_fill <= 0:
                        await self._unwind(no_token, n_fill, no_book, f"one-leg fill NO attempt={attempt}")
                    else:
                        if y_fill > 0:
                            await self._unwind(yes_token, y_fill, yes_book, f"partial fill YES attempt={attempt}")
                        if n_fill > 0:
                            await self._unwind(no_token, n_fill, no_book, f"partial fill NO attempt={attempt}")

                logger.warning(
                    f"[{asset}] ⚠️ Not filled | YES({y_st},filled={y_fill}) NO({n_st},filled={n_fill}) attempt={attempt}"
                )
                await asyncio.sleep(0.2)

            except Exception as e:
                logger.warning(f"[{asset}] order attempt failed: {e} attempt={attempt}")
                await asyncio.gather(self._cancel(yes_oid), self._cancel(no_oid))
                await asyncio.sleep(0.25)

        logger.warning(f"[{asset}] ❌ Giving up after {self.s.max_retries} attempts.")

    async def run_forever(self) -> None:
        logger.info("Bot started.")
        logger.info(f"Assets={self.s.assets} window={self.s.window_sec}s dry_run={self.s.dry_run}")
        logger.info(f"min_edge={self.s.min_edge} max_slippage={self.s.max_slippage}")
        logger.info(
            f"gates: arm<= {self.s.arm_before_boundary_sec}s, tradeable in [{self.s.min_tte_select_sec},{self.s.max_tte_select_sec}] "
            f"stop_trying<= {self.s.stop_trying_before_boundary_sec}s"
        )

        while True:
            now = time.time()
            if now - self._last_heartbeat >= self.s.log_every_sec:
                self._last_heartbeat = now
                now_ts = int(now)
                tte = self._tte(now_ts)
                logger.info(
                    f"HEARTBEAT | tte={tte}s | armed={self._armed(now_ts)} tradeable={self._tradeable(now_ts)} "
                    f"| trades_last_min={len(self._trade_ts)} | cooldown_ok={self._cooldown_ok()}"
                )

            t0 = time.time()
            try:
                for a in self.s.assets:
                    await self.try_trade(a.upper())
            except Exception as e:
                logger.warning(f"loop error: {e}")

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
    if s.min_usdc > s.max_usdc:
        raise RuntimeError("MIN_USDC must be <= MAX_USDC")
    if s.size_mult <= 0:
        raise RuntimeError("SIZE_MULT must be > 0")


async def main() -> None:
    _setup_logging()
    s = Settings.load()
    _sanity(s)

    bot = ArbBot(s)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
