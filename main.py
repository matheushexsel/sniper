# main.py
"""
Polymarket 15m paired-outcome arb bot (single-file, Railway-ready).

Edge:
- In a binary market, one side settles at $1 and the other at $0.
- If you BUY both sides for total < $1, you lock payoff ~ (1 - total_cost) per paired share
  (ignoring fees and any execution/unwind losses).

This build:
- No batch order posting (PostOrdersArgs not available in your py-clob-client).
- Instead: place both orders concurrently via asyncio.gather + to_thread.
- Conservative: only trades when edge >= MIN_EDGE and slippage <= MAX_SLIPPAGE.
"""

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
# Env helpers (supports both PM_* and POLYMARKET_*)
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
    clob_host: str
    gamma_host: str
    data_api_base: str

    chain_id: int
    signature_type: int
    private_key: str
    funder: str

    api_key: str
    api_secret: str
    api_passphrase: str

    window_sec: int
    arm_before_boundary_sec: int
    stop_trying_before_boundary_sec: int
    min_tte_select_sec: int
    max_tte_select_sec: int

    assets: List[str]
    slug_scan_buckets_ahead: int
    slug_scan_buckets_behind: int
    max_slug_lookups: int

    poll_sec: float
    log_every_sec: float
    debug: bool

    min_usdc: float
    max_usdc: float
    base_usdc: float
    step_usdc: float
    size_mult: float

    min_edge: float
    max_slippage: float

    dry_run: bool
    max_trades_per_min: int
    cooldown_sec: float
    fill_timeout_sec: float
    max_retries: int

    sell_opposite: bool

    @staticmethod
    def load() -> "Settings":
        return Settings(
            clob_host=_env("CLOB_HOST", "POLYMARKET_CLOB_HOST", default="https://clob.polymarket.com"),
            gamma_host=_env("GAMMA_HOST", "POLYMARKET_GAMMA_HOST", default="https://gamma-api.polymarket.com"),
            data_api_base=_env("DATA_API_BASE", default="https://data-api.polymarket.com"),

            chain_id=_env_int("CHAIN_ID", "POLYMARKET_CHAIN_ID", default=137),
            signature_type=_env_int("PM_SIGNATURE_TYPE", "POLYMARKET_SIGNATURE_TYPE", default=2),
            private_key=_env("PM_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY", default=""),
            funder=_env("PM_FUNDER", "POLYMARKET_FUNDER", default=""),

            api_key=_env("PM_API_KEY", "POLYMARKET_API_KEY", default=""),
            api_secret=_env("PM_API_SECRET", "POLYMARKET_API_SECRET", default=""),
            api_passphrase=_env("PM_API_PASSPHRASE", "POLYMARKET_API_PASSPHRASE", default=""),

            window_sec=_env_int("WINDOW_SEC", default=900),
            arm_before_boundary_sec=_env_int("ARM_BEFORE_BOUNDARY_SEC", default=900),
            stop_trying_before_boundary_sec=_env_int("STOP_TRYING_BEFORE_BOUNDARY_SEC", default=3),
            min_tte_select_sec=_env_int("MIN_TTE_SELECT_SEC", default=10),
            max_tte_select_sec=_env_int("MAX_TTE_SELECT_SEC", default=900),

            assets=_env_json_list("ASSETS", default=["BTC"]),
            slug_scan_buckets_ahead=_env_int("SLUG_SCAN_BUCKETS_AHEAD", default=12),
            slug_scan_buckets_behind=_env_int("SLUG_SCAN_BUCKETS_BEHIND", default=12),
            max_slug_lookups=_env_int("MAX_SLUG_LOOKUPS", default=50),

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
        )


# ----------------------------
# Polymarket event page -> token IDs
# ----------------------------

def _extract_next_data_json(html: str) -> Dict[str, Any]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ in event page HTML")
    return json.loads(m.group(1))


def _dig_for_tokens(next_data: Dict[str, Any]) -> Tuple[str, List[str], List[str]]:
    def walk(node: Any):
        if isinstance(node, dict):
            yield node
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)

    market_id = ""
    token_ids: List[str] = []
    outcomes: List[str] = []

    for d in walk(next_data):
        if "clobTokenIds" in d and isinstance(d.get("clobTokenIds"), list):
            tids = [str(x) for x in d["clobTokenIds"] if str(x).strip()]
            if len(tids) >= 2:
                token_ids = tids
                if isinstance(d.get("outcomes"), list):
                    outcomes = [str(x) for x in d["outcomes"]]
                elif isinstance(d.get("outcomeNames"), list):
                    outcomes = [str(x) for x in d["outcomeNames"]]
                if isinstance(d.get("id"), str):
                    market_id = d["id"]
                break

    if len(token_ids) < 2:
        raise RuntimeError("Failed to extract CLOB token IDs from event page")

    if not outcomes:
        outcomes = ["Yes", "No"]

    return market_id, token_ids, outcomes


async def fetch_market_from_slug(slug: str) -> Dict[str, str]:
    slug = slug.split("?")[0].strip()
    url = f"https://polymarket.com/event/{slug}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        next_data = _extract_next_data_json(r.text)
        market_id, token_ids, labels = _dig_for_tokens(next_data)

    # normalize yes/no by label if available
    yes_idx, no_idx = 0, 1
    if len(labels) >= 2:
        l0 = labels[0].strip().lower()
        l1 = labels[1].strip().lower()
        if l0 == "no" and l1 == "yes":
            yes_idx, no_idx = 1, 0

    return {
        "slug": slug,
        "market_id": market_id,
        "yes_token_id": str(token_ids[yes_idx]),
        "no_token_id": str(token_ids[no_idx]),
        "yes_label": labels[yes_idx] if len(labels) > yes_idx else "Yes",
        "no_label": labels[no_idx] if len(labels) > no_idx else "No",
    }


# ----------------------------
# Slug discovery
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


async def _find_slug_via_gamma(s: Settings, asset: str) -> Optional[str]:
    minutes = int(s.window_sec / 60)
    pat = re.compile(rf"^{asset.lower()}-updown-{minutes}m-(\d+)$")
    url = f"{s.gamma_host.rstrip('/')}/markets"
    params = {"closed": "false", "limit": 500}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.json()

        if not isinstance(data, list):
            return None

        now_ts = int(time.time())
        cands: List[Tuple[int, str]] = []
        for m in data:
            slug = str(m.get("slug") or "").strip()
            mo = pat.match(slug)
            if not mo:
                continue
            ts = int(mo.group(1))
            if now_ts < ts + s.window_sec:
                cands.append((ts, slug))

        if not cands:
            for m in data:
                slug = str(m.get("slug") or "").strip()
                mo = pat.match(slug)
                if mo:
                    cands.append((int(mo.group(1)), slug))

        if not cands:
            return None

        cands.sort(key=lambda x: x[0], reverse=True)
        return cands[0][1]
    except Exception as e:
        if s.debug:
            logger.warning(f"Gamma lookup failed for {asset}: {e}")
        return None


async def _find_slug_by_candidates(s: Settings, asset: str) -> Optional[str]:
    now_ts = int(time.time())
    candidates = _compute_candidate_slugs(asset, now_ts, s.window_sec, s.slug_scan_buckets_ahead, s.slug_scan_buckets_behind)
    max_tries = min(s.max_slug_lookups, len(candidates))
    for i in range(max_tries):
        slug = candidates[i]
        try:
            info = await fetch_market_from_slug(slug)
            return info["slug"]
        except Exception:
            continue
    return None


async def find_active_slug(s: Settings, asset: str) -> Optional[str]:
    slug = await _find_slug_via_gamma(s, asset)
    if slug:
        return slug
    return await _find_slug_by_candidates(s, asset)


# ----------------------------
# Order book helpers
# ----------------------------

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

        # If creds provided, use them; else derive them
        if self.s.api_key and self.s.api_secret and self.s.api_passphrase:
            c.set_api_creds(
                {"apiKey": self.s.api_key, "secret": self.s.api_secret, "passphrase": self.s.api_passphrase}
            )
            logger.info("Configured API creds from env.")
        else:
            derived = c.create_or_derive_api_creds()
            c.set_api_creds(derived)
            logger.info("Derived API creds via private key.")

        try:
            logger.info(f"Wallet address: {c.get_address()}")
            if funder:
                logger.info(f"Funder: {funder}")
        except Exception:
            pass

        return c

    def get_order_book(self, token_id: str) -> Dict[str, Any]:
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
# Bot
# ----------------------------

class ArbBot:
    def __init__(self, s: Settings):
        self.s = s
        self.trader = PolymarketTrader(s)

        self._active: Dict[str, Dict[str, str]] = {}
        self._last_log = 0.0
        self._last_trade = 0.0
        self._trade_ts: List[float] = []

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

    async def _refresh_market(self, asset: str) -> Optional[Dict[str, str]]:
        slug = await find_active_slug(self.s, asset)
        if not slug:
            return None
        cached = self._active.get(asset)
        if cached and cached.get("slug") == slug:
            return cached
        info = await fetch_market_from_slug(slug)
        self._active[asset] = info
        logger.info(f"[{asset}] Active: {slug} | YES={info['yes_token_id']} NO={info['no_token_id']}")
        return info

    def _pick_notional(self) -> float:
        if self.s.step_usdc <= 0:
            return float(max(self.s.min_usdc, min(self.s.base_usdc, self.s.max_usdc)))
        steps = int(max(0, math.floor((self.s.max_usdc - self.s.min_usdc) / self.s.step_usdc)))
        k = random.randint(0, max(0, steps))
        val = self.s.min_usdc + k * self.s.step_usdc
        return float(max(self.s.min_usdc, min(val, self.s.max_usdc)))

    async def _books(self, yes_token: str, no_token: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        t1 = asyncio.to_thread(self.trader.get_order_book, yes_token)
        t2 = asyncio.to_thread(self.trader.get_order_book, no_token)
        return await asyncio.gather(t1, t2)

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
        if not self._armed(now_ts) or not self._tradeable(now_ts):
            return
        if not self._trades_per_min_ok() or not self._cooldown_ok():
            return

        market = await self._refresh_market(asset)
        if not market:
            return

        yes_token = market["yes_token_id"]
        no_token = market["no_token_id"]

        try:
            yes_book, no_book = await self._books(yes_token, no_token)
        except Exception as e:
            logger.warning(f"[{asset}] book fetch failed: {e}")
            return

        yes_best = _best_ask(yes_book)
        no_best = _best_ask(no_book)
        if not yes_best or not no_best:
            return

        yes_best_p, _ = yes_best
        no_best_p, _ = no_best

        notional = self._pick_notional() * max(0.0, self.s.size_mult)
        notional = max(self.s.min_usdc, min(notional, self.s.max_usdc))

        est_price = max(yes_best_p, no_best_p, 0.01)
        shares = max(1.0, math.floor(notional / est_price))

        yes_worst = _walk_asks_for_size(yes_book, shares)
        no_worst = _walk_asks_for_size(no_book, shares)
        if yes_worst is None or no_worst is None:
            return

        if not _slippage_ok(yes_best_p, yes_worst, self.s.max_slippage):
            return
        if not _slippage_ok(no_best_p, no_worst, self.s.max_slippage):
            return

        total_cost = yes_worst + no_worst
        edge = 1.0 - total_cost
        if edge < self.s.min_edge:
            return

        if time.time() - self._last_log >= self.s.log_every_sec:
            self._last_log = time.time()
            logger.info(
                f"[{asset}] edge={edge:.4f} total={total_cost:.4f} shares={shares:.0f} "
                f"YES@{yes_worst:.4f} NO@{no_worst:.4f} tte={self._tte(now_ts)}s"
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
                # Place both orders concurrently (best alternative to missing batch post)
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

                # cancel both
                await asyncio.gather(self._cancel(yes_oid), self._cancel(no_oid))

                # unwind any filled leg(s)
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

        while True:
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
        raise RuntimeError("PM_SIGNATURE_TYPE / POLYMARKET_SIGNATURE_TYPE must be 0/1/2")
    if s.window_sec <= 0:
        raise RuntimeError("WINDOW_SEC must be > 0")
    if s.min_usdc > s.max_usdc:
        raise RuntimeError("MIN_USDC must be <= MAX_USDC")


async def main() -> None:
    _setup_logging()
    s = Settings.load()
    _sanity(s)
    bot = ArbBot(s)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
