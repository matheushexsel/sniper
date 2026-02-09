# main.py
"""
Polymarket 15m paired-outcome arb bot (single-file, Railway-ready).

Core idea:
- In a binary market, exactly one side settles at $1.00 and the other at $0.00.
- If you can BUY both sides for total cost < $1.00 (after slippage/fees buffer),
  you lock a deterministic payoff at resolution: + (1.00 - total_cost) per paired share.

Design goals for Railway:
- Single process, no web server.
- Pure env-var configuration.
- Conservative execution controls: rate limiting, time-to-boundary gating, cancel/unwind.

Supports BOTH naming schemes:
- Your Railway vars (PM_*, GAMMA_HOST, CLOB_HOST, WINDOW_SEC, etc.)
- The other bot naming (POLYMARKET_*)

IMPORTANT:
- This bot uses `py-clob-client` (the same dependency style as the repo you sent).
- If you’re using Magic.link / email-based account, you may need FUNDER depending on signature type.
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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

# py-clob-client (Polymarket CLOB)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs,
    OrderType,
    PostOrdersArgs,
    PartialCreateOrderOptions,
)
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
    # Quiet overly chatty libs if needed
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
    # fallback: comma-separated
    return [x.strip() for x in raw.split(",") if x.strip()]


# ----------------------------
# Settings
# ----------------------------

@dataclass
class Settings:
    # Endpoints
    clob_host: str
    gamma_host: str
    data_api_base: str

    # Chain/signature
    chain_id: int
    signature_type: int
    private_key: str
    funder: str

    # Optional API creds (can be derived)
    api_key: str
    api_secret: str
    api_passphrase: str

    # Strategy timing
    window_sec: int
    arm_before_boundary_sec: int
    close_before_boundary_sec: int
    stop_trying_before_boundary_sec: int
    min_time_to_end_sec: int
    min_tte_select_sec: int
    max_tte_select_sec: int
    exit_at_sec: int
    close_hard_sec: int

    # Market selection / scanning
    assets: List[str]
    slug_scan_buckets_ahead: int
    slug_scan_buckets_behind: int
    max_slug_lookups: int
    fetch_limit: int

    # Poll / logging
    poll_sec: float
    log_every_sec: float
    debug: bool

    # Trading sizing
    min_usdc: float
    max_usdc: float
    base_usdc: float
    step_usdc: float
    size_mult: float

    # Arb thresholds
    min_edge: float
    max_slippage: float

    # Execution controls
    dry_run: bool
    max_trades_per_min: int
    cooldown_sec: float
    fill_timeout_sec: float
    max_retries: int

    # Unwind behavior
    sell_opposite: bool
    sell_opposite_on_divergence: bool
    divergence_threshold: float

    @staticmethod
    def load() -> "Settings":
        # Prefer user-provided vars; support both naming schemes.
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
            close_before_boundary_sec=_env_int("CLOSE_BEFORE_BOUNDARY_SEC", default=30),
            stop_trying_before_boundary_sec=_env_int("STOP_TRYING_BEFORE_BOUNDARY_SEC", default=3),
            min_time_to_end_sec=_env_int("MIN_TIME_TO_END_SEC", default=60),
            min_tte_select_sec=_env_int("MIN_TTE_SELECT_SEC", default=10),
            max_tte_select_sec=_env_int("MAX_TTE_SELECT_SEC", default=900),
            exit_at_sec=_env_int("EXIT_AT_SEC", default=780),
            close_hard_sec=_env_int("CLOSE_HARD_SEC", default=30),

            assets=_env_json_list("ASSETS", default=["BTC"]),
            slug_scan_buckets_ahead=_env_int("SLUG_SCAN_BUCKETS_AHEAD", default=12),
            slug_scan_buckets_behind=_env_int("SLUG_SCAN_BUCKETS_BEHIND", default=12),
            max_slug_lookups=_env_int("MAX_SLUG_LOOKUPS", default=50),
            fetch_limit=_env_int("FETCH_LIMIT", default=50),

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
            sell_opposite_on_divergence=_env_bool("SELL_OPPOSITE_ON_DIVERGENCE", default=True),
            divergence_threshold=_env_float("DIVERGENCE_THRESHOLD", default=0.25),
        )


# ----------------------------
# Polymarket Event Page parser (token IDs + market id)
# ----------------------------

def _extract_next_data_json(html: str) -> Dict[str, Any]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ in event page HTML")
    return json.loads(m.group(1))

def _dig_for_market_and_tokens(next_data: Dict[str, Any]) -> Tuple[str, List[str], List[str]]:
    """
    Returns:
      market_id, token_ids, outcome_labels
    token_ids/outcomes are aligned by outcome order on the page.
    """
    # This is defensive: Polymarket has changed this structure historically.
    # We search the JSON tree for dicts containing keys we need.
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
        # Some payloads have "market" objects with id + outcomes/markets
        # Typical keys seen: id, clobTokenIds, outcomes/outcomeNames
        if not market_id and isinstance(d.get("id"), str) and len(d.get("id")) >= 6:
            # Heuristic: accept only if token ids appear nearby
            pass

        # Common pattern: {"id": "...", "clobTokenIds": ["...","..."], "outcomes": ["Yes","No"]}
        if "clobTokenIds" in d and isinstance(d.get("clobTokenIds"), list):
            tids = [str(x) for x in d["clobTokenIds"] if str(x).strip()]
            if len(tids) >= 2:
                token_ids = tids
                # outcomes might be present too
                if isinstance(d.get("outcomes"), list):
                    outcomes = [str(x) for x in d["outcomes"]]
                elif isinstance(d.get("outcomeNames"), list):
                    outcomes = [str(x) for x in d["outcomeNames"]]

                # try to find market id near it
                if isinstance(d.get("id"), str):
                    market_id = d["id"]

                # If not, we’ll keep searching; but token_ids is the key output.

    if not token_ids or len(token_ids) < 2:
        raise RuntimeError("Failed to extract CLOB token IDs from event page")

    if not market_id:
        # If we couldn't confidently identify market id, return blank; we can still trade by token IDs.
        market_id = ""

    if not outcomes:
        outcomes = ["YES", "NO"]

    return market_id, token_ids, outcomes

async def fetch_market_from_slug(slug: str) -> Dict[str, str]:
    """
    Scrape https://polymarket.com/event/<slug> to extract market id and token ids.
    Returns dict with: market_id, yes_token_id, no_token_id, yes_label, no_label
    """
    slug = slug.split("?")[0].strip()
    url = f"https://polymarket.com/event/{slug}"

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        next_data = _extract_next_data_json(r.text)
        market_id, token_ids, labels = _dig_for_market_and_tokens(next_data)

    # Normalize to YES/NO ordering:
    # Many pages use ["Yes","No"]. If it's reversed, we detect via label.
    yes_idx = 0
    no_idx = 1
    if len(labels) >= 2:
        l0 = labels[0].strip().lower()
        l1 = labels[1].strip().lower()
        if l0 in ("no", "down", "false") and l1 in ("yes", "up", "true"):
            yes_idx, no_idx = 1, 0

    return {
        "slug": slug,
        "market_id": market_id,
        "yes_token_id": str(token_ids[yes_idx]),
        "no_token_id": str(token_ids[no_idx]),
        "yes_label": labels[yes_idx] if len(labels) > yes_idx else "YES",
        "no_label": labels[no_idx] if len(labels) > no_idx else "NO",
    }


# ----------------------------
# Market slug discovery
# ----------------------------

def _bucket_start(ts: int, window_sec: int) -> int:
    return (ts // window_sec) * window_sec

def _compute_candidate_slugs(asset: str, now_ts: int, window_sec: int, ahead: int, behind: int) -> List[str]:
    """
    For Polymarket crypto 15m markets, common slug patterns:
      btc-updown-15m-<ts_rounded>
    Generalize to:
      {asset_lower}-updown-{window_minutes}m-<ts_rounded>

    We generate candidates around current window to cover transitions.
    """
    asset_l = asset.lower()
    minutes = int(window_sec / 60)
    base = _bucket_start(now_ts, window_sec)

    # Try multiple timestamp anchors: start-of-window and end-of-window.
    # Some markets use ts = window start; some could be end. We attempt both.
    candidates: List[str] = []
    for off in range(-behind, ahead + 1):
        start_ts = base + off * window_sec
        end_ts = start_ts  # first attempt: start as in the bot you provided
        candidates.append(f"{asset_l}-updown-{minutes}m-{end_ts}")
        # second attempt: end-of-window anchor
        candidates.append(f"{asset_l}-updown-{minutes}m-{start_ts + window_sec}")

    # De-dupe preserving order
    seen = set()
    out: List[str] = []
    for s in candidates:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

async def _find_slug_via_gamma(settings: Settings, asset: str) -> Optional[str]:
    """
    Query Gamma API for open markets and pick the best matching slug for the asset/window.
    """
    minutes = int(settings.window_sec / 60)
    pattern = re.compile(rf"^{asset.lower()}-updown-{minutes}m-(\d+)$")
    url = f"{settings.gamma_host.rstrip('/')}/markets"

    params = {"closed": "false", "limit": 500}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.json()

        if not isinstance(data, list):
            return None

        now_ts = int(time.time())
        candidates: List[Tuple[int, str]] = []
        for m in data:
            slug = str((m.get("slug") or "")).strip()
            mo = pattern.match(slug)
            if not mo:
                continue
            ts = int(mo.group(1))
            # "Open" heuristics: we want one that hasn't ended yet.
            if now_ts < ts + settings.window_sec:
                candidates.append((ts, slug))

        # Fallback: if none pass open heuristic, take newest matching
        if not candidates:
            for m in data:
                slug = str((m.get("slug") or "")).strip()
                mo = pattern.match(slug)
                if mo:
                    candidates.append((int(mo.group(1)), slug))

        if not candidates:
            return None

        # Prefer open markets; else latest timestamp
        candidates.sort(key=lambda x: (x[0] + settings.window_sec > now_ts, x[0]), reverse=True)
        return candidates[0][1]
    except Exception as e:
        if settings.debug:
            logger.warning(f"Gamma lookup failed for {asset}: {e}")
        return None

async def _find_slug_by_computed_candidates(settings: Settings, asset: str) -> Optional[str]:
    """
    Try computed candidates and validate by scraping the event page (fast enough with limits).
    """
    now_ts = int(time.time())
    candidates = _compute_candidate_slugs(
        asset=asset,
        now_ts=now_ts,
        window_sec=settings.window_sec,
        ahead=settings.slug_scan_buckets_ahead,
        behind=settings.slug_scan_buckets_behind,
    )
    max_tries = min(settings.max_slug_lookups, len(candidates))
    for i in range(max_tries):
        slug = candidates[i]
        try:
            info = await fetch_market_from_slug(slug)
            # If it exists, prefer one that hasn't ended yet (anchor timestamp in slug)
            m = re.search(r"-(\d+)$", slug)
            if m:
                ts = int(m.group(1))
                if now_ts < ts + settings.window_sec:
                    return info["slug"]
            # else: still accept if it exists; keep searching for more "open" one
            return info["slug"]
        except Exception:
            continue
    return None

async def find_active_slug(settings: Settings, asset: str) -> Optional[str]:
    # First: gamma (cheap)
    slug = await _find_slug_via_gamma(settings, asset)
    if slug:
        return slug
    # Second: computed candidates (more expensive)
    return await _find_slug_by_computed_candidates(settings, asset)


# ----------------------------
# Order book helpers
# ----------------------------

def _best_ask(book: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    asks = book.get("asks") or []
    if not asks:
        return None
    # py-clob-client often returns list[{"price":"0.52","size":"123"}]
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
    """
    Returns the worst (highest) price you'd pay to fill size_needed by walking asks.
    """
    asks = book.get("asks") or []
    remaining = size_needed
    worst_price = None
    for lvl in asks:
        try:
            p = float(lvl["price"])
            s = float(lvl["size"])
        except Exception:
            continue
        if s <= 0:
            continue
        take = min(s, remaining)
        remaining -= take
        worst_price = p
        if remaining <= 1e-9:
            return worst_price
    return None

def _slippage_ok(best: float, worst: float, max_slip: float) -> bool:
    if best <= 0:
        return False
    slip = (worst - best) / best
    return slip <= max_slip


# ----------------------------
# Trading client (CLOB)
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

        # API creds: if provided, set; else derive (works for many setups)
        if self.s.api_key and self.s.api_secret and self.s.api_passphrase:
            c.set_api_creds(
                {
                    "apiKey": self.s.api_key,
                    "secret": self.s.api_secret,
                    "passphrase": self.s.api_passphrase,
                }
            )
            logger.info("Configured CLOB client using provided API creds.")
        else:
            logger.info("Deriving/creating API creds via private key...")
            derived = c.create_or_derive_api_creds()
            c.set_api_creds(derived)
            # Best effort log; do not print secrets
            try:
                logger.info(f"Derived API key: {getattr(derived, 'api_key', '')}")
            except Exception:
                pass

        try:
            logger.info(f"Wallet address: {c.get_address()}")
            if funder:
                logger.info(f"Funder: {funder}")
        except Exception:
            pass

        return c

    def get_order_book(self, token_id: str) -> Dict[str, Any]:
        return self.client.get_order_book(token_id)

    def place_limit_buy(self, token_id: str, size: float, price: float, tif: str) -> Dict[str, Any]:
        """
        tif: "FOK" (fill-or-kill) or "FAK" (fill-and-kill) depending on py-clob-client.
        We'll map to OrderType.
        """
        tif = tif.upper().strip()
        if tif not in ("FOK", "FAK", "GTC"):
            tif = "FOK"

        order_type = OrderType.FOK if tif == "FOK" else (OrderType.FAK if tif == "FAK" else OrderType.GTC)

        args = OrderArgs(
            price=price,
            size=size,
            side=BUY,
            token_id=token_id,
        )
        opts = PartialCreateOrderOptions(
            tif=order_type
        )
        return self.client.create_and_post_order(args, opts)

    def place_limit_sell(self, token_id: str, size: float, price: float, tif: str = "FAK") -> Dict[str, Any]:
        tif = tif.upper().strip()
        order_type = OrderType.FAK if tif == "FAK" else (OrderType.FOK if tif == "FOK" else OrderType.GTC)
        args = OrderArgs(price=price, size=size, side=SELL, token_id=token_id)
        opts = PartialCreateOrderOptions(tif=order_type)
        return self.client.create_and_post_order(args, opts)

    def place_two_orders_fast(self, o1: Dict[str, Any], o2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Batch post if supported by py-clob-client.
        Inputs are dicts with keys matching create order args.
        """
        # If batch API is unavailable, we will fall back in the bot logic.
        args = PostOrdersArgs(
            orders=[
                OrderArgs(price=o1["price"], size=o1["size"], side=o1["side"], token_id=o1["token_id"]),
                OrderArgs(price=o2["price"], size=o2["size"], side=o2["side"], token_id=o2["token_id"]),
            ],
            options=PartialCreateOrderOptions(
                tif=o1.get("tif", OrderType.FOK)
            ),
        )
        return self.client.post_orders(args)

    def cancel_order(self, order_id: str) -> None:
        try:
            self.client.cancel(order_id)
        except Exception:
            pass

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return self.client.get_order(order_id)


def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    # py-clob-client responses vary; handle both.
    for k in ("orderID", "orderId", "id"):
        if k in resp and resp[k]:
            return str(resp[k])
    # Sometimes response has {"data":{"orderID":...}}
    d = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(d, dict):
        for k in ("orderID", "orderId", "id"):
            if k in d and d[k]:
                return str(d[k])
    return None

def _order_terminal_status(o: Dict[str, Any]) -> str:
    # Typical: "FILLED", "CANCELED", "REJECTED", "OPEN", "PARTIALLY_FILLED"
    for k in ("status", "state"):
        if k in o and o[k]:
            return str(o[k]).upper()
    return ""

def _order_filled_size(o: Dict[str, Any]) -> float:
    for k in ("filledSize", "filled_size", "sizeFilled", "filled"):
        if k in o and o[k] is not None:
            try:
                return float(o[k])
            except Exception:
                pass
    # Sometimes nested
    d = o.get("data")
    if isinstance(d, dict):
        for k in ("filledSize", "filled_size", "sizeFilled", "filled"):
            if k in d and d[k] is not None:
                try:
                    return float(d[k])
                except Exception:
                    pass
    return 0.0


# ----------------------------
# Bot core
# ----------------------------

class ArbBot:
    def __init__(self, s: Settings):
        self.s = s
        self.trader = PolymarketTrader(s)

        self._last_log_ts = 0.0
        self._trade_timestamps: List[float] = []  # for trades/min throttling
        self._last_trade_ts = 0.0

        # Active per asset cache
        self._active: Dict[str, Dict[str, str]] = {}  # asset -> {slug, yes_token_id, no_token_id, ...}

    def _throttle_trades_per_min(self) -> bool:
        now = time.time()
        cutoff = now - 60.0
        self._trade_timestamps = [t for t in self._trade_timestamps if t >= cutoff]
        return len(self._trade_timestamps) < self.s.max_trades_per_min

    def _cooldown_ok(self) -> bool:
        return (time.time() - self._last_trade_ts) >= self.s.cooldown_sec

    def _in_armed_window(self, now_ts: int) -> bool:
        """
        Only run strategy when within 'arm_before_boundary' window.
        """
        window_start = _bucket_start(now_ts, self.s.window_sec)
        boundary = window_start + self.s.window_sec
        tte = boundary - now_ts
        return tte <= self.s.arm_before_boundary_sec

    def _in_tradeable_tte_range(self, now_ts: int) -> bool:
        window_start = _bucket_start(now_ts, self.s.window_sec)
        boundary = window_start + self.s.window_sec
        tte = boundary - now_ts
        return self.s.min_tte_select_sec <= tte <= self.s.max_tte_select_sec

    def _stop_trying_near_boundary(self, now_ts: int) -> bool:
        window_start = _bucket_start(now_ts, self.s.window_sec)
        boundary = window_start + self.s.window_sec
        tte = boundary - now_ts
        return tte <= self.s.stop_trying_before_boundary_sec

    async def _refresh_asset_market(self, asset: str) -> Optional[Dict[str, str]]:
        """
        Find active slug + token IDs for an asset. Cache it.
        """
        slug = await find_active_slug(self.s, asset)
        if not slug:
            return None

        # If unchanged and already cached, keep it
        cached = self._active.get(asset)
        if cached and cached.get("slug") == slug:
            return cached

        info = await fetch_market_from_slug(slug)
        self._active[asset] = info
        logger.info(
            f"[{asset}] Active market: {slug} | YES={info['yes_token_id']} NO={info['no_token_id']}"
        )
        return info

    def _pick_notional_usdc(self) -> float:
        """
        Uses BASE_USDC/STEP_USDC to pick a notional in [MIN_USDC, MAX_USDC].
        """
        if self.s.step_usdc <= 0:
            return float(max(self.s.min_usdc, min(self.s.base_usdc, self.s.max_usdc)))
        steps = int(max(0, math.floor((self.s.max_usdc - self.s.min_usdc) / self.s.step_usdc)))
        if steps <= 0:
            return float(max(self.s.min_usdc, min(self.s.base_usdc, self.s.max_usdc)))
        k = random.randint(0, steps)
        val = self.s.min_usdc + k * self.s.step_usdc
        return float(max(self.s.min_usdc, min(val, self.s.max_usdc)))

    async def _fetch_books(self, yes_token_id: str, no_token_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Fetch in threads (py-clob-client is sync)
        t1 = asyncio.to_thread(self.trader.get_order_book, yes_token_id)
        t2 = asyncio.to_thread(self.trader.get_order_book, no_token_id)
        return await asyncio.gather(t1, t2)

    async def _wait_terminal(self, order_id: str, timeout_sec: float) -> Dict[str, Any]:
        deadline = time.time() + timeout_sec
        last = {}
        while time.time() < deadline:
            try:
                o = await asyncio.to_thread(self.trader.get_order, order_id)
                last = o
                st = _order_terminal_status(o)
                if st in ("FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
                    return o
            except Exception:
                pass
            await asyncio.sleep(0.15)
        return last

    async def _cancel_best_effort(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        await asyncio.to_thread(self.trader.cancel_order, order_id)

    async def _unwind_if_needed(self, token_id: str, filled_size: float, book: Dict[str, Any], reason: str) -> None:
        if filled_size <= 0:
            return
        bid = _best_bid(book)
        if not bid:
            logger.warning(f"Unwind skipped (no bids) | token={token_id} size={filled_size} | {reason}")
            return
        bid_price, _ = bid
        if self.s.dry_run:
            logger.warning(f"[DRY_RUN] Would unwind SELL {filled_size} @ {bid_price} | {reason}")
            return
        try:
            resp = await asyncio.to_thread(self.trader.place_limit_sell, token_id, filled_size, bid_price, "FAK")
            oid = _extract_order_id(resp)
            logger.warning(f"Unwind SELL placed | size={filled_size} price={bid_price} order={oid} | {reason}")
        except Exception as e:
            logger.warning(f"Unwind SELL failed | token={token_id} size={filled_size} | {reason} | err={e}")

    async def try_trade_asset(self, asset: str) -> None:
        now_ts = int(time.time())

        if not self._in_armed_window(now_ts):
            return
        if not self._in_tradeable_tte_range(now_ts):
            return
        if self._stop_trying_near_boundary(now_ts):
            return

        if not self._throttle_trades_per_min():
            return
        if not self._cooldown_ok():
            return

        market = await self._refresh_asset_market(asset)
        if not market:
            return

        yes_token = market["yes_token_id"]
        no_token = market["no_token_id"]

        # Fetch books
        try:
            yes_book, no_book = await self._fetch_books(yes_token, no_token)
        except Exception as e:
            logger.warning(f"[{asset}] Failed to fetch order books: {e}")
            return

        yes_best = _best_ask(yes_book)
        no_best = _best_ask(no_book)
        if not yes_best or not no_best:
            return

        yes_best_price, _ = yes_best
        no_best_price, _ = no_best

        # Pick sizing by USDC notional, convert to share size at WORST price (conservative)
        notional = self._pick_notional_usdc() * max(0.0, self.s.size_mult)
        notional = max(self.s.min_usdc, min(notional, self.s.max_usdc))

        # Start by estimating 1 share, then scale:
        # shares ~= notional / max(best_price) but we confirm with book-walk.
        est_price = max(yes_best_price, no_best_price, 0.01)
        shares = max(1.0, math.floor(notional / est_price))

        # Determine worst prices needed to fill shares
        yes_worst = _walk_asks_for_size(yes_book, shares)
        no_worst = _walk_asks_for_size(no_book, shares)
        if yes_worst is None or no_worst is None:
            return

        # Slippage guard
        if not _slippage_ok(yes_best_price, yes_worst, self.s.max_slippage):
            return
        if not _slippage_ok(no_best_price, no_worst, self.s.max_slippage):
            return

        total_cost = yes_worst + no_worst
        edge = 1.0 - total_cost

        # Edge guard
        if edge < self.s.min_edge:
            return

        # Periodic logging
        if time.time() - self._last_log_ts >= self.s.log_every_sec:
            self._last_log_ts = time.time()
            logger.info(
                f"[{asset}] edge={edge:.4f} total_cost={total_cost:.4f} "
                f"shares={shares:.0f} yes={yes_worst:.4f} no={no_worst:.4f} notional~{notional:.2f}"
            )

        # Execute paired orders
        await self._execute_paired(asset, yes_token, no_token, shares, yes_worst, no_worst)

    async def _execute_paired(self, asset: str, yes_token: str, no_token: str, shares: float, yes_price: float, no_price: float) -> None:
        if self.s.dry_run:
            logger.warning(
                f"[DRY_RUN] [{asset}] Would BUY paired | shares={shares:.0f} YES@{yes_price:.4f} NO@{no_price:.4f} | total={yes_price+no_price:.4f}"
            )
            return

        # Build orders
        tif = "FOK"  # deterministic: either fill immediately or cancel
        yes_order = {"token_id": yes_token, "side": BUY, "size": float(shares), "price": float(yes_price), "tif": OrderType.FOK}
        no_order = {"token_id": no_token, "side": BUY, "size": float(shares), "price": float(no_price), "tif": OrderType.FOK}

        yes_oid: Optional[str] = None
        no_oid: Optional[str] = None

        for attempt in range(1, self.s.max_retries + 1):
            try:
                # Best effort: batch post (fast); if fails, fallback to sequential
                try:
                    resp = await asyncio.to_thread(self.trader.place_two_orders_fast, yes_order, no_order)
                    # Batch response formats vary; try to extract order IDs
                    # Often: {"data":[{"orderID":..},{"orderID":..}]}
                    data = resp.get("data") if isinstance(resp, dict) else None
                    if isinstance(data, list) and len(data) >= 2:
                        yes_oid = _extract_order_id(data[0]) or yes_oid
                        no_oid = _extract_order_id(data[1]) or no_oid
                    if not yes_oid or not no_oid:
                        # Some APIs return "orderIDs"
                        if isinstance(resp, dict) and "orderIDs" in resp and isinstance(resp["orderIDs"], list):
                            ids = resp["orderIDs"]
                            if len(ids) >= 2:
                                yes_oid, no_oid = str(ids[0]), str(ids[1])
                except Exception:
                    # Fallback sequential
                    r1 = await asyncio.to_thread(self.trader.place_limit_buy, yes_token, shares, yes_price, tif)
                    yes_oid = _extract_order_id(r1)
                    r2 = await asyncio.to_thread(self.trader.place_limit_buy, no_token, shares, no_price, tif)
                    no_oid = _extract_order_id(r2)

                if not yes_oid or not no_oid:
                    raise RuntimeError("Failed to obtain order IDs after posting")

                # Wait for both to reach terminal state quickly
                y = await self._wait_terminal(yes_oid, self.s.fill_timeout_sec)
                n = await self._wait_terminal(no_oid, self.s.fill_timeout_sec)

                y_st = _order_terminal_status(y)
                n_st = _order_terminal_status(n)

                y_fill = _order_filled_size(y)
                n_fill = _order_filled_size(n)

                # Success condition: both fully filled (or at least filled to intended shares)
                if y_st == "FILLED" and n_st == "FILLED":
                    self._last_trade_ts = time.time()
                    self._trade_timestamps.append(self._last_trade_ts)
                    logger.info(f"[{asset}] ✅ Paired fill | shares={shares:.0f} | yes={yes_oid} no={no_oid}")
                    return

                # Any partial/one-leg fill -> cancel and unwind if configured
                await self._cancel_best_effort(yes_oid)
                await self._cancel_best_effort(no_oid)

                # Fetch books for unwind reference
                yes_book, no_book = await self._fetch_books(yes_token, no_token)

                # If only one side filled or partial, unwind that side
                if self.s.sell_opposite:
                    if y_fill > 0 and n_fill <= 0:
                        await self._unwind_if_needed(yes_token, y_fill, yes_book, reason=f"one-leg fill (YES). attempt={attempt}")
                    elif n_fill > 0 and y_fill <= 0:
                        await self._unwind_if_needed(no_token, n_fill, no_book, reason=f"one-leg fill (NO). attempt={attempt}")
                    else:
                        # Both partially filled; unwind both (risk-averse)
                        if y_fill > 0:
                            await self._unwind_if_needed(yes_token, y_fill, yes_book, reason=f"partial fill (YES). attempt={attempt}")
                        if n_fill > 0:
                            await self._unwind_if_needed(no_token, n_fill, no_book, reason=f"partial fill (NO). attempt={attempt}")

                logger.warning(
                    f"[{asset}] ⚠️ Paired not filled | YES({y_st},filled={y_fill}) NO({n_st},filled={n_fill}) | attempt={attempt}"
                )

                # brief backoff
                await asyncio.sleep(0.15)
                continue

            except Exception as e:
                logger.warning(f"[{asset}] Order attempt failed: {e} | attempt={attempt}")
                await asyncio.sleep(0.2)

        logger.warning(f"[{asset}] ❌ Giving up after {self.s.max_retries} attempts.")

    async def run_forever(self) -> None:
        logger.info("Bot started.")
        logger.info(f"Assets: {self.s.assets} | window={self.s.window_sec}s | dry_run={self.s.dry_run}")
        logger.info(f"Thresholds: min_edge={self.s.min_edge} max_slippage={self.s.max_slippage}")
        logger.info(f"Rate: poll={self.s.poll_sec}s max_trades_per_min={self.s.max_trades_per_min} cooldown={self.s.cooldown_sec}s")

        while True:
            start = time.time()
            try:
                # Each poll: try each asset (sequential to avoid hammering endpoints)
                for asset in self.s.assets:
                    await self.try_trade_asset(asset.upper())
            except Exception as e:
                logger.warning(f"Main loop error: {e}")

            elapsed = time.time() - start
            sleep_for = max(0.0, self.s.poll_sec - elapsed)
            await asyncio.sleep(sleep_for)


# ----------------------------
# Entrypoint
# ----------------------------

def _sanity_check_env(s: Settings) -> None:
    if not s.private_key:
        raise RuntimeError("Missing PM_PRIVATE_KEY / POLYMARKET_PRIVATE_KEY (required).")
    if s.signature_type not in (0, 1, 2):
        raise RuntimeError("PM_SIGNATURE_TYPE / POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2.")
    if not s.clob_host.startswith("http"):
        raise RuntimeError("CLOB_HOST looks invalid.")
    if not s.gamma_host.startswith("http"):
        raise RuntimeError("GAMMA_HOST looks invalid.")
    if s.min_usdc > s.max_usdc:
        raise RuntimeError("MIN_USDC must be <= MAX_USDC.")
    if s.window_sec <= 0:
        raise RuntimeError("WINDOW_SEC must be > 0.")

async def main() -> None:
    _setup_logging()
    s = Settings.load()
    _sanity_check_env(s)

    bot = ArbBot(s)
    await bot.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
