# main.py
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
    clob_host: str
    gamma_host: str

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
# Time helpers
# ----------------------------

def _bucket_start(ts: int, window_sec: int) -> int:
    return (ts // window_sec) * window_sec


# ----------------------------
# Gamma market resolution (authoritative token IDs)
# ----------------------------

def _extract_yes_no_from_gamma_record(m: Dict[str, Any]) -> Optional[Dict[str, str]]:
    tids = m.get("clobTokenIds")
    if not isinstance(tids, list) or len(tids) < 2:
        return None

    labels = m.get("outcomes") or m.get("outcomeNames") or ["Yes", "No"]
    if not isinstance(labels, list) or len(labels) < 2:
        labels = ["Yes", "No"]

    yes_idx, no_idx = 0, 1
    l0 = str(labels[0]).strip().lower()
    l1 = str(labels[1]).strip().lower()
    if l0 == "no" and l1 == "yes":
        yes_idx, no_idx = 1, 0

    return {
        "slug": str(m.get("slug") or "").strip(),
        "market_id": str(m.get("id") or ""),
        "yes_token_id": str(tids[yes_idx]),
        "no_token_id": str(tids[no_idx]),
        "yes_label": str(labels[yes_idx]),
        "no_label": str(labels[no_idx]),
    }


async def find_active_market_via_gamma(s: Settings, asset: str) -> Optional[Dict[str, str]]:
    minutes = int(s.window_sec / 60)
    pat = re.compile(rf"^{asset.lower()}-updown-{minutes}m-(\d+)$")
    url = f"{s.gamma_host.rstrip('/')}/markets"
    params = {"closed": "false", "limit": 500}

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = r.json()

    if not isinstance(data, list):
        return None

    now_ts = int(time.time())

    candidates: List[Tuple[int, Dict[str, Any]]] = []
    for m in data:
        slug = str(m.get("slug") or "").strip()
        mo = pat.match(slug)
        if not mo:
            continue
        ts = int(mo.group(1))
        # prefer current open window
        if now_ts < ts + s.window_sec:
            candidates.append((ts, m))

    if not candidates:
        # fallback: newest matching
        for m in data:
            slug = str(m.get("slug") or "").strip()
            mo = pat.match(slug)
            if mo:
                candidates.append((int(mo.group(1)), m))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)

    for _, rec in candidates:
        info = _extract_yes_no_from_gamma_record(rec)
        if info and info.get("yes_token_id") and info.get("no_token_id"):
            return info

    return None


# ----------------------------
# Orderbook helpers
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

        if self.s.api_key and self.s.api_secret and self.s.api_passphrase:
            c.set_api_creds(
                {"apiKey": self.s.api_key, "secret": self.s.api_secret, "passphrase": self.s.api_passphrase}
            )
            logger.info("Configured API creds from env.")
        else:
            derived = c.create_or_derive_api_creds()
            c.set_api_creds(derived)
            logger.info("Derived API creds via private key.")

        logger.info(f"Wallet address: {c.get_address()}")
        if funder:
            logger.info(f"Funder: {funder}")
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
        self._invalid_until: Dict[str, float] = {}
        self._last_trade = 0.0
        self._trade_ts: List[float] = []
        self._last_heartbeat = 0.0

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
        until = self._invalid_until.get(asset, 0.0)
        if time.time() < until:
            if self.s.debug:
                logger.info(f"[{asset}] market refresh blocked until {until:.0f}")
            return None

        try:
            info = await find_active_market_via_gamma(self.s, asset)
        except Exception as e:
            logger.warning(f"[{asset}] gamma resolution failed: {e}")
            return None

        if not info:
            logger.warning(f"[{asset}] gamma returned no active market (slug not found).")
            return None

        cached = self._active.get(asset)
        if cached and cached.get("slug") == info.get("slug") and cached.get("yes_token_id") == info.get("yes_token_id"):
            return cached

        self._active[asset] = info
        logger.info(f"[{asset}] Active: {info['slug']} | YES={info['yes_token_id']} NO={info['no_token_id']}")
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
        tte = self._tte(now_ts)
        armed = self._armed(now_ts)
        tradeable = self._tradeable(now_ts)

        if self.s.debug:
            if not armed or not tradeable:
                logger.info(f"[{asset}] skip gates | armed={armed} tradeable={tradeable} tte={tte}")

        if not armed or not tradeable:
            return
        if not self._trades_per_min_ok():
            if self.s.debug:
                logger.info(f"[{asset}] skip rate-limit | trades_last_min={len(self._trade_ts)}")
            return
        if not self._cooldown_ok():
            if self.s.debug:
                logger.info(f"[{asset}] skip cooldown")
            return

        market = await self._refresh_market(asset)
        if not market:
            return

        yes_token = market["yes_token_id"]
        no_token = market["no_token_id"]

        try:
            yes_book, no_book = await self._books(yes_token, no_token)
        except Exception as e:
            msg = str(e)
            if "No orderbook exists" in msg or "status_code=404" in msg:
                logger.warning(f"[{asset}] token IDs not orderbook-enabled; re-resolve in 10s | err={e}")
                self._active.pop(asset, None)
                self._invalid_until[asset] = time.time() + 10.0
                return
            logger.warning(f"[{asset}] book fetch failed: {e}")
            return

        yes_best = _best_ask(yes_book)
        no_best = _best_ask(no_book)
        if not yes_best or not no_best:
            if self.s.debug:
                logger.info(f"[{asset}] no top-of-book liquidity (asks empty).")
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
            if self.s.debug:
                logger.info(f"[{asset}] insufficient ask depth for shares={shares:.0f}")
            return

        if not _slippage_ok(yes_best_p, yes_worst, self.s.max_slippage):
            return
        if not _slippage_ok(no_best_p, no_worst, self.s.max_slippage):
            return

        total_cost = yes_worst + no_worst
        edge = 1.0 - total_cost
        if edge < self.s.min_edge:
            return

        logger.info(
            f"[{asset}] edge={edge:.4f} total={total_cost:.4f} shares={shares:.0f} "
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
