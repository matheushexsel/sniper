import os
import json
import time
import base64
import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Set, Tuple

import aiohttp
import websockets
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts, TokenAccountOpts

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pump_sniper")

# -----------------------------
# Constants
# -----------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={query}"

PUMPPORTAL_WS_URL_DEFAULT = "wss://pumpportal.fun/api/data"

# Jupiter Swap API v1 endpoints (requires x-api-key) :contentReference[oaicite:2]{index=2}
JUP_QUOTE_URL_DEFAULT = "https://api.jup.ag/swap/v1/quote"
JUP_SWAP_URL_DEFAULT = "https://api.jup.ag/swap/v1/swap"

# -----------------------------
# Helpers
# -----------------------------
def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

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

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def now_ts() -> float:
    return time.time()

def is_valid_solana_pubkey(s: str) -> bool:
    try:
        Pubkey.from_string(s)
        return True
    except Exception:
        return False

def lamports_from_sol(sol: float) -> int:
    return int(sol * 1_000_000_000)

# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    # Wallet/RPC
    PRIVATE_KEY_B58: str
    RPC_URL: str

    # PumpPortal WS
    WS_URL: str

    # Jupiter
    JUPITER_API_KEY: str
    JUP_QUOTE_URL: str
    JUP_SWAP_URL: str

    # Buy sizing + fees
    BUY_SOL: float
    SLIPPAGE_BPS: int
    PRIORITY_FEE_MICRO_LAMPORTS: int
    SKIP_PREFLIGHT: bool

    # Confirmation
    CONFIRM_TIMEOUT_SEC: int
    CONFIRM_POLL_SEC: float

    # NEW token strategy (post-warmup filters)
    NEW_WARMUP_SEC: int
    NEW_MIN_VOL_1H_USD: float
    NEW_MIN_LIQ_USD: float
    NEW_MIN_BUYS_1H: int
    NEW_REQUIRE_SOCIALS: bool

    # EXISTING token strategy
    EXISTING_SCAN_EVERY_SEC: int
    EXISTING_QUERIES: List[str]
    EXISTING_LIMIT_PER_QUERY: int
    EXISTING_MIN_CHG_1H_PCT: float
    EXISTING_MIN_VOL_1H_USD: float
    EXISTING_MIN_LIQ_USD: float
    EXISTING_MIN_BUYS_1H: int

    # Risk controls
    MAX_BUYS_PER_SCAN: int
    BUY_COOLDOWN_SEC: int
    MAX_OPEN_POSITIONS: int

    # Sell logic
    TAKE_PROFIT_X: float
    STOP_LOSS_X: float
    SELL_POLL_SEC: int

    # Logging
    LOG_SKIPS: bool


def load_config() -> Config:
    return Config(
        PRIVATE_KEY_B58=env_str("PRIVATE_KEY_B58", ""),
        RPC_URL=env_str("RPC_URL", ""),

        WS_URL=env_str("WS_URL", PUMPPORTAL_WS_URL_DEFAULT),

        JUPITER_API_KEY=env_str("JUPITER_API_KEY", ""),
        JUP_QUOTE_URL=env_str("JUP_QUOTE_URL", JUP_QUOTE_URL_DEFAULT),
        JUP_SWAP_URL=env_str("JUP_SWAP_URL", JUP_SWAP_URL_DEFAULT),

        BUY_SOL=env_float("BUY_SOL", 0.01),
        SLIPPAGE_BPS=env_int("SLIPPAGE_BPS", 150),
        PRIORITY_FEE_MICRO_LAMPORTS=env_int("PRIORITY_FEE_MICRO_LAMPORTS", 8000),
        SKIP_PREFLIGHT=env_bool("SKIP_PREFLIGHT", False),

        CONFIRM_TIMEOUT_SEC=env_int("CONFIRM_TIMEOUT_SEC", 120),
        CONFIRM_POLL_SEC=env_float("CONFIRM_POLL_SEC", 1.5),

        NEW_WARMUP_SEC=env_int("NEW_WARMUP_SEC", 75),
        NEW_MIN_VOL_1H_USD=env_float("NEW_MIN_VOL_1H_USD", 500.0),
        NEW_MIN_LIQ_USD=env_float("NEW_MIN_LIQ_USD", 12000.0),
        NEW_MIN_BUYS_1H=env_int("NEW_MIN_BUYS_1H", 15),
        NEW_REQUIRE_SOCIALS=env_bool("NEW_REQUIRE_SOCIALS", False),

        EXISTING_SCAN_EVERY_SEC=env_int("EXISTING_SCAN_EVERY_SEC", 12),
        EXISTING_QUERIES=[q.strip() for q in env_str("EXISTING_QUERIES", "raydium,solana,pump,SOL/USDC").split(",") if q.strip()],
        EXISTING_LIMIT_PER_QUERY=env_int("EXISTING_LIMIT_PER_QUERY", 80),
        EXISTING_MIN_CHG_1H_PCT=env_float("EXISTING_MIN_CHG_1H_PCT", 3.0),
        EXISTING_MIN_VOL_1H_USD=env_float("EXISTING_MIN_VOL_1H_USD", 8000.0),
        EXISTING_MIN_LIQ_USD=env_float("EXISTING_MIN_LIQ_USD", 25000.0),
        EXISTING_MIN_BUYS_1H=env_int("EXISTING_MIN_BUYS_1H", 40),

        MAX_BUYS_PER_SCAN=env_int("MAX_BUYS_PER_SCAN", 1),
        BUY_COOLDOWN_SEC=env_int("BUY_COOLDOWN_SEC", 20),
        MAX_OPEN_POSITIONS=env_int("MAX_OPEN_POSITIONS", 5),

        TAKE_PROFIT_X=env_float("TAKE_PROFIT_X", 1.6),
        STOP_LOSS_X=env_float("STOP_LOSS_X", 0.75),
        SELL_POLL_SEC=env_int("SELL_POLL_SEC", 5),

        LOG_SKIPS=env_bool("LOG_SKIPS", False),
    )

# -----------------------------
# HTTP client (retry/backoff)
# -----------------------------
class Http:
    def __init__(self):
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        await self.session.close()

    async def get_json(self, url: str, headers: Optional[Dict[str, str]] = None, retries: int = 3) -> Dict[str, Any]:
        delay = 0.6
        last_err = None
        for _ in range(retries):
            try:
                async with self.session.get(url, headers=headers) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        last_err = RuntimeError(f"HTTP {resp.status} GET {url} body={txt[:300]}")
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 4.0)
                        continue
                    return json.loads(txt)
            except Exception as e:
                last_err = e
                await asyncio.sleep(delay)
                delay = min(delay * 2, 4.0)
        raise RuntimeError(f"GET failed: {url} err={last_err}")

    async def post_json(self, url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None, retries: int = 3) -> Dict[str, Any]:
        delay = 0.6
        last_err = None
        for _ in range(retries):
            try:
                async with self.session.post(url, json=payload, headers=headers) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        last_err = RuntimeError(f"HTTP {resp.status} POST {url} body={txt[:300]}")
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 4.0)
                        continue
                    return json.loads(txt)
            except Exception as e:
                last_err = e
                await asyncio.sleep(delay)
                delay = min(delay * 2, 4.0)
        raise RuntimeError(f"POST failed: {url} err={last_err}")

# -----------------------------
# Trading + data
# -----------------------------
class Trader:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.wallet = Keypair.from_base58_string(cfg.PRIVATE_KEY_B58)
        self.wallet_pubkey = str(self.wallet.pubkey())
        self.client = AsyncClient(cfg.RPC_URL)
        self.http = Http()

        self.monitored: Dict[str, float] = {}  # mint -> entry priceUsd
        self.held_cache: Set[str] = set()      # mints we know we hold (best-effort)
        self.last_buy_ts: float = 0.0

        self.open_positions: Set[str] = set()

    async def close(self):
        await self.http.close()
        await self.client.close()

    async def get_token_balance_raw(self, mint: str) -> int:
        # Best-effort ATA balance check. If RPC returns odd shapes, treat as 0 (don’t crash).
        try:
            if not is_valid_solana_pubkey(mint):
                return 0
            opts = TokenAccountOpts(mint=Pubkey.from_string(mint), program_id=TOKEN_PROGRAM_ID)
            resp = await self.client.get_token_accounts_by_owner(self.wallet.pubkey(), opts)
            if not hasattr(resp, "value") or not resp.value:
                return 0
            acct = resp.value[0]
            bal = await self.client.get_token_account_balance(acct.pubkey)
            # bal.value.amount is string
            return int(bal.value.amount)
        except Exception:
            return 0

    async def get_dexscreener_token_data(self, mint: str) -> Dict[str, Any]:
        url = DEXSCREENER_TOKEN_URL.format(mint=mint)
        try:
            data = await self.http.get_json(url, retries=2)
        except Exception as e:
            return {"_err": str(e)}

        pairs = data.get("pairs") or []
        # Pick first Solana pair with best liquidity if possible
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return {}

        sol_pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0), reverse=True)
        p = sol_pairs[0]

        price_usd = float(p.get("priceUsd") or 0.0)
        vol_h1 = float((p.get("volume") or {}).get("h1") or 0.0)
        liq_usd = float((p.get("liquidity") or {}).get("usd") or 0.0)
        txns_h1 = (p.get("txns") or {}).get("h1") or {}
        buys_h1 = int(txns_h1.get("buys") or 0)

        price_chg_h1 = float((p.get("priceChange") or {}).get("h1") or 0.0)

        return {
            "mint": mint,
            "priceUsd": price_usd,
            "volume_h1": vol_h1,
            "liquidity_usd": liq_usd,
            "buys_h1": buys_h1,
            "priceChange_h1": price_chg_h1,
        }

    async def jupiter_quote(self, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
        # Jupiter Swap API v1 quoteouting endpoint, requires x-api-key :contentReference[oaicite:3]{index=3}
        params = (
            f"inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount}"
            f"&slippageBps={slippage_bps}"
            f"&swapMode=ExactIn"
        )
        url = f"{self.cfg.JUP_QUOTE_URL}?{params}"
        headers = {"x-api-key": self.cfg.JUPITER_API_KEY} if self.cfg.JUPITER_API_KEY else {}
        return await self.http.get_json(url, headers=headers, retries=3)

    async def jupiter_swap_tx(self, quote_resp: Dict[str, Any]) -> Dict[str, Any]:
        # Jupiter Swap API v1 swap endpoint, requires x-api-key :contentReference[oaicite:4]{index=4}
        payload = {
            "userPublicKey": self.wallet_pubkey,
            "quoteResponse": quote_resp,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
        }

        # Optional: prioritize
        if self.cfg.PRIORITY_FEE_MICRO_LAMPORTS > 0:
            payload["computeUnitPriceMicroLamports"] = int(self.cfg.PRIORITY_FEE_MICRO_LAMPORTS)

        headers = {"Content-Type": "application/json"}
        if self.cfg.JUPITER_API_KEY:
            headers["x-api-key"] = self.cfg.JUPITER_API_KEY

        return await self.http.post_json(self.cfg.JUP_SWAP_URL, payload, headers=headers, retries=3)

    def sign_versioned_tx(self, tx_b64: str) -> bytes:
        raw = base64.b64decode(tx_b64)
        tx = VersionedTransaction.from_bytes(raw)

        # Build message bytes without relying on .serialize() or .sign()
        try:
            msg_bytes = bytes(tx.message)
        except Exception:
            # Fallback
            msg_bytes = tx.message.to_bytes()

        sig = self.wallet.sign_message(msg_bytes)

        # Construct a new tx with our signature in slot 0
        signed = VersionedTransaction(tx.message, [sig])

        try:
            return bytes(signed)
        except Exception:
            return signed.to_bytes()

    async def send_and_confirm_raw(self, raw_tx: bytes) -> Tuple[str, bool]:
        opts = TxOpts(skip_preflight=self.cfg.SKIP_PREFLIGHT, preflight_commitment="processed")

        send = await self.client.send_raw_transaction(raw_tx, opts=opts)
        sig = str(send.value)

        # Confirm loop
        deadline = now_ts() + self.cfg.CONFIRM_TIMEOUT_SEC
        while now_ts() < deadline:
            st = await self.client.get_signature_statuses([sig])
            val = st.value[0] if st.value else None
            if val is not None:
                # err is None -> confirmed at some level
                if val.err is None and val.confirmation_status is not None:
                    return sig, True
                # err present means failed
                if val.err is not None:
                    return sig, False
            await asyncio.sleep(self.cfg.CONFIRM_POLL_SEC)

        return sig, False

    async def swap(self, action: str, mint: str) -> Optional[str]:
        """
        action: 'buy' (SOL -> mint) or 'sell' (mint -> SOL, sells full balance)
        """
        try:
            if not is_valid_solana_pubkey(mint):
                return None

            if action == "buy":
                in_mint = SOL_MINT
                out_mint = mint
                amount = lamports_from_sol(self.cfg.BUY_SOL)
            else:
                bal = await self.get_token_balance_raw(mint)
                if bal <= 0:
                    return None
                in_mint = mint
                out_mint = SOL_MINT
                amount = bal

            quote = await self.jupiter_quote(in_mint, out_mint, amount, self.cfg.SLIPPAGE_BPS)

            # Quote v1 returns routes under "routePlan" inside quoteResponse.
            # If routePlan empty, no route.
            route_plan = quote.get("routePlan") or []
            if not route_plan:
                log.warning(f"Swap error ({action}) mint={mint}: no route in quote")
                return None

            swap_resp = await self.jupiter_swap_tx(quote)

            tx_b64 = swap_resp.get("swapTransaction")
            if not tx_b64:
                log.warning(f"Swap error ({action}) mint={mint}: no swapTransaction in response")
                return None

            raw_signed = self.sign_versioned_tx(tx_b64)
            sig, ok = await self.send_and_confirm_raw(raw_signed)

            if ok:
                log.info(f"{action.upper()} confirmed: https://solscan.io/tx/{sig}")
            else:
                log.warning(f"{action.upper()} NOT confirmed: sig={sig} (timeout or error)")

            return sig
        except Exception as e:
            log.warning(f"Swap error ({action}) mint={mint}: {e}")
            return None

    # -----------------------------
    # Decision logic
    # -----------------------------
    def cooldown_ok(self) -> bool:
        return (now_ts() - self.last_buy_ts) >= self.cfg.BUY_COOLDOWN_SEC

    def positions_ok(self) -> bool:
        return len(self.open_positions) < self.cfg.MAX_OPEN_POSITIONS

    async def maybe_buy(self, mint: str, data: Dict[str, Any], reason: str) -> bool:
        if mint in self.open_positions:
            return False
        if not self.positions_ok():
            return False
        if not self.cooldown_ok():
            return False

        # Best-effort: avoid re-buying if already held
        bal = await self.get_token_balance_raw(mint)
        if bal > 0:
            self.open_positions.add(mint)
            return False

        entry = float(data.get("priceUsd") or 0.0)
        if entry <= 0:
            return False

        sig = await self.swap("buy", mint)
        self.last_buy_ts = now_ts()

        if sig:
            self.open_positions.add(mint)
            self.monitored[mint] = entry
            asyncio.create_task(self.price_monitor(mint, entry))
            log.info(f"BUY sent ({reason}) mint={mint} entry=${entry:.8f}")
            return True

        return False

    async def price_monitor(self, mint: str, entry_price: float):
        while True:
            await asyncio.sleep(self.cfg.SELL_POLL_SEC)
            d = await self.get_dexscreener_token_data(mint)
            px = float(d.get("priceUsd") or 0.0)
            if px <= 0:
                continue

            x = px / entry_price
            if x >= self.cfg.TAKE_PROFIT_X:
                # Sell everything (simple + robust)
                log.info(f"TP hit {x:.2f}x mint={mint} -> selling all")
                await self.swap("sell", mint)
                self.monitored.pop(mint, None)
                self.open_positions.discard(mint)
                return

            if x <= self.cfg.STOP_LOSS_X:
                log.info(f"SL hit {x:.2f}x mint={mint} -> selling all")
                await self.swap("sell", mint)
                self.monitored.pop(mint, None)
                self.open_positions.discard(mint)
                return

    # -----------------------------
    # Existing token scanning
    # -----------------------------
    async def scan_existing_once(self) -> None:
        scanned = 0
        candidates = 0
        buys = 0
        skipped_chain = 0
        skipped_invalid = 0
        skipped_held = 0
        skipped_filters = 0

        buy_budget = self.cfg.MAX_BUYS_PER_SCAN

        for q in self.cfg.EXISTING_QUERIES:
            if buy_budget <= 0:
                break

            url = DEXSCREENER_SEARCH_URL.format(query=q)
            try:
                res = await self.http.get_json(url, retries=2)
            except Exception:
                continue

            pairs = (res.get("pairs") or [])[: self.cfg.EXISTING_LIMIT_PER_QUERY]
            for p in pairs:
                if buy_budget <= 0:
                    break

                scanned += 1

                if p.get("chainId") != "solana":
                    skipped_chain += 1
                    continue

                mint = ((p.get("baseToken") or {}).get("address") or "").strip()
                if not mint or not is_valid_solana_pubkey(mint):
                    skipped_invalid += 1
                    continue

                if mint in self.open_positions:
                    skipped_held += 1
                    continue

                chg1h = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol1h = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                buys1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)
                px = float(p.get("priceUsd") or 0.0)

                # Strict momentum filters
                if not (chg1h >= self.cfg.EXISTING_MIN_CHG_1H_PCT and vol1h >= self.cfg.EXISTING_MIN_VOL_1H_USD and liq >= self.cfg.EXISTING_MIN_LIQ_USD and buys1h >= self.cfg.EXISTING_MIN_BUYS_1H and px > 0):
                    skipped_filters += 1
                    if self.cfg.LOG_SKIPS:
                        log.info(f"EXISTING skip mint={mint} chg1h={chg1h:.2f}% vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px={px:.8f}")
                    continue

                candidates += 1
                log.info(f"EXISTING candidate {mint} | chg1h={chg1h:.2f}% vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.8f}")

                data = {
                    "priceUsd": px,
                    "volume_h1": vol1h,
                    "liquidity_usd": liq,
                    "buys_h1": buys1h,
                    "priceChange_h1": chg1h,
                }

                did_buy = await self.maybe_buy(mint, data, reason="existing_momentum")
                if did_buy:
                    buys += 1
                    buy_budget -= 1

        log.info(
            f"ExistingScan scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(chain={skipped_chain}, invalid={skipped_invalid}, held={skipped_held}, filters={skipped_filters}) "
            f"thresholds(chg1h>={self.cfg.EXISTING_MIN_CHG_1H_PCT}, vol1h>={self.cfg.EXISTING_MIN_VOL_1H_USD}, liq>={self.cfg.EXISTING_MIN_LIQ_USD}, buys1h>={self.cfg.EXISTING_MIN_BUYS_1H})"
        )

    async def scan_existing_loop(self):
        cycle = 0
        while True:
            cycle += 1
            await self.scan_existing_once()
            await asyncio.sleep(self.cfg.EXISTING_SCAN_EVERY_SEC)

    # -----------------------------
    # New token WS
    # -----------------------------
    async def monitor_new_tokens(self):
        backoff = 5
        while True:
            try:
                async with websockets.connect(self.cfg.WS_URL, ping_interval=10, ping_timeout=30) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    log.info("Connected to PumpPortal WS. Subscribed to new tokens.")
                    backoff = 5

                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        mint = data.get("mint")
                        if not mint:
                            continue

                        if not is_valid_solana_pubkey(mint):
                            continue

                        # Optional socials
                        has_social = bool(data.get("twitter") or data.get("telegram") or data.get("website"))

                        # Warmup so Dexscreener has volume/liq
                        await asyncio.sleep(self.cfg.NEW_WARMUP_SEC)

                        d = await self.get_dexscreener_token_data(mint)
                        if not d:
                            continue

                        vol1h = float(d.get("volume_h1") or 0.0)
                        liq = float(d.get("liquidity_usd") or 0.0)
                        buys1h = int(d.get("buys_h1") or 0)
                        px = float(d.get("priceUsd") or 0.0)

                        if px <= 0:
                            continue

                        if self.cfg.NEW_REQUIRE_SOCIALS and not has_social:
                            if self.cfg.LOG_SKIPS:
                                log.info(f"NEW skip mint={mint}: no socials")
                            continue

                        if not (vol1h >= self.cfg.NEW_MIN_VOL_1H_USD and liq >= self.cfg.NEW_MIN_LIQ_USD and buys1h >= self.cfg.NEW_MIN_BUYS_1H):
                            if self.cfg.LOG_SKIPS:
                                log.info(f"NEW skip mint={mint} vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h}")
                            continue

                        log.info(f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.8f}")
                        await self.maybe_buy(mint, d, reason="new_launch")

            except Exception as e:
                log.warning(f"WS error: {e} reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(30)
            log.info(f"Heartbeat: monitored={len(self.monitored)} open_positions={len(self.open_positions)} wallet={self.wallet_pubkey}")

# -----------------------------
# Main
# -----------------------------
async def main():
    cfg = load_config()

    if not cfg.PRIVATE_KEY_B58:
        raise RuntimeError("Missing PRIVATE_KEY_B58 env var")
    if not cfg.RPC_URL:
        raise RuntimeError("Missing RPC_URL env var")
    if not cfg.JUPITER_API_KEY:
        raise RuntimeError("Missing JUPITER_API_KEY env var (required for api.jup.ag swap/v1)")  # :contentReference[oaicite:5]{index=5}

    log.info("BOOT CONFIG:")
    log.info(f"  WS_URL={cfg.WS_URL}")
    log.info(f"  RPC_URL set={'yes' if bool(cfg.RPC_URL) else 'no'}")
    log.info(f"  BUY_SOL={cfg.BUY_SOL} SLIPPAGE_BPS={cfg.SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={cfg.PRIORITY_FEE_MICRO_LAMPORTS} SKIP_PREFLIGHT={cfg.SKIP_PREFLIGHT}")
    log.info(f"  CONFIRM: timeout={cfg.CONFIRM_TIMEOUT_SEC}s poll={cfg.CONFIRM_POLL_SEC}s")
    log.info(f"  NEW: warmup={cfg.NEW_WARMUP_SEC}s minVol1h={cfg.NEW_MIN_VOL_1H_USD} minLiq={cfg.NEW_MIN_LIQ_USD} minBuys1h={cfg.NEW_MIN_BUYS_1H} requireSocials={cfg.NEW_REQUIRE_SOCIALS}")
    log.info(f"  EXISTING: queries={','.join(cfg.EXISTING_QUERIES)} limitPerQuery={cfg.EXISTING_LIMIT_PER_QUERY} scanEvery={cfg.EXISTING_SCAN_EVERY_SEC}s")
    log.info(f"           minChg1h={cfg.EXISTING_MIN_CHG_1H_PCT}% minVol1h={cfg.EXISTING_MIN_VOL_1H_USD} minLiq={cfg.EXISTING_MIN_LIQ_USD} minBuys1h={cfg.EXISTING_MIN_BUYS_1H}")
    log.info(f"  RISK: MAX_BUYS_PER_SCAN={cfg.MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={cfg.BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={cfg.MAX_OPEN_POSITIONS}")
    log.info(f"  SELL: tp={cfg.TAKE_PROFIT_X}x sl={cfg.STOP_LOSS_X}x poll={cfg.SELL_POLL_SEC}s")
    log.info(f"  LOG_SKIPS={cfg.LOG_SKIPS}")

    t = Trader(cfg)

    try:
        tasks = [
            asyncio.create_task(t.heartbeat()),
            asyncio.create_task(t.scan_existing_loop()),
            asyncio.create_task(t.monitor_new_tokens()),
        ]
        await asyncio.gather(*tasks)
    finally:
        await t.close()

if __name__ == "__main__":
    asyncio.run(main())
