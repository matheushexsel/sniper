# pump_sniper.py
# Solana Micro-Scalper (Option A = NET profit per round-trip)
# - PumpPortal WS (new tokens) + DexScreener (existing movers)
# - Jupiter Swap API v1 endpoints (/swap/v1/quote + /swap/v1/swap)
# - Cents-based exits: TARGET_PROFIT_USD / MAX_LOSS_USD / MAX_HOLD_SEC
# - Pre-trade round-trip edge check (quote-driven) to avoid bag holds
# - No bags: optional AUTO_LIQUIDATE_ON_BOOT sells all non-SOL/USDC holdings at startup
#
# DISCLAIMER: This is a high-risk trading bot. You can lose SOL fast due to slippage,
# MEV, route failures, token taxes, freezes, and rug mechanics. Run small size first.

import os
import json
import time
import base64
import binascii
import socket
import asyncio
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse

import requests
import base58
import websockets
import urllib3.util.connection as urllib3_cn

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction


# Force IPv4 DNS resolution (helps in some cloud runtimes)
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET


# ===================== UTIL =====================

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

def env_str(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v not in (None, "") else float(default)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else int(default)

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _strip_wrappers(s: str) -> str:
    s = s.strip()
    if s.upper().startswith("PRIVATE_KEY="):
        s = s.split("=", 1)[1].strip()
    if (len(s) >= 2) and ((s[0] == s[-1]) and s[0] in ("'", '"')):
        s = s[1:-1].strip()
    return s


def parse_keypair(private_key_env: str) -> Keypair:
    """
    Accepts common Solana secret formats:
      1) base58 64-byte secret key
      2) base58 32-byte seed -> Keypair.from_seed
      3) JSON array of ints (len 64 or 32)
      4) base64 of 64/32 bytes
      5) hex string of 64/32 bytes
    """
    pk = _strip_wrappers(private_key_env)

    # 1) JSON array format
    try:
        arr = json.loads(pk)
        if isinstance(arr, list) and all(isinstance(x, int) for x in arr):
            raw = bytes(arr)
            if len(raw) == 64:
                return Keypair.from_bytes(raw)
            if len(raw) == 32:
                return Keypair.from_seed(raw)
    except Exception:
        pass

    # 2) base58 decode
    try:
        raw = base58.b58decode(pk)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # 3) base64 decode
    try:
        raw = base64.b64decode(pk, validate=True)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # 4) hex decode
    try:
        hx = pk[2:] if pk.lower().startswith("0x") else pk
        raw = binascii.unhexlify(hx)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    raise RuntimeError(
        "PRIVATE_KEY format invalid. Use: base58 64-byte secret key OR base58 32-byte seed OR JSON array (32/64) OR base64/hex (32/64). "
        "Also ensure you did NOT paste the wallet address/public key."
    )


def is_valid_solana_mint(mint: str) -> bool:
    try:
        Pubkey.from_string(mint)
        return True
    except Exception:
        return False


# ===================== HTTP =====================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pump-micro-scalper/1.0"})


def http_get_json(url: str, timeout: float = 10.0, retries: int = 3, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    last = None
    for i in range(retries):
        try:
            h = dict(SESSION.headers)
            if headers:
                h.update(headers)
            r = SESSION.get(url, timeout=timeout, headers=h)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.35 * (2 ** i))
    raise RuntimeError(f"GET failed after {retries} retries: {url} err={last}")

def http_post_json(url: str, payload: Dict[str, Any], timeout: float = 15.0, retries: int = 3, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    last = None
    for i in range(retries):
        try:
            h = dict(SESSION.headers)
            if headers:
                h.update(headers)
            r = SESSION.post(url, json=payload, timeout=timeout, headers=h)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"POST failed after {retries} retries: {url} err={last}")


# ===================== CONFIG =====================

RPC_URL = env_str("RPC_URL")
WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")

PRIVATE_KEY = env_str("PRIVATE_KEY")

EXPECTED_WALLET = (os.getenv("EXPECTED_WALLET") or "").strip()

# Jupiter
JUP_BASE_URL = os.getenv("JUP_BASE_URL", "https://api.jup.ag").strip().rstrip("/")
JUP_API_KEY = (os.getenv("JUP_API_KEY") or os.getenv("JUPITER_API_KEY") or "").strip()
if not JUP_API_KEY:
    raise RuntimeError("Missing required env var: JUP_API_KEY (or JUPITER_API_KEY)")

# Trading size / tx behavior
BUY_SOL = env_float("BUY_SOL", 0.01)
SLIPPAGE_BPS = env_int("SLIPPAGE_BPS", 150)  # 1.5%
PRIORITY_FEE_MICRO_LAMPORTS = env_int("PRIORITY_FEE_MICRO_LAMPORTS", 8000)
SKIP_PREFLIGHT = env_bool("SKIP_PREFLIGHT", True)

# Cents-based micro scalping (Option A = NET profit)
TARGET_PROFIT_USD = env_float("TARGET_PROFIT_USD", 0.03)  # 3 cents net
MAX_LOSS_USD = env_float("MAX_LOSS_USD", 0.03)            # 3 cents net loss cap
MAX_HOLD_SEC = env_int("MAX_HOLD_SEC", 90)                # hard time stop
MONITOR_POLL_SEC = env_float("MONITOR_POLL_SEC", 2.0)

# Pre-trade edge gating (quote-driven)
MIN_ROUNDTRIP_EDGE_USD = env_float("MIN_ROUNDTRIP_EDGE_USD", 0.02)  # must show >= 2c edge before buy
EDGE_BUFFER_USD = env_float("EDGE_BUFFER_USD", 0.02)                # extra safety buffer (slippage/fees/MEV)

# Risk controls
MAX_BUYS_PER_SCAN = env_int("MAX_BUYS_PER_SCAN", 1)
BUY_COOLDOWN_SEC = env_int("BUY_COOLDOWN_SEC", 10)  # for micro scalps, lower cooldown is normal
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 3)  # micro scalper should keep this small

# New launches filters
NEW_WARMUP_SEC = env_int("NEW_WARMUP_SEC", 75)
NEW_MIN_VOL_1H_USD = env_float("NEW_MIN_VOL_1H_USD", 1500.0)
NEW_MIN_LIQ_USD = env_float("NEW_MIN_LIQ_USD", 25000.0)
NEW_MIN_BUYS_1H = env_int("NEW_MIN_BUYS_1H", 30)
NEW_REQUIRE_SOCIALS = env_bool("NEW_REQUIRE_SOCIALS", False)

# Existing movers filters
EXISTING_SCAN_EVERY_SEC = env_int("EXISTING_SCAN_EVERY_SEC", 12)
EXISTING_QUERIES = os.getenv("EXISTING_QUERIES", "raydium,solana,pump,SOL/USDC")
EXISTING_LIMIT_PER_QUERY = env_int("EXISTING_LIMIT_PER_QUERY", 80)
EXISTING_MIN_CHG_1H = env_float("EXISTING_MIN_CHG_1H", 3.0)
EXISTING_MIN_VOL_1H_USD = env_float("EXISTING_MIN_VOL_1H_USD", 20000.0)
EXISTING_MIN_LIQ_USD = env_float("EXISTING_MIN_LIQ_USD", 50000.0)
EXISTING_MIN_BUYS_1H = env_int("EXISTING_MIN_BUYS_1H", 60)

# Operational
LOG_SKIPS = env_bool("LOG_SKIPS", True)
AUTO_LIQUIDATE_ON_BOOT = env_bool("AUTO_LIQUIDATE_ON_BOOT", True)

# Jupiter rate limit safety
JUP_MIN_INTERVAL_SEC = env_float("JUP_MIN_INTERVAL_SEC", 0.9)

SOL_MINT = "So11111111111111111111111111111111111111112"   # wSOL
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


# ===================== GLOBALS =====================

client = AsyncClient(RPC_URL)
wallet = parse_keypair(PRIVATE_KEY)
WALLET_PUBKEY = wallet.pubkey()

if EXPECTED_WALLET:
    if str(WALLET_PUBKEY) != EXPECTED_WALLET:
        raise RuntimeError(
            f"Wrong wallet loaded. EXPECTED_WALLET={EXPECTED_WALLET} but derived WALLET={WALLET_PUBKEY}. Fix PRIVATE_KEY env var."
        )

BUY_AMOUNT_LAMPORTS = int(BUY_SOL * 1e9)

# positions keyed by mint:
# {
#   "entry_lamports": int,    # estimated SOL-in cost (lamports)
#   "entry_ts": float,        # timestamp
# }
positions: Dict[str, Dict[str, Any]] = {}
monitored: Set[str] = set()
pending_buys: Dict[str, float] = {}   # mint -> ts
pending_sells: Dict[str, float] = {}  # mint -> ts
_last_buy_ts = 0.0


# ===================== JUP THROTTLE =====================

_JUP_LOCK = asyncio.Lock()
_JUP_NEXT_TS = 0.0

async def jup_throttle() -> None:
    global _JUP_NEXT_TS
    async with _JUP_LOCK:
        now = time.time()
        if now < _JUP_NEXT_TS:
            await asyncio.sleep(_JUP_NEXT_TS - now)
        _JUP_NEXT_TS = time.time() + float(JUP_MIN_INTERVAL_SEC)

def jup_headers() -> Dict[str, str]:
    return {"x-api-key": JUP_API_KEY}


# ===================== DEXSCREENER =====================

def dexscreener_token_data(mint: str) -> Dict[str, Any]:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    data = http_get_json(url, timeout=8.0, retries=2)
    pairs = data.get("pairs") or []
    best = None
    for p in pairs:
        if p.get("chainId") != "solana":
            continue
        liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
        if best is None or liq > float((best.get("liquidity") or {}).get("usd") or 0.0):
            best = p
    if not best:
        return {}

    def f(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    return {
        "priceUsd": f(best.get("priceUsd")),
        "volume_h1": f((best.get("volume") or {}).get("h1")),
        "liquidity_usd": f((best.get("liquidity") or {}).get("usd")),
        "buys_h1": int(((best.get("txns") or {}).get("h1") or {}).get("buys") or 0),
        "sells_h1": int(((best.get("txns") or {}).get("h1") or {}).get("sells") or 0),
        "change_h1": f((best.get("priceChange") or {}).get("h1")),
    }

def dexscreener_search(query: str, limit: int) -> List[Dict[str, Any]]:
    url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
    data = http_get_json(url, timeout=8.0, retries=2)
    pairs = data.get("pairs") or []
    out = [p for p in pairs if p.get("chainId") == "solana"]
    return out[:limit]


# ===================== BALANCES =====================

async def get_sol_balance() -> float:
    try:
        resp = await client.get_balance(WALLET_PUBKEY)
        lamports = int(resp.value) if resp and getattr(resp, "value", None) is not None else 0
        return lamports / 1e9
    except Exception:
        return 0.0

async def get_token_balance_raw(mint: str) -> int:
    if not is_valid_solana_mint(mint):
        return 0
    try:
        opts = TokenAccountOpts(mint=Pubkey.from_string(mint), program_id=TOKEN_PROGRAM_ID)
        resp = await client.get_token_accounts_by_owner(WALLET_PUBKEY, opts)
        if not resp or not getattr(resp, "value", None):
            return 0
        acct = resp.value[0].pubkey
        bal = await client.get_token_account_balance(acct)
        return int(bal.value.amount)  # type: ignore
    except Exception:
        return 0


# ===================== JUPITER SWAP V1 =====================

def jup_quote_url(input_mint: str, output_mint: str, amount: int) -> str:
    return (
        f"{JUP_BASE_URL}/swap/v1/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={SLIPPAGE_BPS}"
    )

def jup_swap_url() -> str:
    return f"{JUP_BASE_URL}/swap/v1/swap"

async def sol_price_usdc() -> float:
    # quote 1 SOL -> USDC
    await jup_throttle()
    q = http_get_json(jup_quote_url(SOL_MINT, USDC_MINT, 1_000_000_000), timeout=10, retries=2, headers=jup_headers())
    out = int(q.get("outAmount") or 0)
    return out / 1_000_000  # USDC decimals = 6

async def roundtrip_edge_usd(output_mint: str, in_lamports: int) -> float:
    # Simulate SOL->TOKEN then TOKEN->SOL using quoted outAmount, convert edge to USD using SOL->USDC quote
    sp = await sol_price_usdc()

    await jup_throttle()
    buy_q = http_get_json(jup_quote_url(SOL_MINT, output_mint, in_lamports), timeout=10, retries=2, headers=jup_headers())
    buy_out = int(buy_q.get("outAmount") or 0)
    if buy_out <= 0:
        return -999.0

    await jup_throttle()
    sell_q = http_get_json(jup_quote_url(output_mint, SOL_MINT, buy_out), timeout=10, retries=2, headers=jup_headers())
    sell_out = int(sell_q.get("outAmount") or 0)
    if sell_out <= 0:
        return -999.0

    edge_sol = (sell_out - in_lamports) / 1e9
    return edge_sol * sp

async def send_swap(action: str, mint: str) -> Optional[str]:
    """
    Execute swap via Jupiter swap/v1.
    - buy: SOL_MINT -> mint for BUY_AMOUNT_LAMPORTS
    - sell: mint -> SOL_MINT for full token balance
    """
    try:
        if action == "buy":
            input_mint = SOL_MINT
            output_mint = mint
            amount = BUY_AMOUNT_LAMPORTS
        else:
            bal = await get_token_balance_raw(mint)
            if bal <= 0:
                return None
            input_mint = mint
            output_mint = SOL_MINT
            amount = bal

        # Quote
        await jup_throttle()
        quote = http_get_json(jup_quote_url(input_mint, output_mint, amount), timeout=12.0, retries=2, headers=jup_headers())
        if isinstance(quote, dict) and quote.get("error"):
            log(f"Swap error ({action}) mint={mint}: quote_error={quote.get('error')}")
            return None

        swap_payload: Dict[str, Any] = {
            "quoteResponse": quote,
            "userPublicKey": str(WALLET_PUBKEY),
            "wrapAndUnwrapSol": True,
        }
        if PRIORITY_FEE_MICRO_LAMPORTS > 0:
            swap_payload["computeUnitPriceMicroLamports"] = int(PRIORITY_FEE_MICRO_LAMPORTS)

        # Swap build
        await jup_throttle()
        swap = http_post_json(jup_swap_url(), swap_payload, timeout=20.0, retries=2, headers=jup_headers())

        tx_b64 = swap.get("swapTransaction") if isinstance(swap, dict) else None
        if not tx_b64:
            log(f"Swap error ({action}) mint={mint}: no swapTransaction")
            return None

        raw_tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
        signed_tx = VersionedTransaction(raw_tx.message, [wallet])

        opts = TxOpts(skip_preflight=SKIP_PREFLIGHT, preflight_commitment="processed")
        res = await client.send_transaction(signed_tx, opts=opts)
        sig = str(res.value)
        log(f"{action.upper()} sent: https://solscan.io/tx/{sig}")
        return sig
    except Exception as e:
        log(f"Swap error ({action}) mint={mint}: {e}")
        return None


# ===================== RISK / STATE =====================

def can_buy_now() -> bool:
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False
    if (time.time() - _last_buy_ts) < BUY_COOLDOWN_SEC:
        return False
    return True

def mark_bought() -> None:
    global _last_buy_ts
    _last_buy_ts = time.time()


# ===================== POSITION MONITOR (CENTS-BASED) =====================

async def position_monitor(mint: str) -> None:
    try:
        while True:
            await asyncio.sleep(float(MONITOR_POLL_SEC))

            pos = positions.get(mint)
            if not pos:
                monitored.discard(mint)
                return

            bal = await get_token_balance_raw(mint)
            if bal <= 0:
                # position closed externally or drained
                positions.pop(mint, None)
                monitored.discard(mint)
                return

            entry_lamports = int(pos["entry_lamports"])
            entry_ts = float(pos["entry_ts"])
            age = time.time() - entry_ts

            # executable sell value (token -> SOL outAmount)
            sp = await sol_price_usdc()
            await jup_throttle()
            q = http_get_json(jup_quote_url(mint, SOL_MINT, bal), timeout=10, retries=2, headers=jup_headers())
            out_lamports = int(q.get("outAmount") or 0)
            if out_lamports <= 0:
                continue

            pnl_sol = (out_lamports - entry_lamports) / 1e9
            pnl_usd = pnl_sol * sp

            # Exit logic (Option A = net cents)
            if pnl_usd >= TARGET_PROFIT_USD:
                if mint not in pending_sells:
                    pending_sells[mint] = time.time()
                    log(f"EXIT TP mint={mint} pnl_usd={pnl_usd:.4f} age={age:.1f}s -> SELL")
                    await send_swap("sell", mint)
                return

            if pnl_usd <= -MAX_LOSS_USD:
                if mint not in pending_sells:
                    pending_sells[mint] = time.time()
                    log(f"EXIT SL mint={mint} pnl_usd={pnl_usd:.4f} age={age:.1f}s -> SELL")
                    await send_swap("sell", mint)
                return

            if age >= MAX_HOLD_SEC:
                if mint not in pending_sells:
                    pending_sells[mint] = time.time()
                    log(f"EXIT TIME mint={mint} pnl_usd={pnl_usd:.4f} age={age:.1f}s -> SELL")
                    await send_swap("sell", mint)
                return

            # (optional) periodic telemetry (keep it sparse)
            if LOG_SKIPS and int(time.time()) % 15 == 0:
                log(f"PNL mint={mint} bal={bal} outLamports={out_lamports} pnl_usd={pnl_usd:.4f} age={age:.1f}s")

    except Exception as e:
        log(f"Monitor error mint={mint}: {e}")
        monitored.discard(mint)


# ===================== RECONCILE (BALANCE-BASED, NO LIES) =====================

async def reconcile_loop() -> None:
    """
    Solana RPC / status lookups can lag or miss.
    For micro scalping, we treat "balance changed" as truth.
    - If pending buy mint has balance > 0 => create position + start monitor
    - If position mint has balance == 0 => close position
    """
    while True:
        await asyncio.sleep(2.0)

        # Promote pending buys to positions if they landed
        for mint in list(pending_buys.keys()):
            bal = await get_token_balance_raw(mint)
            if bal > 0:
                if mint not in positions:
                    positions[mint] = {
                        "entry_lamports": int(BUY_AMOUNT_LAMPORTS),  # conservative: assume full spend
                        "entry_ts": time.time(),
                    }
                    log(f"RECONCILE: BUY landed mint={mint} bal={bal} -> tracking position")
                    if mint not in monitored and len(monitored) < MAX_OPEN_POSITIONS:
                        monitored.add(mint)
                        asyncio.create_task(position_monitor(mint))
                pending_buys.pop(mint, None)

        # Clean pending sells once balance is gone
        for mint in list(pending_sells.keys()):
            bal = await get_token_balance_raw(mint)
            if bal <= 0:
                pending_sells.pop(mint, None)
                positions.pop(mint, None)
                monitored.discard(mint)
                log(f"RECONCILE: SELL done mint={mint} -> closed")


# ===================== NO-BAGS: LIQUIDATE ON BOOT =====================

def _mint_from_token_account(item: Any) -> Optional[str]:
    # best-effort extract mint from parsed token account response
    try:
        data = item.account.data
        parsed = getattr(data, "parsed", None)
        if parsed and isinstance(parsed, dict):
            info = parsed.get("info") or {}
            mint = info.get("mint")
            if mint and isinstance(mint, str):
                return mint
    except Exception:
        return None
    return None

async def liquidate_all_holdings_on_boot() -> None:
    if not AUTO_LIQUIDATE_ON_BOOT:
        return

    # scan SPL token accounts for any non-zero holdings and sell them
    try:
        resp = await client.get_token_accounts_by_owner(
            WALLET_PUBKEY,
            TokenAccountOpts(program_id=TOKEN_PROGRAM_ID)
        )
        items = resp.value if resp and getattr(resp, "value", None) else []
        sold = 0

        for item in items:
            mint = _mint_from_token_account(item)
            if not mint or not is_valid_solana_mint(mint):
                continue
            if mint in (SOL_MINT, USDC_MINT):
                continue

            # check balance
            try:
                acct = item.pubkey
                bal = await client.get_token_account_balance(acct)
                amt_raw = int(bal.value.amount)  # type: ignore
            except Exception:
                continue

            if amt_raw <= 0:
                continue

            log(f"BOOT LIQUIDATE: mint={mint} raw_bal={amt_raw} -> SELL")
            await send_swap("sell", mint)
            sold += 1
            await asyncio.sleep(0.2)

        if sold > 0:
            log(f"BOOT LIQUIDATE complete: attempted_sells={sold} (reconcile will confirm closures)")
        else:
            log("BOOT LIQUIDATE: nothing to sell")

    except Exception as e:
        log(f"BOOT LIQUIDATE error: {e}")


# ===================== ENTRY FLOW =====================

async def maybe_buy_mint(mint: str, px_usd: float, context: str) -> None:
    """
    Core entry gate for micro scalper:
    - must not already hold it
    - must not already be tracked/pending
    - must pass risk limits
    - must pass quote-based round-trip edge test
    """
    if not is_valid_solana_mint(mint):
        return
    if mint in positions or mint in pending_buys or mint in pending_sells:
        if LOG_SKIPS:
            log(f"SKIP already_tracked mint={mint} ctx={context}")
        return
    if not can_buy_now():
        if LOG_SKIPS:
            log(f"SKIP risk mint={mint} ctx={context} positions={len(positions)} cooldown={(time.time()-_last_buy_ts):.1f}s")
        return

    # do not scale into existing holdings (no bags)
    bal = await get_token_balance_raw(mint)
    if bal > 0:
        if LOG_SKIPS:
            log(f"SKIP holding mint={mint} bal={bal} ctx={context}")
        return

    # quote-driven roundtrip edge gating (this is the key for "few cents net" strategy)
    edge = await roundtrip_edge_usd(mint, BUY_AMOUNT_LAMPORTS)
    need = MIN_ROUNDTRIP_EDGE_USD + EDGE_BUFFER_USD
    if edge < need:
        if LOG_SKIPS:
            log(f"SKIP no_edge mint={mint} edge_usd={edge:.4f} need>={need:.4f} ctx={context}")
        return

    sig = await send_swap("buy", mint)
    if not sig:
        return

    pending_buys[mint] = time.time()
    mark_bought()
    log(f"BUY queued mint={mint} edge_usd={edge:.4f} entry_px_usd={px_usd:.10f} ctx={context}")


# ===================== EXISTING MOVERS =====================

async def scan_existing_tokens() -> None:
    queries = [q.strip() for q in EXISTING_QUERIES.split(",") if q.strip()]
    cycle = 0

    while True:
        cycle += 1
        scanned = 0
        candidates = 0
        buys = 0
        skipped_invalid = 0
        skipped_filters = 0
        skipped_risk = 0
        skipped_tracked = 0

        seen: Dict[str, Dict[str, Any]] = {}

        try:
            for q in queries:
                pairs = dexscreener_search(q, EXISTING_LIMIT_PER_QUERY)
                for p in pairs:
                    scanned += 1
                    mint = (p.get("baseToken") or {}).get("address") or ""
                    if not is_valid_solana_mint(mint):
                        skipped_invalid += 1
                        continue
                    if mint in seen:
                        continue
                    seen[mint] = p

            ranked: List[Tuple[float, Dict[str, Any]]] = []
            for mint, p in seen.items():
                change1h = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol1h = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                buys1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)
                if (change1h >= EXISTING_MIN_CHG_1H and
                    vol1h >= EXISTING_MIN_VOL_1H_USD and
                    liq >= EXISTING_MIN_LIQ_USD and
                    buys1h >= EXISTING_MIN_BUYS_1H):
                    ranked.append((change1h, p))
                else:
                    skipped_filters += 1

            ranked.sort(key=lambda x: x[0], reverse=True)

            for _, p in ranked:
                if buys >= MAX_BUYS_PER_SCAN:
                    break
                mint = (p.get("baseToken") or {}).get("address") or ""
                change1h = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol1h = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                buys1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)
                px = float(p.get("priceUsd") or 0.0)

                candidates += 1
                log(f"EXISTING candidate {mint} | chg1h={change1h:.2f}% vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.10f}")

                if mint in positions or mint in pending_buys or mint in pending_sells:
                    skipped_tracked += 1
                    continue
                if not can_buy_now():
                    skipped_risk += 1
                    continue

                await maybe_buy_mint(mint, px, context="existing")
                if mint in pending_buys:
                    buys += 1

        except Exception as e:
            log(f"ExistingScan error: {e}")

        log(
            "ExistingScan "
            f"cycle={cycle} scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(invalid={skipped_invalid}, filters={skipped_filters}, risk={skipped_risk}, tracked={skipped_tracked}) "
            f"bot_positions={len(positions)} pending_buys={len(pending_buys)}"
        )

        await asyncio.sleep(EXISTING_SCAN_EVERY_SEC)


# ===================== NEW LAUNCHES (WS) =====================

async def monitor_new_launches() -> None:
    backoff = 5
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=30) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log("Connected to PumpPortal WS. Subscribed to new tokens.")
                backoff = 5

                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    mint = data.get("mint")
                    if not mint or not is_valid_solana_mint(mint):
                        continue

                    socials_ok = bool(data.get("twitter") or data.get("telegram") or data.get("website"))
                    if NEW_REQUIRE_SOCIALS and not socials_ok:
                        continue

                    # Warmup to let liquidity form
                    await asyncio.sleep(NEW_WARMUP_SEC)

                    td = dexscreener_token_data(mint)
                    vol1h = float(td.get("volume_h1") or 0.0)
                    liq = float(td.get("liquidity_usd") or 0.0)
                    buys1h = int(td.get("buys_h1") or 0)
                    px = float(td.get("priceUsd") or 0.0)

                    if px <= 0:
                        continue
                    if vol1h < NEW_MIN_VOL_1H_USD or liq < NEW_MIN_LIQ_USD or buys1h < NEW_MIN_BUYS_1H:
                        if LOG_SKIPS:
                            log(f"SKIP NEW filters mint={mint} vol1h={vol1h:.1f} liq={liq:.1f} buys1h={buys1h} px={px:.10f}")
                        continue

                    log(f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.10f} socials={socials_ok}")
                    await maybe_buy_mint(mint, px, context="new")

        except websockets.exceptions.ConnectionClosedError as e:
            log(f"WS closed: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log(f"WS error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ===================== HEARTBEAT =====================

async def heartbeat() -> None:
    while True:
        await asyncio.sleep(60)
        log(
            f"Heartbeat: bot_positions={len(positions)} monitored={len(monitored)} "
            f"pending_buys={len(pending_buys)} pending_sells={len(pending_sells)} "
            f"TARGET_PROFIT_USD={TARGET_PROFIT_USD} MAX_LOSS_USD={MAX_LOSS_USD} MAX_HOLD_SEC={MAX_HOLD_SEC}"
        )


# ===================== MAIN =====================

async def main() -> None:
    sol_bal = await get_sol_balance()

    log("BOOT CONFIG:")
    log(f"  WALLET={WALLET_PUBKEY}")
    log(f"  WS_URL={WS_URL}")
    log(f"  RPC_URL={RPC_URL}")
    log(f"  JUP_BASE_URL={JUP_BASE_URL}")
    log(f"  BUY_SOL={BUY_SOL} SLIPPAGE_BPS={SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={PRIORITY_FEE_MICRO_LAMPORTS} SKIP_PREFLIGHT={SKIP_PREFLIGHT}")
    log(f"  MICRO: TARGET_PROFIT_USD={TARGET_PROFIT_USD} MAX_LOSS_USD={MAX_LOSS_USD} MAX_HOLD_SEC={MAX_HOLD_SEC} MONITOR_POLL_SEC={MONITOR_POLL_SEC}")
    log(f"  EDGE: MIN_ROUNDTRIP_EDGE_USD={MIN_ROUNDTRIP_EDGE_USD} EDGE_BUFFER_USD={EDGE_BUFFER_USD}")
    log(f"  NEW: warmup={NEW_WARMUP_SEC}s minVol1h={NEW_MIN_VOL_1H_USD} minLiq={NEW_MIN_LIQ_USD} minBuys1h={NEW_MIN_BUYS_1H} requireSocials={NEW_REQUIRE_SOCIALS}")
    log(f"  EXISTING: queries={EXISTING_QUERIES} limitPerQuery={EXISTING_LIMIT_PER_QUERY} scanEvery={EXISTING_SCAN_EVERY_SEC}s")
    log(f"           minChg1h={EXISTING_MIN_CHG_1H}% minVol1h={EXISTING_MIN_VOL_1H_USD} minLiq={EXISTING_MIN_LIQ_USD} minBuys1h={EXISTING_MIN_BUYS_1H}")
    log(f"  RISK: MAX_BUYS_PER_SCAN={MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}")
    log(f"  JUP_MIN_INTERVAL_SEC={JUP_MIN_INTERVAL_SEC}")
    log(f"  LOG_SKIPS={LOG_SKIPS}")
    log(f"  AUTO_LIQUIDATE_ON_BOOT={AUTO_LIQUIDATE_ON_BOOT}")
    log(f"  SOL_BALANCE={sol_bal:.6f} SOL")

    # No bags: dump everything on boot (except SOL/USDC) so we only trade what this bot opens
    await liquidate_all_holdings_on_boot()

    asyncio.create_task(reconcile_loop())
    asyncio.create_task(scan_existing_tokens())
    asyncio.create_task(heartbeat())
    await monitor_new_launches()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(client.close())
            loop.close()
        except Exception:
            pass
