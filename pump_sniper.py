# pump_sniper.py
# Pump Sniper -> Micro-Scalper (A-mode: bot positions only)
# - PumpPortal WS (new tokens) + Dexscreener filters (candidates)
# - Jupiter swap/v1 for quotes + swaps (requires x-api-key)
# - Exit logic based on Jupiter SELL quotes (realistic PnL), not Dexscreener spot
#
# ENV REQUIRED:
#   RPC_URL
#   PRIVATE_KEY
#   JUP_API_KEY (or JUPITER_API_KEY)
#
# OPTIONAL / RECOMMENDED:
#   EXPECTED_WALLET (public address safety check)
#   SKIP_PREFLIGHT=true (many bots do this; reconcile confirms by balance)
#
# MICRO SETTINGS (defaults are conservative; tune for your style):
#   TP_PCT=0.006      # +0.6%
#   SL_PCT=0.004      # -0.4%
#   MAX_HOLD_SEC=180  # 3 minutes
#   MONITOR_POLL_SEC=2
#   MIN_SOL_BALANCE_SOL=0.02
#
# IMPORTANT:
# - Micro-scalping is extremely sensitive to fees/slippage/MEV.
# - You should start with tiny size until you see stable behavior.

import os
import json
import time
import base64
import binascii
import socket
import asyncio
from typing import Any, Dict, List, Optional, Tuple, Set

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
    Accepts:
      - base58 64-byte secret key
      - base58 32-byte seed
      - JSON array of ints (32/64)
      - base64 (32/64)
      - hex (32/64)
    """
    pk = _strip_wrappers(private_key_env)

    # JSON array
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

    # base58
    try:
        raw = base58.b58decode(pk)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # base64
    try:
        raw = base64.b64decode(pk, validate=True)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # hex
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
        "PRIVATE_KEY invalid. Provide a Solana secret (base58 64 bytes typical). "
        "Do NOT paste the public wallet address."
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

MIN_SOL_BALANCE_SOL = env_float("MIN_SOL_BALANCE_SOL", 0.02)

# Jupiter
JUP_BASE_URL = os.getenv("JUP_BASE_URL", "https://api.jup.ag").strip().rstrip("/")
JUP_API_KEY = (os.getenv("JUP_API_KEY") or os.getenv("JUPITER_API_KEY") or "").strip()
if not JUP_API_KEY:
    raise RuntimeError("Missing required env var: JUP_API_KEY (or JUPITER_API_KEY)")

# Trading / swap params
BUY_SOL = env_float("BUY_SOL", 0.01)
SLIPPAGE_BPS = env_int("SLIPPAGE_BPS", 150)
PRIORITY_FEE_MICRO_LAMPORTS = env_int("PRIORITY_FEE_MICRO_LAMPORTS", 8000)
SKIP_PREFLIGHT = env_bool("SKIP_PREFLIGHT", True)

# Micro strategy thresholds (PnL based on Jupiter sell quote)
TP_PCT = env_float("TP_PCT", 0.006)            # +0.6%
SL_PCT = env_float("SL_PCT", 0.004)            # -0.4%
MAX_HOLD_SEC = env_int("MAX_HOLD_SEC", 180)    # max holding time
MONITOR_POLL_SEC = env_float("MONITOR_POLL_SEC", 2.0)

# New launches filters
NEW_WARMUP_SEC = env_int("NEW_WARMUP_SEC", 75)
NEW_MIN_VOL_1H_USD = env_float("NEW_MIN_VOL_1H_USD", 500.0)
NEW_MIN_LIQ_USD = env_float("NEW_MIN_LIQ_USD", 12000.0)
NEW_MIN_BUYS_1H = env_int("NEW_MIN_BUYS_1H", 15)
NEW_REQUIRE_SOCIALS = env_bool("NEW_REQUIRE_SOCIALS", False)

# Existing movers filters
EXISTING_SCAN_EVERY_SEC = env_int("EXISTING_SCAN_EVERY_SEC", 12)
EXISTING_QUERIES = os.getenv("EXISTING_QUERIES", "raydium,solana,pump,SOL/USDC")
EXISTING_LIMIT_PER_QUERY = env_int("EXISTING_LIMIT_PER_QUERY", 80)
EXISTING_MIN_CHG_1H = env_float("EXISTING_MIN_CHG_1H", 3.0)
EXISTING_MIN_VOL_1H_USD = env_float("EXISTING_MIN_VOL_1H_USD", 8000.0)
EXISTING_MIN_LIQ_USD = env_float("EXISTING_MIN_LIQ_USD", 25000.0)
EXISTING_MIN_BUYS_1H = env_int("EXISTING_MIN_BUYS_1H", 40)

# Risk controls (bot positions only)
MAX_BUYS_PER_SCAN = env_int("MAX_BUYS_PER_SCAN", 1)
BUY_COOLDOWN_SEC = env_int("BUY_COOLDOWN_SEC", 15)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 5)
MAX_MONITORS = env_int("MAX_MONITORS", 20)

# Confirmation behavior
CONFIRM_BEFORE_BUY = env_bool("CONFIRM_BEFORE_BUY", False)  # for micro bots, reconcile is more reliable than status
CONFIRM_TIMEOUT_SEC = env_int("CONFIRM_TIMEOUT_SEC", 120)
CONFIRM_POLL_SEC = env_float("CONFIRM_POLL_SEC", 1.5)

LOG_SKIPS = env_bool("LOG_SKIPS", False)

SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL mint
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

# ===================== GLOBALS =====================

client = AsyncClient(RPC_URL)
wallet = parse_keypair(PRIVATE_KEY)
WALLET_PUBKEY = wallet.pubkey()

BUY_AMOUNT_LAMPORTS = int(BUY_SOL * 1e9)

# bot_positions: mint -> position info
# entry_in_lamports: lamports spent (from quote / or fallback)
# amount_raw: token amount held (raw)
# entry_ts: unix timestamp
bot_positions: Dict[str, Dict[str, Any]] = {}

# pending_buys: sig -> info (pre balances + quote)
pending_buys: Dict[str, Dict[str, Any]] = {}

# pending_sells: sig -> info
pending_sells: Dict[str, Dict[str, Any]] = {}

# monitors set to avoid duplicate tasks
monitored: Set[str] = set()

_last_buy_ts = 0.0

# ===================== JUP THROTTLE =====================

# Keep it slow to avoid rate limiting. If you need higher throughput, tune carefully.
JUP_MIN_INTERVAL_SEC = env_float("JUP_MIN_INTERVAL_SEC", 0.9)

_JUP_LOCK = asyncio.Lock()
_JUP_NEXT_TS = 0.0

async def jup_throttle() -> None:
    global _JUP_NEXT_TS
    async with _JUP_LOCK:
        now = time.time()
        if now < _JUP_NEXT_TS:
            await asyncio.sleep(_JUP_NEXT_TS - now)
        _JUP_NEXT_TS = time.time() + JUP_MIN_INTERVAL_SEC

def jup_headers() -> Dict[str, str]:
    return {
        "x-api-key": JUP_API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

# ===================== DEXSCREENER (DISCOVERY ONLY) =====================

def dexscreener_token_data(mint: str) -> Dict[str, Any]:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    data = http_get_json(url, timeout=8.0, retries=3)
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
    data = http_get_json(url, timeout=8.0, retries=3)
    pairs = data.get("pairs") or []
    return [p for p in pairs if p.get("chainId") == "solana"][:limit]

# ===================== BALANCES =====================

async def get_sol_balance_lamports(pubkey: Pubkey) -> int:
    resp = await client.get_balance(pubkey)
    return int(resp.value)

async def get_token_balance_raw(mint: str) -> int:
    if not is_valid_solana_mint(mint):
        return 0
    try:
        opts = TokenAccountOpts(
            mint=Pubkey.from_string(mint),
            program_id=TOKEN_PROGRAM_ID
        )
        resp = await client.get_token_accounts_by_owner(WALLET_PUBKEY, opts)
        if not resp or not getattr(resp, "value", None):
            return 0

        total = 0
        for item in resp.value:
            acct = item.pubkey
            bal = await client.get_token_account_balance(acct)
            total += int(bal.value.amount)  # type: ignore
        return total
    except Exception as e:
        log(f"Balance error for {mint}: {e}")
        return 0

# ===================== JUPITER QUOTES + SWAPS =====================

def build_quote_url(input_mint: str, output_mint: str, amount: int) -> str:
    return (
        f"{JUP_BASE_URL}/swap/v1/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={SLIPPAGE_BPS}"
        f"&restrictIntermediateTokens=true"
    )

async def jup_quote(input_mint: str, output_mint: str, amount: int) -> Dict[str, Any]:
    await jup_throttle()
    url = build_quote_url(input_mint, output_mint, amount)
    q = http_get_json(url, timeout=12.0, retries=3, headers=jup_headers())
    if isinstance(q, dict) and q.get("error"):
        raise RuntimeError(f"JUP quote error: {q.get('error')}")
    return q

async def jup_swap(quote: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "quoteResponse": quote,
        "userPublicKey": str(WALLET_PUBKEY),
        "wrapAndUnwrapSol": True,
    }
    if PRIORITY_FEE_MICRO_LAMPORTS > 0:
        payload["computeUnitPriceMicroLamports"] = int(PRIORITY_FEE_MICRO_LAMPORTS)

    await jup_throttle()
    return http_post_json(
        f"{JUP_BASE_URL}/swap/v1/swap",
        payload,
        timeout=20.0,
        retries=3,
        headers=jup_headers(),
    )

async def send_buy(mint: str) -> Optional[str]:
    try:
        # Pre balances for reconcile
        pre_sol = await get_sol_balance_lamports(WALLET_PUBKEY)
        pre_tok = await get_token_balance_raw(mint)

        quote = await jup_quote(SOL_MINT, mint, BUY_AMOUNT_LAMPORTS)
        swap = await jup_swap(quote)

        tx_b64 = swap.get("swapTransaction")
        if not tx_b64:
            log(f"BUY swap error mint={mint}: no swapTransaction")
            return None

        raw_tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
        signed_tx = VersionedTransaction(raw_tx.message, [wallet])

        opts = TxOpts(skip_preflight=SKIP_PREFLIGHT, preflight_commitment="processed")
        res = await client.send_transaction(signed_tx, opts=opts)
        sig = str(res.value)

        pending_buys[sig] = {
            "mint": mint,
            "sent_ts": time.time(),
            "pre_sol": int(pre_sol),
            "pre_tok": int(pre_tok),
            "quote_in": int(quote.get("inAmount") or BUY_AMOUNT_LAMPORTS),
            "quote_out": int(quote.get("outAmount") or 0),
        }

        log(f"BUY sent: https://solscan.io/tx/{sig} mint={mint} inLamports={pending_buys[sig]['quote_in']} expOutRaw={pending_buys[sig]['quote_out']}")
        return sig
    except Exception as e:
        log(f"BUY error mint={mint}: {e}")
        return None

async def send_sell(mint: str, amount_raw: Optional[int] = None) -> Optional[str]:
    try:
        bal = await get_token_balance_raw(mint)
        if bal <= 0:
            return None

        sell_amt = int(amount_raw) if amount_raw is not None else bal
        sell_amt = min(sell_amt, bal)
        if sell_amt <= 0:
            return None

        pre_tok = bal
        pre_sol = await get_sol_balance_lamports(WALLET_PUBKEY)

        quote = await jup_quote(mint, SOL_MINT, sell_amt)
        swap = await jup_swap(quote)

        tx_b64 = swap.get("swapTransaction")
        if not tx_b64:
            log(f"SELL swap error mint={mint}: no swapTransaction")
            return None

        raw_tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
        signed_tx = VersionedTransaction(raw_tx.message, [wallet])

        opts = TxOpts(skip_preflight=SKIP_PREFLIGHT, preflight_commitment="processed")
        res = await client.send_transaction(signed_tx, opts=opts)
        sig = str(res.value)

        pending_sells[sig] = {
            "mint": mint,
            "sent_ts": time.time(),
            "pre_tok": int(pre_tok),
            "pre_sol": int(pre_sol),
            "sell_amt": int(sell_amt),
            "quote_out": int(quote.get("outAmount") or 0),
        }

        log(f"SELL sent: https://solscan.io/tx/{sig} mint={mint} sellRaw={sell_amt} expOutLamports={pending_sells[sig]['quote_out']}")
        return sig
    except Exception as e:
        log(f"SELL error mint={mint}: {e}")
        return None

# ===================== RISK =====================

def can_buy_now() -> bool:
    global _last_buy_ts
    if len(bot_positions) >= MAX_OPEN_POSITIONS:
        return False
    if (time.time() - _last_buy_ts) < BUY_COOLDOWN_SEC:
        return False
    return True

def mark_bought() -> None:
    global _last_buy_ts
    _last_buy_ts = time.time()

# ===================== RECONCILE =====================

async def reconcile_loop() -> None:
    """
    Reconcile pending buys/sells by checking wallet balances.
    This is more reliable than signature status lookups under RPC lag.
    """
    while True:
        try:
            # Promote buys when token balance increases
            for sig, info in list(pending_buys.items()):
                mint = info["mint"]
                pre_tok = int(info["pre_tok"])
                pre_sol = int(info["pre_sol"])
                now_tok = await get_token_balance_raw(mint)

                if now_tok > pre_tok:
                    delta = now_tok - pre_tok
                    now_sol = await get_sol_balance_lamports(WALLET_PUBKEY)

                    # Prefer quote inAmount; fallback to SOL delta (includes fees)
                    entry_in = int(info.get("quote_in") or 0)
                    sol_spent = pre_sol - now_sol
                    if entry_in <= 0 and sol_spent > 0:
                        entry_in = sol_spent
                    if entry_in <= 0:
                        entry_in = BUY_AMOUNT_LAMPORTS

                    bot_positions[mint] = {
                        "mint": mint,
                        "amount_raw": int(delta),
                        "entry_in_lamports": int(entry_in),
                        "entry_ts": time.time(),
                        "last_quote_ts": 0.0,
                    }

                    log(f"RECONCILE BUY: mint={mint} receivedRaw={delta} entryInLamports={entry_in} sig={sig}")

                    pending_buys.pop(sig, None)
                    mark_bought()

                    # Start monitor
                    if mint not in monitored and len(monitored) < MAX_MONITORS:
                        monitored.add(mint)
                        asyncio.create_task(position_monitor(mint))

            # Clear sells when token balance drops
            for sig, info in list(pending_sells.items()):
                mint = info["mint"]
                pre_tok = int(info["pre_tok"])
                now_tok = await get_token_balance_raw(mint)

                # If balance dropped materially, treat as exited (simple and effective)
                if now_tok < pre_tok:
                    log(f"RECONCILE SELL: mint={mint} preTok={pre_tok} nowTok={now_tok} sig={sig} -> clearing bot position")
                    bot_positions.pop(mint, None)
                    monitored.discard(mint)
                    pending_sells.pop(sig, None)

            # Expire stale pendings
            now = time.time()
            for sig, info in list(pending_buys.items()):
                if now - float(info.get("sent_ts", now)) > 15 * 60:
                    log(f"RECONCILE: expiring stale pending BUY sig={sig} mint={info.get('mint')}")
                    pending_buys.pop(sig, None)

            for sig, info in list(pending_sells.items()):
                if now - float(info.get("sent_ts", now)) > 15 * 60:
                    log(f"RECONCILE: expiring stale pending SELL sig={sig} mint={info.get('mint')}")
                    pending_sells.pop(sig, None)

        except Exception as e:
            log(f"Reconcile error: {e}")

        await asyncio.sleep(3)

# ===================== MONITOR (JUP PNL-BASED) =====================

async def position_monitor(mint: str) -> None:
    """
    Monitor bot position using Jupiter SELL quotes.
    Exits on:
      - pnl_pct >= TP_PCT
      - pnl_pct <= -SL_PCT
      - hold time >= MAX_HOLD_SEC
    """
    try:
        while True:
            await asyncio.sleep(MONITOR_POLL_SEC)

            pos = bot_positions.get(mint)
            if not pos:
                monitored.discard(mint)
                return

            entry_in = int(pos["entry_in_lamports"])
            amt_raw = int(pos["amount_raw"])
            entry_ts = float(pos["entry_ts"])

            # If you no longer hold tokens, clear
            bal = await get_token_balance_raw(mint)
            if bal <= 0:
                log(f"MONITOR: mint={mint} balance=0 -> clearing")
                bot_positions.pop(mint, None)
                monitored.discard(mint)
                return

            # Use current balance for realism (partial fills / airdrops)
            amt_to_quote = min(amt_raw, bal)
            if amt_to_quote <= 0:
                bot_positions.pop(mint, None)
                monitored.discard(mint)
                return

            # Get sell quote -> lamports out
            try:
                quote = await jup_quote(mint, SOL_MINT, amt_to_quote)
            except Exception as e:
                log(f"MONITOR quote error mint={mint}: {e}")
                continue

            out_lamports = int(quote.get("outAmount") or 0)
            if out_lamports <= 0 or entry_in <= 0:
                continue

            pnl = out_lamports - entry_in
            pnl_pct = pnl / entry_in

            age = time.time() - entry_ts

            # Logging (lightweight)
            log(f"PNL mint={mint} age={age:.0f}s in={entry_in} out={out_lamports} pnlLamports={pnl} pnlPct={pnl_pct*100:.2f}%")

            # Exit rules
            if pnl_pct >= TP_PCT:
                log(f"EXIT TP mint={mint} pnlPct={pnl_pct*100:.2f}% -> SELL")
                await send_sell(mint, amount_raw=amt_to_quote)
                return

            if pnl_pct <= -SL_PCT:
                log(f"EXIT SL mint={mint} pnlPct={pnl_pct*100:.2f}% -> SELL")
                await send_sell(mint, amount_raw=amt_to_quote)
                return

            if age >= MAX_HOLD_SEC:
                log(f"EXIT TIME mint={mint} age={age:.0f}s -> SELL")
                await send_sell(mint, amount_raw=amt_to_quote)
                return

    finally:
        monitored.discard(mint)

# ===================== EXISTING MOVERS (DISCOVERY) =====================

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
        skipped_already_bot = 0

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
                    if mint not in seen:
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
                chg = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                b1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)
                px = float(p.get("priceUsd") or 0.0)

                candidates += 1
                log(f"EXISTING candidate {mint} | chg1h={chg:.2f}% vol1h=${vol:.2f} liq=${liq:.2f} buys1h={b1h} px=${px:.10f}")

                if mint in bot_positions:
                    skipped_already_bot += 1
                    continue

                if not can_buy_now():
                    skipped_risk += 1
                    continue

                # A-mode: do NOT adopt holdings; but avoid buying if already holding (basic safety)
                bal = await get_token_balance_raw(mint)
                if bal > 0:
                    if LOG_SKIPS:
                        log(f"SKIP holding mint={mint} bal={bal}")
                    continue

                sig = await send_buy(mint)
                if not sig:
                    continue

                buys += 1

        except Exception as e:
            log(f"ExistingScan error: {e}")

        log(
            f"ExistingScan cycle={cycle} scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(invalid={skipped_invalid}, filters={skipped_filters}, already_bot={skipped_already_bot}, risk={skipped_risk}) "
            f"bot_positions={len(bot_positions)} pending_buys={len(pending_buys)}"
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

                    # Warmup to avoid dead launches
                    await asyncio.sleep(NEW_WARMUP_SEC)

                    td = dexscreener_token_data(mint)
                    vol1h = float(td.get("volume_h1") or 0.0)
                    liq = float(td.get("liquidity_usd") or 0.0)
                    buys1h = int(td.get("buys_h1") or 0)
                    px = float(td.get("priceUsd") or 0.0)

                    if vol1h < NEW_MIN_VOL_1H_USD or liq < NEW_MIN_LIQ_USD or buys1h < NEW_MIN_BUYS_1H or px <= 0:
                        continue

                    if mint in bot_positions:
                        continue
                    if not can_buy_now():
                        continue

                    bal = await get_token_balance_raw(mint)
                    if bal > 0:
                        continue

                    log(f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.10f} socials={socials_ok}")

                    await send_buy(mint)

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
            f"Heartbeat: bot_positions={len(bot_positions)} monitored={len(monitored)} "
            f"pending_buys={len(pending_buys)} pending_sells={len(pending_sells)} "
            f"TP_PCT={TP_PCT} SL_PCT={SL_PCT} MAX_HOLD_SEC={MAX_HOLD_SEC}"
        )

# ===================== MAIN =====================

async def main() -> None:
    log("BOOT CONFIG:")
    log(f"  WALLET={WALLET_PUBKEY}")
    log(f"  WS_URL={WS_URL}")
    log(f"  RPC_URL={RPC_URL}")
    log(f"  JUP_BASE_URL={JUP_BASE_URL}")
    log(f"  BUY_SOL={BUY_SOL} SLIPPAGE_BPS={SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={PRIORITY_FEE_MICRO_LAMPORTS} SKIP_PREFLIGHT={SKIP_PREFLIGHT}")
    log(f"  MICRO: TP_PCT={TP_PCT} SL_PCT={SL_PCT} MAX_HOLD_SEC={MAX_HOLD_SEC} MONITOR_POLL_SEC={MONITOR_POLL_SEC}")
    log(f"  NEW: warmup={NEW_WARMUP_SEC}s minVol1h={NEW_MIN_VOL_1H_USD} minLiq={NEW_MIN_LIQ_USD} minBuys1h={NEW_MIN_BUYS_1H} requireSocials={NEW_REQUIRE_SOCIALS}")
    log(f"  EXISTING: queries={EXISTING_QUERIES} limitPerQuery={EXISTING_LIMIT_PER_QUERY} scanEvery={EXISTING_SCAN_EVERY_SEC}s")
    log(f"           minChg1h={EXISTING_MIN_CHG_1H}% minVol1h={EXISTING_MIN_VOL_1H_USD} minLiq={EXISTING_MIN_LIQ_USD} minBuys1h={EXISTING_MIN_BUYS_1H}")
    log(f"  RISK: MAX_BUYS_PER_SCAN={MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}")
    log(f"  JUP_MIN_INTERVAL_SEC={JUP_MIN_INTERVAL_SEC}")

    if EXPECTED_WALLET and str(WALLET_PUBKEY) != EXPECTED_WALLET:
        raise RuntimeError(
            f"Wrong wallet loaded. EXPECTED_WALLET={EXPECTED_WALLET} but derived WALLET={WALLET_PUBKEY}. Fix PRIVATE_KEY env var."
        )

    sol_lamports = await get_sol_balance_lamports(WALLET_PUBKEY)
    sol = sol_lamports / 1e9
    log(f"  SOL_BALANCE={sol:.6f} SOL")
    if sol < MIN_SOL_BALANCE_SOL:
        raise RuntimeError(f"SOL balance too low: {sol:.6f} SOL. Need MIN_SOL_BALANCE_SOL={MIN_SOL_BALANCE_SOL:.6f} SOL")

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
