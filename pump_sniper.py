# pump_sniper.py
# Pump Sniper (Solana) - PumpPortal WS + DexScreener filters + Jupiter swap/v1
# Micro-scalp mode: TP/SL in percent + max hold seconds. Reconcile by on-chain balance.
#
# Phase 1 profitability tuning:
# - Higher-liquidity/volume universe to reduce spread/impact
# - Lower slippage + lower priority fee
# - Slightly larger notional
# - Per-mint cooldown to avoid re-trading the same mint rapidly (fee grinder)

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
    Accepts common Solana secret formats:
      1) base58 64-byte secret key
      2) base58 32-byte seed -> Keypair.from_seed
      3) JSON array of ints (len 64 or 32)
      4) base64 of 64/32 bytes
      5) hex string of 64/32 bytes
    """
    pk = _strip_wrappers(private_key_env)

    # 1) JSON array
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

    # 2) base58
    try:
        raw = base58.b58decode(pk)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # 3) base64
    try:
        raw = base64.b64decode(pk, validate=True)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # 4) hex
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
        "PRIVATE_KEY format invalid. Use base58 64-byte secret key OR base58 32-byte seed OR JSON array (32/64) OR base64/hex (32/64)."
    )


def is_valid_solana_mint(mint: str) -> bool:
    try:
        Pubkey.from_string(mint)
        return True
    except Exception:
        return False


# ===================== HTTP =====================

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pump-sniper/2.1"})

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
            time.sleep(0.4 * (2 ** i))
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
            time.sleep(0.6 * (2 ** i))
    raise RuntimeError(f"POST failed after {retries} retries: {url} err={last}")


# ===================== CONFIG =====================

RPC_URL = env_str("RPC_URL")
WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")

PRIVATE_KEY = env_str("PRIVATE_KEY")

# Wallet safety (optional)
EXPECTED_WALLET = (os.getenv("EXPECTED_WALLET") or "").strip()

# Jupiter
JUP_BASE_URL = os.getenv("JUP_BASE_URL", "https://api.jup.ag").strip().rstrip("/")
JUP_API_KEY = (os.getenv("JUP_API_KEY") or os.getenv("JUPITER_API_KEY") or "").strip()
if not JUP_API_KEY:
    raise RuntimeError("Missing required env var: JUP_API_KEY (or JUPITER_API_KEY)")

# Trading (Phase 1 defaults)
BUY_SOL = env_float("BUY_SOL", 0.03)
SLIPPAGE_BPS = env_int("SLIPPAGE_BPS", 80)  # 0.8%
PRIORITY_FEE_MICRO_LAMPORTS = env_int("PRIORITY_FEE_MICRO_LAMPORTS", 3000)
SKIP_PREFLIGHT = env_bool("SKIP_PREFLIGHT", True)

# Micro exits (percent-based) (Phase 1 defaults)
TP_PCT = env_float("TP_PCT", 0.012)        # 1.2%
SL_PCT = env_float("SL_PCT", 0.008)        # 0.8%
MAX_HOLD_SEC = env_int("MAX_HOLD_SEC", 75) # seconds
MONITOR_POLL_SEC = env_float("MONITOR_POLL_SEC", 1.5)

# Per-mint cooldown to avoid fee grinder
MINT_COOLDOWN_SEC = env_int("MINT_COOLDOWN_SEC", 300)  # 5 minutes

# New launches filters (Phase 1 tighter)
NEW_WARMUP_SEC = env_int("NEW_WARMUP_SEC", 90)
NEW_MIN_VOL_1H_USD = env_float("NEW_MIN_VOL_1H_USD", 1500.0)
NEW_MIN_LIQ_USD = env_float("NEW_MIN_LIQ_USD", 50000.0)
NEW_MIN_BUYS_1H = env_int("NEW_MIN_BUYS_1H", 40)
NEW_REQUIRE_SOCIALS = env_bool("NEW_REQUIRE_SOCIALS", True)

# Existing movers filters (Phase 1 tighter)
EXISTING_SCAN_EVERY_SEC = env_int("EXISTING_SCAN_EVERY_SEC", 15)
EXISTING_QUERIES = os.getenv("EXISTING_QUERIES", "raydium,solana,pump,SOL/USDC")
EXISTING_LIMIT_PER_QUERY = env_int("EXISTING_LIMIT_PER_QUERY", 60)
EXISTING_MIN_CHG_1H = env_float("EXISTING_MIN_CHG_1H", 2.0)
EXISTING_MIN_VOL_1H_USD = env_float("EXISTING_MIN_VOL_1H_USD", 50000.0)
EXISTING_MIN_LIQ_USD = env_float("EXISTING_MIN_LIQ_USD", 250000.0)
EXISTING_MIN_BUYS_1H = env_int("EXISTING_MIN_BUYS_1H", 120)

# Risk controls (Phase 1 defaults)
MAX_BUYS_PER_SCAN = env_int("MAX_BUYS_PER_SCAN", 1)
BUY_COOLDOWN_SEC = env_int("BUY_COOLDOWN_SEC", 15)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 2)

# Jupiter throttle (Phase 1 default)
JUP_MIN_INTERVAL_SEC = env_float("JUP_MIN_INTERVAL_SEC", 1.1)

# Reconcile (Phase 1 faster)
RECONCILE_SEC = env_int("RECONCILE_SEC", 8)

LOG_SKIPS = env_bool("LOG_SKIPS", False)

SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL mint
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


# ===================== GLOBALS =====================

client = AsyncClient(RPC_URL)
wallet = parse_keypair(PRIVATE_KEY)
WALLET_PUBKEY = wallet.pubkey()

if EXPECTED_WALLET and str(WALLET_PUBKEY) != EXPECTED_WALLET:
    raise RuntimeError(
        f"Wrong wallet loaded. EXPECTED_WALLET={EXPECTED_WALLET} but derived WALLET={WALLET_PUBKEY}. Fix PRIVATE_KEY env var."
    )

BUY_AMOUNT_LAMPORTS = int(BUY_SOL * 1e9)

# bot_positions: mint -> dict(entry_usd, opened_ts, size_raw_at_detect, last_px_usd)
bot_positions: Dict[str, Dict[str, Any]] = {}
monitored: Set[str] = set()

pending_buys: Dict[str, float] = {}   # mint -> started_ts
pending_sells: Dict[str, float] = {}  # mint -> started_ts

_last_buy_ts = 0.0
_pos_lock = asyncio.Lock()

# Per-mint last-trade timestamps (prevents re-buy churn)
_last_trade_ts: Dict[str, float] = {}  # mint -> epoch seconds


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
    out = [p for p in pairs if p.get("chainId") == "solana"]
    return out[:limit]


# ===================== BALANCES =====================

async def get_sol_balance() -> float:
    try:
        r = await client.get_balance(WALLET_PUBKEY)
        lamports = int(r.value) if r and getattr(r, "value", None) is not None else 0
        return lamports / 1e9
    except Exception:
        return 0.0

async def get_token_balance(mint: str) -> int:
    if not is_valid_solana_mint(mint):
        return 0
    try:
        opts = TokenAccountOpts(mint=Pubkey.from_string(mint), program_id=TOKEN_PROGRAM_ID)
        resp = await client.get_token_accounts_by_owner(WALLET_PUBKEY, opts)
        if not resp or not getattr(resp, "value", None):
            return 0
        total = 0
        for v in resp.value:
            bal = await client.get_token_account_balance(v.pubkey)
            total += int(bal.value.amount)  # type: ignore
        return total
    except Exception:
        return 0


# ===================== JUPITER swap/v1 =====================

def build_quote_url(input_mint: str, output_mint: str, amount: int) -> str:
    return (
        f"{JUP_BASE_URL}/swap/v1/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={SLIPPAGE_BPS}"
    )

async def send_swap(action: str, mint: str) -> Optional[str]:
    """
    Uses Jupiter Quote + Swap endpoint:
      GET  /swap/v1/quote
      POST /swap/v1/swap
    Signs and submits the returned VersionedTransaction.
    """
    try:
        if action == "buy":
            input_mint = SOL_MINT
            output_mint = mint
            amount = BUY_AMOUNT_LAMPORTS
        else:
            bal = await get_token_balance(mint)
            if bal <= 0:
                return None
            input_mint = mint
            output_mint = SOL_MINT
            amount = bal

        quote_url = build_quote_url(input_mint, output_mint, amount)

        await jup_throttle()
        try:
            quote = http_get_json(quote_url, timeout=12.0, retries=3, headers=jup_headers())
        except Exception as e:
            log(f"Swap error ({action}) mint={mint}: quote_error={e}")
            return None

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

        await jup_throttle()
        try:
            swap = http_post_json(
                f"{JUP_BASE_URL}/swap/v1/swap",
                swap_payload,
                timeout=20.0,
                retries=3,
                headers=jup_headers(),
            )
        except Exception as e:
            log(f"Swap error ({action}) mint={mint}: swap_error={e}")
            return None

        tx_b64 = swap.get("swapTransaction") if isinstance(swap, dict) else None
        if not tx_b64:
            log(f"Swap error ({action}) mint={mint}: no swapTransaction in response")
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


# ===================== RISK =====================

def can_buy_now() -> bool:
    global _last_buy_ts
    if (time.time() - _last_buy_ts) < BUY_COOLDOWN_SEC:
        return False
    return True

def mark_bought() -> None:
    global _last_buy_ts
    _last_buy_ts = time.time()


# ===================== POSITION TRACKING =====================

async def track_position(mint: str, entry_usd: float, opened_ts: float, size_raw: int) -> None:
    async with _pos_lock:
        bot_positions[mint] = {
            "entry_usd": float(entry_usd),
            "opened_ts": float(opened_ts),
            "size_raw": int(size_raw),
            "last_px_usd": float(entry_usd),
        }
        monitored.add(mint)

async def untrack_position(mint: str) -> None:
    async with _pos_lock:
        bot_positions.pop(mint, None)
        monitored.discard(mint)

async def get_bot_positions_snapshot() -> Dict[str, Dict[str, Any]]:
    async with _pos_lock:
        return dict(bot_positions)


# ===================== MONITOR / EXIT =====================

async def monitor_position(mint: str) -> None:
    """
    Exits:
      - TP: price >= entry*(1+TP_PCT)
      - SL: price <= entry*(1-SL_PCT)
      - TIME: held >= MAX_HOLD_SEC
    Uses DexScreener priceUsd as the signal.
    Executes SELL via Jupiter and relies on reconcile to confirm removal.
    """
    while True:
        await asyncio.sleep(MONITOR_POLL_SEC)

        snap = await get_bot_positions_snapshot()
        pos = snap.get(mint)
        if not pos:
            return  # already closed

        entry = float(pos.get("entry_usd") or 0.0)
        opened_ts = float(pos.get("opened_ts") or time.time())
        if entry <= 0:
            await untrack_position(mint)
            return

        td = dexscreener_token_data(mint)
        px = float(td.get("priceUsd") or 0.0)
        if px <= 0:
            continue

        now = time.time()
        held = now - opened_ts
        tp_level = entry * (1.0 + TP_PCT)
        sl_level = entry * (1.0 - SL_PCT)

        async with _pos_lock:
            if mint in bot_positions:
                bot_positions[mint]["last_px_usd"] = px

        reason = None
        if px >= tp_level:
            reason = f"TP hit entry={entry:.10f} now={px:.10f} (+{(px/entry-1)*100:.2f}%)"
        elif px <= sl_level:
            reason = f"SL hit entry={entry:.10f} now={px:.10f} ({(px/entry-1)*100:.2f}%)"
        elif held >= MAX_HOLD_SEC:
            reason = f"TIME exit held={held:.1f}s entry={entry:.10f} now={px:.10f} ({(px/entry-1)*100:.2f}%)"

        if not reason:
            continue

        if mint in pending_sells:
            continue

        pending_sells[mint] = time.time()
        log(f"EXIT -> SELL mint={mint} reason={reason}")
        sig = await send_swap("sell", mint)
        if sig:
            _last_trade_ts[mint] = time.time()
            log(f"SELL submitted mint={mint} sig={sig} (reconcile will confirm by balance)")
        else:
            log(f"SELL failed mint={mint} (will retry on next tick)")


# ===================== RECONCILE LOOP =====================

async def reconcile_loop() -> None:
    """
    Makes the bot truthful:
      - If we sent a BUY and balance shows up -> track position + start monitor
      - If we sent a SELL and balance is gone -> untrack position
      - If we hold tokens without tracking -> ignore (no surprise selling)
    """
    while True:
        await asyncio.sleep(RECONCILE_SEC)

        # confirm buys by balance
        for mint in list(pending_buys.keys()):
            bal = await get_token_balance(mint)
            if bal > 0:
                td = dexscreener_token_data(mint)
                entry = float(td.get("priceUsd") or 0.0)
                if entry <= 0:
                    entry = 0.000000001
                await track_position(mint, entry_usd=entry, opened_ts=time.time(), size_raw=bal)
                asyncio.create_task(monitor_position(mint))
                pending_buys.pop(mint, None)
                log(f"RECONCILE: BUY confirmed by balance mint={mint} bal={bal} entry={entry:.10f}")

        # confirm sells by balance
        for mint in list(pending_sells.keys()):
            bal = await get_token_balance(mint)
            if bal <= 0:
                await untrack_position(mint)
                pending_sells.pop(mint, None)
                log(f"RECONCILE: SELL confirmed by balance mint={mint} bal=0 -> closed")


# ===================== ENTRY (EXISTING + NEW) =====================

async def try_buy(mint: str, px_usd: float) -> None:
    # risk checks
    snap = await get_bot_positions_snapshot()
    if mint in snap or mint in pending_buys or mint in pending_sells:
        return
    if len(snap) >= MAX_OPEN_POSITIONS:
        return

    # per-mint cooldown: avoid fee grinder behavior
    last_t = _last_trade_ts.get(mint, 0.0)
    if (time.time() - last_t) < MINT_COOLDOWN_SEC:
        return

    if not can_buy_now():
        return

    pending_buys[mint] = time.time()
    sig = await send_swap("buy", mint)
    if sig:
        mark_bought()
        _last_trade_ts[mint] = time.time()
        log(f"BUY submitted mint={mint} px=${px_usd:.10f} sig={sig} (reconcile will confirm by balance)")
    else:
        pending_buys.pop(mint, None)


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

        seen_pairs: Dict[str, Dict[str, Any]] = {}

        try:
            for q in queries:
                pairs = dexscreener_search(q, EXISTING_LIMIT_PER_QUERY)
                for p in pairs:
                    scanned += 1
                    mint = (p.get("baseToken") or {}).get("address") or ""
                    if not is_valid_solana_mint(mint):
                        skipped_invalid += 1
                        continue
                    if mint in seen_pairs:
                        continue
                    seen_pairs[mint] = p

            ranked: List[Tuple[float, Dict[str, Any]]] = []
            for _mint, p in seen_pairs.items():
                change1h = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol1h = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                buys1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)

                if (
                    change1h >= EXISTING_MIN_CHG_1H and
                    vol1h >= EXISTING_MIN_VOL_1H_USD and
                    liq >= EXISTING_MIN_LIQ_USD and
                    buys1h >= EXISTING_MIN_BUYS_1H
                ):
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
                log(
                    f"EXISTING candidate {mint} | chg1h={change1h:.2f}% "
                    f"vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.10f}"
                )

                snap = await get_bot_positions_snapshot()
                if mint in snap or mint in pending_buys or mint in pending_sells:
                    skipped_tracked += 1
                    continue

                if len(snap) >= MAX_OPEN_POSITIONS or not can_buy_now():
                    skipped_risk += 1
                    continue

                # If already holding it, don't buy (avoid doubling into bags)
                bal = await get_token_balance(mint)
                if bal > 0:
                    skipped_tracked += 1
                    continue

                await try_buy(mint, px_usd=px)
                buys += 1

        except Exception as e:
            log(f"ExistingScan error: {e}")

        log(
            "ExistingScan "
            f"cycle={cycle} scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(invalid={skipped_invalid}, filters={skipped_filters}, risk={skipped_risk}, tracked={skipped_tracked}) "
            f"bot_positions={len((await get_bot_positions_snapshot()))} pending_buys={len(pending_buys)}"
        )

        await asyncio.sleep(EXISTING_SCAN_EVERY_SEC)


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

                    await asyncio.sleep(NEW_WARMUP_SEC)

                    td = dexscreener_token_data(mint)
                    vol1h = float(td.get("volume_h1") or 0.0)
                    liq = float(td.get("liquidity_usd") or 0.0)
                    buys1h = int(td.get("buys_h1") or 0)
                    px = float(td.get("priceUsd") or 0.0)

                    if vol1h < NEW_MIN_VOL_1H_USD or liq < NEW_MIN_LIQ_USD or buys1h < NEW_MIN_BUYS_1H or px <= 0:
                        continue

                    snap = await get_bot_positions_snapshot()
                    if mint in snap or mint in pending_buys or mint in pending_sells:
                        continue
                    if len(snap) >= MAX_OPEN_POSITIONS or not can_buy_now():
                        continue

                    # per-mint cooldown
                    last_t = _last_trade_ts.get(mint, 0.0)
                    if (time.time() - last_t) < MINT_COOLDOWN_SEC:
                        continue

                    bal = await get_token_balance(mint)
                    if bal > 0:
                        continue

                    log(
                        f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} "
                        f"buys1h={buys1h} px=${px:.10f} socials={socials_ok}"
                    )
                    await try_buy(mint, px_usd=px)

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
        snap = await get_bot_positions_snapshot()
        log(
            f"Heartbeat: bot_positions={len(snap)} monitored={len(monitored)} "
            f"pending_buys={len(pending_buys)} pending_sells={len(pending_sells)} "
            f"TP_PCT={TP_PCT} SL_PCT={SL_PCT} MAX_HOLD_SEC={MAX_HOLD_SEC} "
            f"MINT_COOLDOWN_SEC={MINT_COOLDOWN_SEC} "
            f"BUY_SOL={BUY_SOL} SLIPPAGE_BPS={SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={PRIORITY_FEE_MICRO_LAMPORTS}"
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
    log(f"  MICRO: TP_PCT={TP_PCT} SL_PCT={SL_PCT} MAX_HOLD_SEC={MAX_HOLD_SEC} MONITOR_POLL_SEC={MONITOR_POLL_SEC}")
    log(f"  MINT_COOLDOWN_SEC={MINT_COOLDOWN_SEC}")
    log(f"  NEW: warmup={NEW_WARMUP_SEC}s minVol1h={NEW_MIN_VOL_1H_USD} minLiq={NEW_MIN_LIQ_USD} minBuys1h={NEW_MIN_BUYS_1H} requireSocials={NEW_REQUIRE_SOCIALS}")
    log(f"  EXISTING: queries={EXISTING_QUERIES} limitPerQuery={EXISTING_LIMIT_PER_QUERY} scanEvery={EXISTING_SCAN_EVERY_SEC}s")
    log(f"           minChg1h={EXISTING_MIN_CHG_1H}% minVol1h={EXISTING_MIN_VOL_1H_USD} minLiq={EXISTING_MIN_LIQ_USD} minBuys1h={EXISTING_MIN_BUYS_1H}")
    log(f"  RISK: MAX_BUYS_PER_SCAN={MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}")
    log(f"  JUP_MIN_INTERVAL_SEC={JUP_MIN_INTERVAL_SEC}")
    log(f"  RECONCILE_SEC={RECONCILE_SEC}")
    log(f"  SOL_BALANCE={sol_bal:.6f} SOL")
    log(f"  LOG_SKIPS={LOG_SKIPS}")

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
