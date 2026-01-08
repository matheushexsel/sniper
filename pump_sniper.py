import asyncio
import json
import os
import time
import base64
import binascii
from typing import Any, Dict, List, Optional, Tuple, Set

import base58
import requests
import websockets

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction


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
    # Remove accidental "PRIVATE_KEY=" prefix if user pasted raw line
    if s.upper().startswith("PRIVATE_KEY="):
        s = s.split("=", 1)[1].strip()
    # Remove surrounding quotes
    if (len(s) >= 2) and ((s[0] == s[-1]) and s[0] in ("'", '"')):
        s = s[1:-1].strip()
    return s


def parse_keypair(private_key_env: str) -> Keypair:
    """
    Accepts common Solana secret formats:
      1) base58 64-byte secret key (most common)
      2) base58 32-byte seed (also common) -> Keypair.from_seed
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

    # 3) base64 decode (often ends with '=')
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
        # allow "0x" prefix
        hx = pk[2:] if pk.lower().startswith("0x") else pk
        raw = binascii.unhexlify(hx)
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except Exception:
        pass

    # If user mistakenly pasted a public key (wallet address), it will decode base58 to 32 bytes,
    # but it's NOT a seed. There's no reliable way to detect that, so we fail hard.
    raise RuntimeError(
        "PRIVATE_KEY format invalid. Use: base58 64-byte secret key OR base58 32-byte seed OR JSON array (32/64) OR base64/hex (32/64). "
        "Also ensure you did NOT paste the wallet address/public key."
    )


def http_get_json(url: str, timeout: float = 8.0, retries: int = 3) -> Dict[str, Any]:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "pump-sniper/1.0"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.4 * (2 ** i))
    raise RuntimeError(f"GET failed after {retries} retries: {url} err={last}")

def http_post_json(url: str, payload: Dict[str, Any], timeout: float = 12.0, retries: int = 3) -> Dict[str, Any]:
    last = None
    for i in range(retries):
        try:
            r = requests.post(url, json=payload, timeout=timeout, headers={"User-Agent": "pump-sniper/1.0"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.6 * (2 ** i))
    raise RuntimeError(f"POST failed after {retries} retries: {url} err={last}")

def is_valid_solana_mint(mint: str) -> bool:
    try:
        Pubkey.from_string(mint)
        return True
    except Exception:
        return False


# ===================== CONFIG (ENV VARS) =====================

RPC_URL = env_str("RPC_URL")
WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")

PRIVATE_KEY = env_str("PRIVATE_KEY")

# Trading
BUY_SOL = env_float("BUY_SOL", 0.01)
SLIPPAGE_BPS = env_int("SLIPPAGE_BPS", 150)  # 1.5%
PRIORITY_FEE_MICRO_LAMPORTS = env_int("PRIORITY_FEE_MICRO_LAMPORTS", 8000)  # 0 = none
SKIP_PREFLIGHT = env_bool("SKIP_PREFLIGHT", False)

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

# Risk controls
MAX_BUYS_PER_SCAN = env_int("MAX_BUYS_PER_SCAN", 1)
BUY_COOLDOWN_SEC = env_int("BUY_COOLDOWN_SEC", 20)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 5)

# Sell logic
TAKE_PROFIT_X = env_float("TAKE_PROFIT_X", 1.6)
STOP_LOSS_X = env_float("STOP_LOSS_X", 0.75)
PRICE_POLL_SEC = env_int("PRICE_POLL_SEC", 5)
MAX_MONITORS = env_int("MAX_MONITORS", 20)

# Confirmation
CONFIRM_BEFORE_BUY = env_bool("CONFIRM_BEFORE_BUY", True)
CONFIRM_TIMEOUT_SEC = env_int("CONFIRM_TIMEOUT_SEC", 120)
CONFIRM_POLL_SEC = env_float("CONFIRM_POLL_SEC", 1.5)

LOG_SKIPS = env_bool("LOG_SKIPS", False)

SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


# ===================== GLOBALS =====================

client = AsyncClient(RPC_URL)
wallet = parse_keypair(PRIVATE_KEY)
WALLET_PUBKEY = wallet.pubkey()

BUY_AMOUNT_LAMPORTS = int(BUY_SOL * 1e9)

positions: Dict[str, float] = {}
monitored: Set[str] = set()
_last_buy_ts = 0.0


# ===================== DEXSCREENER DATA =====================

def dexscreener_token_data(mint: str) -> Dict[str, Any]:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    data = http_get_json(url)
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
    data = http_get_json(url)
    pairs = data.get("pairs") or []
    out = [p for p in pairs if p.get("chainId") == "solana"]
    return out[:limit]


# ===================== BALANCE =====================

async def get_token_balance(mint: str) -> int:
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
        acct = resp.value[0].pubkey
        bal = await client.get_token_account_balance(acct)
        amt = int(bal.value.amount)  # type: ignore
        return amt
    except Exception as e:
        if "InvalidParams" in str(type(e)) or "InvalidParamsMessage" in str(type(e)):
            log(f"Balance RPC returned non-standard response for {mint}: {type(e)}")
            return 0
        log(f"Balance error for {mint}: {e}")
        return 0


# ===================== JUPITER SWAP =====================

def build_jupiter_quote_url(input_mint: str, output_mint: str, amount: int) -> str:
    return (
        "https://quote-api.jup.ag/v6/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={SLIPPAGE_BPS}"
    )

async def send_swap(action: str, mint: str) -> Optional[str]:
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

        quote_url = build_jupiter_quote_url(input_mint, output_mint, amount)

        try:
            quote = http_get_json(quote_url, timeout=10.0, retries=3)
        except Exception as e:
            log(f"Swap error ({action}) mint={mint}: quote_error={e}")
            return None

        if "error" in quote:
            log(f"Swap error ({action}) mint={mint}: quote_error={quote.get('error')}")
            return None

        swap_payload = {
            "quoteResponse": quote,
            "userPublicKey": str(WALLET_PUBKEY),
            "wrapAndUnwrapSol": True,
        }
        if PRIORITY_FEE_MICRO_LAMPORTS > 0:
            swap_payload["computeUnitPriceMicroLamports"] = int(PRIORITY_FEE_MICRO_LAMPORTS)

        try:
            swap = http_post_json("https://quote-api.jup.ag/v6/swap", swap_payload, timeout=15.0, retries=3)
        except Exception as e:
            log(f"Swap error ({action}) mint={mint}: swap_error={e}")
            return None

        tx_b64 = swap.get("swapTransaction")
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


async def wait_confirm(sig: str) -> bool:
    deadline = time.time() + CONFIRM_TIMEOUT_SEC
    while time.time() < deadline:
        try:
            st = await client.get_signature_statuses([sig], search_transaction_history=True)
            v = st.value[0] if st and getattr(st, "value", None) else None
            if v is not None:
                if getattr(v, "err", None):
                    return False
                cs = getattr(v, "confirmation_status", None)
                if cs in ("confirmed", "finalized"):
                    return True
                confs = getattr(v, "confirmations", None)
                if confs is None:
                    return True
        except Exception:
            pass
        await asyncio.sleep(CONFIRM_POLL_SEC)
    return False


# ===================== SELL MONITOR =====================

async def price_monitor(mint: str, entry_price: float) -> None:
    while True:
        await asyncio.sleep(PRICE_POLL_SEC)
        data = dexscreener_token_data(mint)
        px = float(data.get("priceUsd") or 0.0)
        if px <= 0:
            continue
        mult = px / entry_price if entry_price > 0 else 0.0
        if mult >= TAKE_PROFIT_X:
            log(f"TP hit mint={mint} entry={entry_price:.10f} now={px:.10f} x={mult:.2f} -> SELL")
            await send_swap("sell", mint)
            positions.pop(mint, None)
            monitored.discard(mint)
            return
        if mult <= STOP_LOSS_X:
            log(f"SL hit mint={mint} entry={entry_price:.10f} now={px:.10f} x={mult:.2f} -> SELL")
            await send_swap("sell", mint)
            positions.pop(mint, None)
            monitored.discard(mint)
            return


# ===================== RISK =====================

def can_buy_now() -> bool:
    global _last_buy_ts
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False
    if (time.time() - _last_buy_ts) < BUY_COOLDOWN_SEC:
        return False
    return True

def mark_bought() -> None:
    global _last_buy_ts
    _last_buy_ts = time.time()


# ===================== EXISTING MOVERS =====================

async def scan_existing_tokens() -> None:
    queries = [q.strip() for q in EXISTING_QUERIES.split(",") if q.strip()]
    cycle = 0

    while True:
        cycle += 1
        scanned = 0
        candidates = 0
        buys = 0

        skipped_chain = 0
        skipped_invalid = 0
        skipped_held = 0
        skipped_filters = 0
        skipped_confirm = 0
        skipped_cooldown = 0
        skipped_maxpos = 0

        seen_pairs: Dict[str, Dict[str, Any]] = {}

        try:
            for q in queries:
                pairs = dexscreener_search(q, EXISTING_LIMIT_PER_QUERY)
                for p in pairs:
                    scanned += 1
                    if p.get("chainId") != "solana":
                        skipped_chain += 1
                        continue

                    mint = (p.get("baseToken") or {}).get("address") or ""
                    if not is_valid_solana_mint(mint):
                        skipped_invalid += 1
                        continue

                    if mint in seen_pairs:
                        continue
                    seen_pairs[mint] = p

            ranked: List[Tuple[float, Dict[str, Any]]] = []
            for mint, p in seen_pairs.items():
                change1h = float((p.get("priceChange") or {}).get("h1") or 0.0)
                vol1h = float((p.get("volume") or {}).get("h1") or 0.0)
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                buys1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys") or 0)
                px = float(p.get("priceUsd") or 0.0)

                if change1h >= EXISTING_MIN_CHG_1H and vol1h >= EXISTING_MIN_VOL_1H_USD and liq >= EXISTING_MIN_LIQ_USD and buys1h >= EXISTING_MIN_BUYS_1H:
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

                if mint in monitored or mint in positions:
                    skipped_held += 1
                    continue

                if not can_buy_now():
                    if len(positions) >= MAX_OPEN_POSITIONS:
                        skipped_maxpos += 1
                    else:
                        skipped_cooldown += 1
                    continue

                if await get_token_balance(mint) > 0:
                    skipped_held += 1
                    monitored.add(mint)
                    continue

                sig = await send_swap("buy", mint)
                if not sig:
                    continue

                if CONFIRM_BEFORE_BUY:
                    ok = await wait_confirm(sig)
                    if not ok:
                        skipped_confirm += 1
                        log(f"BUY NOT confirmed for {mint}: sig={sig} reason=timeout waiting confirmation")
                        continue

                entry = px
                if entry <= 0:
                    td = dexscreener_token_data(mint)
                    entry = float(td.get("priceUsd") or 0.0)

                monitored.add(mint)
                mark_bought()
                buys += 1

                if entry > 0:
                    positions[mint] = entry
                    if len(positions) <= MAX_MONITORS:
                        asyncio.create_task(price_monitor(mint, entry))

        except Exception as e:
            log(f"ExistingScan error: {e}")

        log(
            "ExistingScan "
            f"cycle={cycle} scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(chain={skipped_chain}, invalid={skipped_invalid}, held={skipped_held}, "
            f"filters={skipped_filters}, confirm={skipped_confirm}, cooldown={skipped_cooldown}, maxpos={skipped_maxpos}) "
            f"thresholds(chg1h>={EXISTING_MIN_CHG_1H}, vol1h>={EXISTING_MIN_VOL_1H_USD}, liq>={EXISTING_MIN_LIQ_USD}, buys1h>={EXISTING_MIN_BUYS_1H})"
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
                    if not mint:
                        continue
                    if not is_valid_solana_mint(mint):
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

                    if mint in monitored or mint in positions:
                        continue
                    if not can_buy_now():
                        continue
                    if await get_token_balance(mint) > 0:
                        monitored.add(mint)
                        continue

                    log(f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.10f} socials={socials_ok}")

                    sig = await send_swap("buy", mint)
                    if not sig:
                        continue

                    if CONFIRM_BEFORE_BUY:
                        ok = await wait_confirm(sig)
                        if not ok:
                            log(f"NEW BUY NOT confirmed mint={mint} sig={sig} reason=timeout waiting confirmation")
                            continue

                    positions[mint] = px
                    monitored.add(mint)
                    mark_bought()
                    if len(positions) <= MAX_MONITORS:
                        asyncio.create_task(price_monitor(mint, px))

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
        log(f"Heartbeat: monitored={len(monitored)} open_positions={len(positions)}")


# ===================== MAIN =====================

async def main() -> None:
    log("BOOT CONFIG:")
    log(f"  WALLET={WALLET_PUBKEY}")
    log(f"  WS_URL={WS_URL}")
    log(f"  BUY_SOL={BUY_SOL} SLIPPAGE_BPS={SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={PRIORITY_FEE_MICRO_LAMPORTS} SKIP_PREFLIGHT={SKIP_PREFLIGHT}")
    log(f"  CONFIRM: enabled={CONFIRM_BEFORE_BUY} timeout={CONFIRM_TIMEOUT_SEC}s poll={CONFIRM_POLL_SEC}s")
    log(f"  NEW: warmup={NEW_WARMUP_SEC}s minVol1h={NEW_MIN_VOL_1H_USD} minLiq={NEW_MIN_LIQ_USD} minBuys1h={NEW_MIN_BUYS_1H} requireSocials={NEW_REQUIRE_SOCIALS}")
    log(f"  EXISTING: queries={EXISTING_QUERIES} limitPerQuery={EXISTING_LIMIT_PER_QUERY} scanEvery={EXISTING_SCAN_EVERY_SEC}s")
    log(f"           minChg1h={EXISTING_MIN_CHG_1H}% minVol1h={EXISTING_MIN_VOL_1H_USD} minLiq={EXISTING_MIN_LIQ_USD} minBuys1h={EXISTING_MIN_BUYS_1H}")
    log(f"  RISK: MAX_BUYS_PER_SCAN={MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}")
    log(f"  SELL: tp={TAKE_PROFIT_X}x sl={STOP_LOSS_X}x poll={PRICE_POLL_SEC}s maxMonitors={MAX_MONITORS}")
    log(f"  LOG_SKIPS={LOG_SKIPS}")

    asyncio.create_task(scan_existing_tokens())
    asyncio.create_task(heartbeat())
    await monitor_new_launches()


if __name__ == "__main__":
    asyncio.run(main())
