import os
import asyncio
import json
import base64
import random
import time
from typing import Any, Dict, Optional, Tuple

import aiohttp
import websockets
import base58

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction


# ===================== ENV CONFIG =====================
# Required
PRIVATE_KEY_B58 = os.environ["PRIVATE_KEY"]          # base58-encoded 64-byte secret key
RPC_URL = os.environ["RPC_URL"]                      # Helius RPC URL with api-key

# Jupiter Swap API v1 (Jupiter docs show x-api-key required)
JUP_API_KEY = os.environ.get("JUP_API_KEY", "").strip()

# Optional runtime tuning
WS_URL = os.environ.get("WS_URL", "wss://pumpportal.fun/api/data")

BUY_SOL = float(os.environ.get("BUY_SOL", "0.01"))
SLIPPAGE_BPS = int(os.environ.get("SLIPPAGE_BPS", "50"))  # 50 = 0.5%
PRIORITY_FEE_MICRO_LAMPORTS = int(os.environ.get("PRIORITY_FEE_MICRO_LAMPORTS", "0"))
SKIP_PREFLIGHT = os.environ.get("SKIP_PREFLIGHT", "true").lower() == "true"

# New token filters (USD values from Dexscreener)
MIN_VOLUME_USD_NEW = float(os.environ.get("MIN_VOLUME_USD_NEW", "0"))
MIN_LIQUIDITY_USD_NEW = float(os.environ.get("MIN_LIQUIDITY_USD_NEW", "0"))
MIN_BUYS_H1_NEW = int(os.environ.get("MIN_BUYS_H1_NEW", "0"))
REQUIRE_SOCIALS = os.environ.get("REQUIRE_SOCIALS", "false").lower() == "true"
NEW_TOKEN_WARMUP_SEC = int(os.environ.get("NEW_TOKEN_WARMUP_SEC", "60"))

# Existing token scan filters
MIN_PRICE_CHANGE_1H = float(os.environ.get("MIN_PRICE_CHANGE_1H", "0"))  # percent
MIN_VOLUME_USD_EXISTING = float(os.environ.get("MIN_VOLUME_USD_EXISTING", "0"))
MIN_LIQUIDITY_USD_EXISTING = float(os.environ.get("MIN_LIQUIDITY_USD_EXISTING", "0"))
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "30"))

# Sell rules (multipliers vs entry price)
PROFIT_TARGET_X = float(os.environ.get("PROFIT_TARGET_X", "3.0"))  # sell-half trigger
STOP_LOSS_X = float(os.environ.get("STOP_LOSS_X", "0.5"))          # sell-all trigger
PRICE_POLL_SEC = int(os.environ.get("PRICE_POLL_SEC", "5"))

# Safety
MAX_CONCURRENT_MONITORS = int(os.environ.get("MAX_CONCURRENT_MONITORS", "25"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Constants
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_keypair_from_b58(b58_str: str) -> Keypair:
    raw = base58.b58decode(b58_str)
    # solders expects 64 bytes (secret + public)
    if len(raw) != 64:
        raise ValueError(f"PRIVATE_KEY must decode to 64 bytes, got {len(raw)} bytes")
    return Keypair.from_bytes(raw)


def lamports(sol: float) -> int:
    return int(sol * 1_000_000_000)


def is_valid_solana_pubkey(s: str) -> bool:
    try:
        Pubkey.from_string(s)
        return True
    except Exception:
        return False


def jup_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    # Jupiter docs show x-api-key required for /swap/v1/*
    if JUP_API_KEY:
        headers["x-api-key"] = JUP_API_KEY
    return headers


# ===================== HTTP HELPERS =====================

class HttpError(Exception):
    pass


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout_sec: int = 12,
    retries: int = 3,
    backoff_base: float = 1.5,
) -> Dict[str, Any]:
    headers = headers or {}

    for attempt in range(retries):
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_sec)
            if method.upper() == "GET":
                async with session.get(url, headers=headers, timeout=timeout) as r:
                    text = await r.text()
                    ct = (r.headers.get("Content-Type") or "").lower()

                    if r.status in (429, 500, 502, 503, 504):
                        raise HttpError(f"HTTP {r.status} transient: {text[:160]}")
                    if r.status != 200:
                        raise HttpError(f"HTTP {r.status}: {text[:160]}")
                    if "application/json" not in ct:
                        raise HttpError(f"Non-JSON ct={ct}: {text[:160]}")
                    return await r.json()

            async with session.post(url, headers=headers, json=payload or {}, timeout=timeout) as r:
                text = await r.text()
                ct = (r.headers.get("Content-Type") or "").lower()

                if r.status in (429, 500, 502, 503, 504):
                    raise HttpError(f"HTTP {r.status} transient: {text[:160]}")
                if r.status != 200:
                    raise HttpError(f"HTTP {r.status}: {text[:160]}")
                if "application/json" not in ct:
                    raise HttpError(f"Non-JSON ct={ct}: {text[:160]}")
                return await r.json()

        except Exception as e:
            if attempt == retries - 1:
                raise
            sleep_s = (backoff_base ** attempt) + random.random()
            log(f"HTTP retry {attempt+1}/{retries} after error: {e}. Sleeping {sleep_s:.2f}s")
            await asyncio.sleep(sleep_s)

    return {}


# ===================== DEXSCREENER =====================
# Use Solana-scoped endpoint to avoid EVM / bad addresses:
# https://api.dexscreener.com/token-pairs/v1/solana/{tokenAddress}
# (Dexscreener API reference lists token-pairs/v1 and rate limits) :contentReference[oaicite:2]{index=2}

async def get_token_data_dexscreener(
    session: aiohttp.ClientSession,
    mint: str
) -> Dict[str, Any]:
    url = f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}"
    try:
        pools = await fetch_json(session, url, method="GET", headers={"Accept": "application/json"}, retries=2)
        if not isinstance(pools, list) or not pools:
            return {}
        # Choose the first pool; you can improve selection later (highest liquidity, etc.)
        p = pools[0]
        return {
            "priceUsd": float(p.get("priceUsd", 0) or 0),
            "volume_h1": float((p.get("volume") or {}).get("h1", 0) or 0),
            "liquidity_usd": float((p.get("liquidity") or {}).get("usd", 0) or 0),
            "buys_h1": int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0),
            "change_h1": float((p.get("priceChange") or {}).get("h1", 0) or 0),
        }
    except Exception as e:
        log(f"Dexscreener token data error for {mint}: {e}")
        return {}


async def search_pairs_dexscreener(
    session: aiohttp.ClientSession,
    q: str,
    limit: int = 150
) -> list:
    url = f"https://api.dexscreener.com/latest/dex/search?q={q}"
    try:
        r = await fetch_json(session, url, method="GET", headers={"Accept": "application/json"}, retries=2)
        pairs = r.get("pairs") or []
        return pairs[:limit]
    except Exception as e:
        log(f"Dexscreener search error: {e}")
        return []


# ===================== SOLANA BALANCES =====================

async def get_token_balance(
    client: AsyncClient,
    owner: Pubkey,
    mint: str
) -> int:
    if not is_valid_solana_pubkey(mint):
        return 0
    try:
        opts = TokenAccountOpts(mint=Pubkey.from_string(mint), program_id=TOKEN_PROGRAM_ID)
        resp = await client.get_token_accounts_by_owner(owner, opts)
        if not resp.value:
            return 0
        # Pick first ATA
        ata = resp.value[0].pubkey
        bal = await client.get_token_account_balance(ata)
        return int(bal.value.amount)
    except Exception as e:
        log(f"Balance error for {mint}: {e}")
        return 0


# ===================== JUPITER SWAP (v1) =====================
# Docs show:
# GET https://api.jup.ag/swap/v1/quote with x-api-key :contentReference[oaicite:3]{index=3}
# POST https://api.jup.ag/swap/v1/swap with x-api-key required :contentReference[oaicite:4]{index=4}

async def jupiter_quote(
    session: aiohttp.ClientSession,
    input_mint: str,
    output_mint: str,
    amount: int
) -> Dict[str, Any]:
    url = (
        "https://api.jup.ag/swap/v1/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={SLIPPAGE_BPS}"
    )
    return await fetch_json(session, url, method="GET", headers=jup_headers(), retries=2)


async def jupiter_swap_tx(
    session: aiohttp.ClientSession,
    quote: Dict[str, Any],
    user_pubkey: str
) -> Dict[str, Any]:
    payload = {
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
    }
    # Optional: computeUnitPriceMicroLamports can be used for priority fee
    if PRIORITY_FEE_MICRO_LAMPORTS > 0:
        payload["computeUnitPriceMicroLamports"] = PRIORITY_FEE_MICRO_LAMPORTS

    url = "https://api.jup.ag/swap/v1/swap"
    return await fetch_json(session, url, method="POST", headers=jup_headers(), payload=payload, retries=2)


async def send_swap(
    session: aiohttp.ClientSession,
    client: AsyncClient,
    keypair: Keypair,
    action: str,
    mint: str,
    buy_lamports: int,
) -> Optional[str]:
    """
    action: "buy" or "sell"
    buy_lamports: used only for buy. sell uses full balance.
    Returns signature (string) on success.
    """
    try:
        owner = keypair.pubkey()
        if action == "buy":
            input_mint, output_mint, amount = SOL_MINT, mint, buy_lamports
        else:
            bal = await get_token_balance(client, owner, mint)
            if bal <= 0:
                log(f"No balance to sell for {mint}")
                return None
            input_mint, output_mint, amount = mint, SOL_MINT, bal

        quote = await jupiter_quote(session, input_mint, output_mint, amount)
        if not quote or quote.get("error"):
            log(f"Jupiter quote error for {mint}: {quote.get('error') if isinstance(quote, dict) else quote}")
            return None

        swap = await jupiter_swap_tx(session, quote, str(owner))
        swap_tx_b64 = swap.get("swapTransaction")
        if not swap_tx_b64:
            log(f"Jupiter swap response missing swapTransaction: {str(swap)[:220]}")
            return None

        tx_bytes = base64.b64decode(swap_tx_b64)
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Sign the transaction message
        msg_bytes = bytes(tx.message.serialize())
        sig = keypair.sign_message(msg_bytes)
        signed = VersionedTransaction(tx.message, [sig])

        # Send raw tx via RPC
        opts = TxOpts(skip_preflight=SKIP_PREFLIGHT, preflight_commitment="processed")
        resp = await client.send_raw_transaction(bytes(signed), opts=opts)
        signature = resp.value
        log(f"{action.upper()} sent: https://solscan.io/tx/{signature}")
        return signature

    except Exception as e:
        log(f"Swap error ({action}) for {mint}: {e}")
        return None


# ===================== STRATEGY LOOPS =====================

monitored_tokens: Dict[str, float] = {}  # mint -> entry price
monitor_semaphore: asyncio.Semaphore


async def price_monitor(
    session: aiohttp.ClientSession,
    client: AsyncClient,
    keypair: Keypair,
    mint: str,
    entry_price: float,
) -> None:
    async with monitor_semaphore:
        try:
            sold_half = False
            while True:
                await asyncio.sleep(PRICE_POLL_SEC)
                data = await get_token_data_dexscreener(session, mint)
                px = float(data.get("priceUsd", 0) or 0)
                if px <= 0 or entry_price <= 0:
                    continue

                mult = px / entry_price

                if (not sold_half) and mult >= PROFIT_TARGET_X:
                    log(f"PROFIT trigger {mult:.2f}x for {mint}. Selling FULL balance (simplified).")
                    # NOTE: implementing true 50% requires reading token decimals + splitting amount.
                    # Keep it deterministic: sell all on target.
                    await send_swap(session, client, keypair, "sell", mint, lamports(BUY_SOL))
                    monitored_tokens.pop(mint, None)
                    return

                if mult <= STOP_LOSS_X:
                    log(f"STOP-LOSS trigger {mult:.2f}x for {mint}. Selling FULL balance.")
                    await send_swap(session, client, keypair, "sell", mint, lamports(BUY_SOL))
                    monitored_tokens.pop(mint, None)
                    return

        except Exception as e:
            log(f"Price monitor error for {mint}: {e}")
            monitored_tokens.pop(mint, None)


async def scan_existing_tokens(
    session: aiohttp.ClientSession,
    client: AsyncClient,
    keypair: Keypair,
) -> None:
    owner = keypair.pubkey()
    while True:
        try:
            pairs = await search_pairs_dexscreener(session, q="SOL", limit=150)
            for p in pairs:
                if (p.get("chainId") or "").lower() != "solana":
                    continue

                base = p.get("baseToken") or {}
                mint = base.get("address") or ""
                if not mint or not is_valid_solana_pubkey(mint):
                    continue

                if mint in monitored_tokens:
                    continue

                # Skip if already held
                held = await get_token_balance(client, owner, mint)
                if held > 0:
                    continue

                change1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                vol1h = float((p.get("volume") or {}).get("h1", 0) or 0)
                liq = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                px = float(p.get("priceUsd", 0) or 0)

                if change1h >= MIN_PRICE_CHANGE_1H and vol1h >= MIN_VOLUME_USD_EXISTING and liq >= MIN_LIQUIDITY_USD_EXISTING:
                    log(f"EXISTING snipe candidate {mint} | chg1h={change1h:.2f}% vol1h=${vol1h:.2f} liq=${liq:.2f}")
                    sig = await send_swap(session, client, keypair, "buy", mint, lamports(BUY_SOL))
                    if sig and px > 0:
                        monitored_tokens[mint] = px
                        asyncio.create_task(price_monitor(session, client, keypair, mint, px))

        except Exception as e:
            log(f"Existing scan loop error: {e}")

        await asyncio.sleep(SCAN_INTERVAL_SEC)


async def monitor_new_launches(
    session: aiohttp.ClientSession,
    client: AsyncClient,
    keypair: Keypair,
) -> None:
    owner = keypair.pubkey()

    backoff = 5
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=10,
                ping_timeout=30,
                close_timeout=10,
                max_queue=1024,
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log("Connected to PumpPortal WS. Subscribed to new tokens.")
                backoff = 5

                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)

                    mint = data.get("mint")
                    if not mint:
                        continue

                    if not is_valid_solana_pubkey(mint):
                        continue

                    if mint in monitored_tokens:
                        continue

                    held = await get_token_balance(client, owner, mint)
                    if held > 0:
                        continue

                    # Optional social constraint (based on event payload)
                    has_social = bool(data.get("twitter") or data.get("telegram") or data.get("website"))

                    if REQUIRE_SOCIALS and not has_social:
                        continue

                    # Warmup to avoid Dex returning zeros right after mint
                    await asyncio.sleep(NEW_TOKEN_WARMUP_SEC)

                    tdata = await get_token_data_dexscreener(session, mint)
                    px = float(tdata.get("priceUsd", 0) or 0)
                    vol = float(tdata.get("volume_h1", 0) or 0)
                    liq = float(tdata.get("liquidity_usd", 0) or 0)
                    buys = int(tdata.get("buys_h1", 0) or 0)

                    if vol < MIN_VOLUME_USD_NEW or liq < MIN_LIQUIDITY_USD_NEW or buys < MIN_BUYS_H1_NEW:
                        continue

                    log(f"NEW snipe candidate {mint} | vol1h=${vol:.2f} liq=${liq:.2f} buys1h={buys} social={has_social}")
                    sig = await send_swap(session, client, keypair, "buy", mint, lamports(BUY_SOL))
                    if sig and px > 0:
                        monitored_tokens[mint] = px
                        asyncio.create_task(price_monitor(session, client, keypair, mint, px))

        except websockets.exceptions.ConnectionClosedError as e:
            log(f"WS closed: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log(f"WS error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ===================== MAIN =====================

async def main() -> None:
    keypair = load_keypair_from_b58(PRIVATE_KEY_B58)
    sol_client = AsyncClient(RPC_URL)
    global monitor_semaphore
    monitor_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MONITORS)

    # Single shared HTTP session
    async with aiohttp.ClientSession() as session:
        try:
            asyncio.create_task(scan_existing_tokens(session, sol_client, keypair))
            await monitor_new_launches(session, sol_client, keypair)
        finally:
            await sol_client.close()


if __name__ == "__main__":
    asyncio.run(main())
