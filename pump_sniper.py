import os
import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Set, Tuple

import aiohttp
import websockets
import base58
import base64

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction
from solders.message import to_bytes_versioned

# -----------------------------
# Helpers
# -----------------------------

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None and v != "" else default
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None and v != "" else default
    except Exception:
        return default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def is_valid_solana_mint(mint: str) -> bool:
    try:
        Pubkey.from_string(mint)
        return True
    except Exception:
        return False

# -----------------------------
# Config
# -----------------------------

@dataclass
class Config:
    # Core
    RPC_URL: str
    WS_URL: str

    # Wallet
    PRIVATE_KEY_B58: str

    # Buy settings
    BUY_SOL: float
    SLIPPAGE_BPS: int
    PRIORITY_FEE_MICRO_LAMPORTS: int
    SKIP_PREFLIGHT: bool
    CONFIRM_TIMEOUT_SEC: int
    CONFIRM_POLL_SEC: float

    # New token filters
    NEW_WARMUP_SEC: int
    NEW_MIN_VOL_USD_1H: float
    NEW_MIN_LIQ_USD: float
    NEW_MIN_BUYS_1H: int
    NEW_REQUIRE_SOCIALS: bool

    # Existing scan
    EXISTING_QUERIES: List[str]
    EXISTING_LIMIT_PER_QUERY: int
    EXISTING_SCAN_EVERY_SEC: int
    EXISTING_MIN_CHG_1H: float
    EXISTING_MIN_VOL_USD_1H: float
    EXISTING_MIN_LIQ_USD: float
    EXISTING_MIN_BUYS_1H: int

    # Risk gating
    MAX_BUYS_PER_SCAN: int
    BUY_COOLDOWN_SEC: int
    MAX_OPEN_POSITIONS: int

    # Sell
    TAKE_PROFIT_X: float
    STOP_LOSS_X: float
    SELL_POLL_SEC: int

    # Logging
    LOG_SKIPS: bool

def load_config() -> Config:
    # Accept both names to avoid breaking your Railway vars
    pk = env_str("PRIVATE_KEY_B58", "")
    if not pk:
        pk = env_str("PRIVATE_KEY_", "")  # your existing var name
    if not pk:
        pk = env_str("PRIVATE_KEY", "")

    return Config(
        RPC_URL=env_str("RPC_URL", ""),
        WS_URL=env_str("WS_URL", "wss://pumpportal.fun/api/data"),

        PRIVATE_KEY_B58=pk,

        BUY_SOL=env_float("BUY_SOL", 0.01),
        SLIPPAGE_BPS=env_int("SLIPPAGE_BPS", 150),
        PRIORITY_FEE_MICRO_LAMPORTS=env_int("PRIORITY_FEE_MICRO_LAMPORTS", 2000),
        SKIP_PREFLIGHT=env_bool("SKIP_PREFLIGHT", False),
        CONFIRM_TIMEOUT_SEC=env_int("CONFIRM_TIMEOUT_SEC", 90),
        CONFIRM_POLL_SEC=env_float("CONFIRM_POLL_SEC", 1.5),

        NEW_WARMUP_SEC=env_int("NEW_WARMUP_SEC", 60),
        NEW_MIN_VOL_USD_1H=env_float("NEW_MIN_VOL_USD_1H", 300.0),
        NEW_MIN_LIQ_USD=env_float("NEW_MIN_LIQ_USD", 8000.0),
        NEW_MIN_BUYS_1H=env_int("NEW_MIN_BUYS_1H", 10),
        NEW_REQUIRE_SOCIALS=env_bool("NEW_REQUIRE_SOCIALS", False),

        EXISTING_QUERIES=[q.strip() for q in env_str("EXISTING_QUERIES", "raydium,solana,pump,SOL/USDC").split(",") if q.strip()],
        EXISTING_LIMIT_PER_QUERY=env_int("EXISTING_LIMIT_PER_QUERY", 80),
        EXISTING_SCAN_EVERY_SEC=env_int("EXISTING_SCAN_EVERY_SEC", 15),
        EXISTING_MIN_CHG_1H=env_float("EXISTING_MIN_CHG_1H", 2.0),
        EXISTING_MIN_VOL_USD_1H=env_float("EXISTING_MIN_VOL_USD_1H", 3000.0),
        EXISTING_MIN_LIQ_USD=env_float("EXISTING_MIN_LIQ_USD", 15000.0),
        EXISTING_MIN_BUYS_1H=env_int("EXISTING_MIN_BUYS_1H", 25),

        MAX_BUYS_PER_SCAN=env_int("MAX_BUYS_PER_SCAN", 2),
        BUY_COOLDOWN_SEC=env_int("BUY_COOLDOWN_SEC", 10),
        MAX_OPEN_POSITIONS=env_int("MAX_OPEN_POSITIONS", 20),

        TAKE_PROFIT_X=env_float("TAKE_PROFIT_X", 1.6),
        STOP_LOSS_X=env_float("STOP_LOSS_X", 0.75),
        SELL_POLL_SEC=env_int("SELL_POLL_SEC", 5),

        LOG_SKIPS=env_bool("LOG_SKIPS", False),
    )

# -----------------------------
# Dexscreener
# -----------------------------

DEX_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={q}"

async def fetch_json(session: aiohttp.ClientSession, url: str, timeout: int = 12) -> Dict[str, Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            return await resp.json(content_type=None)
    except Exception:
        return {}

def pick_best_solana_pair(pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # Prefer solana pairs with highest liquidity USD
    sol_pairs = [p for p in pairs if (p.get("chainId") == "solana")]
    if not sol_pairs:
        return None
    sol_pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0), reverse=True)
    return sol_pairs[0]

async def get_token_data(session: aiohttp.ClientSession, mint: str) -> Dict[str, Any]:
    url = DEX_TOKEN_URL.format(mint=mint)
    data = await fetch_json(session, url)
    pairs = data.get("pairs") or []
    pair = pick_best_solana_pair(pairs)
    if not pair:
        return {}

    def fnum(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    price = fnum(pair.get("priceUsd") or 0)
    vol1h = fnum((pair.get("volume") or {}).get("h1") or 0)
    liq = fnum((pair.get("liquidity") or {}).get("usd") or 0)
    buys1h = int(((pair.get("txns") or {}).get("h1") or {}).get("buys") or 0)

    return {
        "priceUsd": price,
        "volume1hUsd": vol1h,
        "liquidityUsd": liq,
        "buys1h": buys1h,
        "pair": pair,
    }

async def search_pairs(session: aiohttp.ClientSession, q: str) -> List[Dict[str, Any]]:
    url = DEX_SEARCH_URL.format(q=aiohttp.helpers.quote(q, safe=""))
    data = await fetch_json(session, url)
    return data.get("pairs") or []

# -----------------------------
# Jupiter swap + Solana send
# -----------------------------

SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

JUP_QUOTE = "https://quote-api.jup.ag/v6/quote"
JUP_SWAP = "https://quote-api.jup.ag/v6/swap"

def keypair_from_b58(pk_b58: str) -> Keypair:
    raw = base58.b58decode(pk_b58)
    return Keypair.from_bytes(raw)

async def get_token_balance_base_units(client: AsyncClient, owner: Pubkey, mint: Pubkey) -> int:
    # Returns base units (raw integer, not UI amount)
    try:
        opts = TokenAccountOpts(mint=mint, program_id=TOKEN_PROGRAM_ID)
        resp = await client.get_token_accounts_by_owner(owner, opts)
        # resp can be an error message object in some cases; guard hard.
        val = getattr(resp, "value", None)
        if not val:
            return 0
        token_acc = val[0].pubkey
        bal = await client.get_token_account_balance(token_acc)
        bval = getattr(bal, "value", None)
        if not bval:
            return 0
        return int(bval.amount)
    except Exception:
        return 0

async def jupiter_quote(session: aiohttp.ClientSession, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": str(slippage_bps),
    }
    try:
        async with session.get(JUP_QUOTE, params=params, timeout=aiohttp.ClientTimeout(total=12)) as resp:
            return await resp.json(content_type=None)
    except Exception:
        return {"error": "quote request failed"}

async def jupiter_swap_tx(session: aiohttp.ClientSession, quote_resp: Dict[str, Any], user_pubkey: str, compute_unit_price_micro_lamports: int) -> Dict[str, Any]:
    payload = {
        "quoteResponse": quote_resp,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
    }
    # Jupiter supports computeUnitPriceMicroLamports (priority fee) in swap request
    if compute_unit_price_micro_lamports and compute_unit_price_micro_lamports > 0:
        payload["computeUnitPriceMicroLamports"] = int(compute_unit_price_micro_lamports)

    try:
        async with session.post(JUP_SWAP, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            return await resp.json(content_type=None)
    except Exception:
        return {"error": "swap request failed"}

def sign_jupiter_swap_tx(tx_b64: str, kp: Keypair) -> bytes:
    raw = base64.b64decode(tx_b64)
    tx = VersionedTransaction.from_bytes(raw)

    msg_bytes = to_bytes_versioned(tx.message)
    sig = kp.sign_message(msg_bytes)

    # Replace signature for kp's pubkey in the signature array (payer is usually index 0, but don’t assume)
    acct_keys = list(tx.message.account_keys)
    try:
        signer_index = acct_keys.index(kp.pubkey())
    except ValueError:
        signer_index = 0  # fallback

    sigs = list(tx.signatures)
    # Ensure list long enough
    while len(sigs) < len(acct_keys):
        sigs.append(Signature.default())
    sigs[signer_index] = sig

    signed = VersionedTransaction.populate(tx.message, sigs)
    return bytes(signed)

async def wait_for_confirmation(client: AsyncClient, sig_str: str, timeout_sec: int, poll_sec: float) -> Tuple[bool, str]:
    # Returns (confirmed, reason)
    deadline = time.time() + timeout_sec
    sig = Signature.from_string(sig_str)

    while time.time() < deadline:
        try:
            st = await client.get_signature_statuses([sig], search_transaction_history=True)
            vals = getattr(st, "value", None)
            if vals and vals[0] is not None:
                s0 = vals[0]
                err = getattr(s0, "err", None)
                conf = getattr(s0, "confirmation_status", None)  # processed/confirmed/finalized
                if err is not None:
                    return False, f"rpc err={err}"
                if conf in ("confirmed", "finalized"):
                    return True, f"status={conf}"
        except Exception:
            pass

        await asyncio.sleep(poll_sec)

    # One last diagnostic: did RPC ever see it?
    try:
        st = await client.get_signature_statuses([sig], search_transaction_history=True)
        vals = getattr(st, "value", None)
        if vals and vals[0] is None:
            return False, "timeout; RPC never saw signature (not broadcast / dropped)"
    except Exception:
        pass

    return False, "timeout waiting confirmation"

async def send_swap(
    session: aiohttp.ClientSession,
    client: AsyncClient,
    kp: Keypair,
    action: str,
    mint_str: str,
    buy_amount_lamports: int,
    cfg: Config,
) -> Optional[str]:
    owner = kp.pubkey()

    if action == "buy":
        quote = await jupiter_quote(session, SOL_MINT, mint_str, buy_amount_lamports, cfg.SLIPPAGE_BPS)
    else:
        bal = await get_token_balance_base_units(client, owner, Pubkey.from_string(mint_str))
        if bal <= 0:
            log(f"SELL skipped (no balance) mint={mint_str}")
            return None
        quote = await jupiter_quote(session, mint_str, SOL_MINT, bal, cfg.SLIPPAGE_BPS)

    if not quote or quote.get("error"):
        log(f"Swap error ({action}) mint={mint_str}: quote_error={quote.get('error') if quote else 'empty'}")
        return None

    swap = await jupiter_swap_tx(session, quote, str(owner), cfg.PRIORITY_FEE_MICRO_LAMPORTS)
    if not swap or swap.get("error"):
        log(f"Swap error ({action}) mint={mint_str}: swap_error={swap.get('error') if swap else 'empty'}")
        return None

    tx_b64 = swap.get("swapTransaction")
    if not tx_b64:
        log(f"Swap error ({action}) mint={mint_str}: missing swapTransaction")
        return None

    try:
        signed_raw = sign_jupiter_swap_tx(tx_b64, kp)
    except Exception as e:
        log(f"Swap error ({action}) mint={mint_str}: signing_failed={e}")
        return None

    try:
        opts = TxOpts(
            skip_preflight=cfg.SKIP_PREFLIGHT,
            preflight_commitment="confirmed",
            max_retries=3,
        )
        resp = await client.send_raw_transaction(signed_raw, opts=opts)
        sig = getattr(resp, "value", None) or str(resp)
        sig_str = str(sig)
        log(f"{action.upper()} submitted sig={sig_str}")

        if cfg.SKIP_PREFLIGHT:
            log("WARNING: SKIP_PREFLIGHT=true. If you see ghost tx links, set SKIP_PREFLIGHT=false.")
        ok, reason = await wait_for_confirmation(client, sig_str, cfg.CONFIRM_TIMEOUT_SEC, cfg.CONFIRM_POLL_SEC)
        if not ok:
            log(f"{action.upper()} NOT confirmed mint={mint_str}: sig={sig_str} reason={reason}")
            return None

        log(f"{action.upper()} CONFIRMED: https://solscan.io/tx/{sig_str}")
        return sig_str
    except Exception as e:
        log(f"Swap error ({action}) mint={mint_str}: send_failed={e}")
        return None

# -----------------------------
# Strategy loops
# -----------------------------

class State:
    def __init__(self) -> None:
        self.monitored: Dict[str, float] = {}  # mint -> entryPriceUsd
        self.held: Set[str] = set()
        self.last_buy_ts: float = 0.0

async def price_monitor_loop(session: aiohttp.ClientSession, client: AsyncClient, kp: Keypair, cfg: Config, st: State) -> None:
    while True:
        await asyncio.sleep(cfg.SELL_POLL_SEC)
        if not st.monitored:
            continue

        mints = list(st.monitored.keys())
        for mint in mints[:]:
            entry = st.monitored.get(mint)
            if not entry or entry <= 0:
                continue

            td = await get_token_data(session, mint)
            px = float(td.get("priceUsd") or 0.0)
            if px <= 0:
                continue

            mult = px / entry
            if mult >= cfg.TAKE_PROFIT_X:
                log(f"TAKE PROFIT mint={mint} mult={mult:.2f}x -> SELL")
                sig = await send_swap(session, client, kp, "sell", mint, 0, cfg)
                if sig:
                    st.monitored.pop(mint, None)
                    st.held.discard(mint)

            elif mult <= cfg.STOP_LOSS_X:
                log(f"STOP LOSS mint={mint} mult={mult:.2f}x -> SELL")
                sig = await send_swap(session, client, kp, "sell", mint, 0, cfg)
                if sig:
                    st.monitored.pop(mint, None)
                    st.held.discard(mint)

async def scan_existing_loop(session: aiohttp.ClientSession, client: AsyncClient, kp: Keypair, cfg: Config, st: State) -> None:
    cycle = 0
    buy_amount_lamports = int(cfg.BUY_SOL * 1_000_000_000)

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

        # Global risk cap
        if len(st.monitored) >= cfg.MAX_OPEN_POSITIONS:
            skipped_maxpos = 1
            log(f"ExistingScan cycle={cycle}: MAX_OPEN_POSITIONS reached ({len(st.monitored)}/{cfg.MAX_OPEN_POSITIONS})")
            await asyncio.sleep(cfg.EXISTING_SCAN_EVERY_SEC)
            continue

        # Cooldown
        if time.time() - st.last_buy_ts < cfg.BUY_COOLDOWN_SEC:
            skipped_cooldown = 1
            await asyncio.sleep(cfg.EXISTING_SCAN_EVERY_SEC)
            continue

        # Pull pairs for each query, dedupe by base mint
        seen: Set[str] = set()
        all_pairs: List[Dict[str, Any]] = []

        for q in cfg.EXISTING_QUERIES:
            pairs = await search_pairs(session, q)
            if pairs:
                all_pairs.extend(pairs[: cfg.EXISTING_LIMIT_PER_QUERY])

        # Dedupe by base address
        deduped: List[Dict[str, Any]] = []
        for p in all_pairs:
            base = ((p.get("baseToken") or {}).get("address")) or ""
            if not base:
                continue
            if base in seen:
                continue
            seen.add(base)
            deduped.append(p)

        for pair in deduped:
            scanned += 1

            if pair.get("chainId") != "solana":
                skipped_chain += 1
                continue

            mint = ((pair.get("baseToken") or {}).get("address")) or ""
            if not mint or not is_valid_solana_mint(mint):
                skipped_invalid += 1
                continue

            if mint in st.held or mint in st.monitored:
                skipped_held += 1
                continue

            chg1h = float((pair.get("priceChange") or {}).get("h1") or 0.0)
            vol1h = float((pair.get("volume") or {}).get("h1") or 0.0)
            liq = float((pair.get("liquidity") or {}).get("usd") or 0.0)
            buys1h = int((((pair.get("txns") or {}).get("h1") or {}).get("buys")) or 0)
            px = float(pair.get("priceUsd") or 0.0)

            if (
                chg1h < cfg.EXISTING_MIN_CHG_1H
                or vol1h < cfg.EXISTING_MIN_VOL_USD_1H
                or liq < cfg.EXISTING_MIN_LIQ_USD
                or buys1h < cfg.EXISTING_MIN_BUYS_1H
                or px <= 0.0
            ):
                skipped_filters += 1
                continue

            candidates += 1
            log(f"EXISTING candidate {mint} | chg1h={chg1h:.2f}% vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.8f}")

            # risk caps per scan
            if buys >= cfg.MAX_BUYS_PER_SCAN:
                continue

            if len(st.monitored) >= cfg.MAX_OPEN_POSITIONS:
                skipped_maxpos += 1
                break

            sig = await send_swap(session, client, kp, "buy", mint, buy_amount_lamports, cfg)
            if not sig:
                skipped_confirm += 1
                continue

            buys += 1
            st.last_buy_ts = time.time()
            st.held.add(mint)
            st.monitored[mint] = px

        log(
            "ExistingScan "
            f"cycle={cycle} scanned={scanned} candidates={candidates} buys={buys} "
            f"skipped(chain={skipped_chain}, invalid={skipped_invalid}, held={skipped_held}, "
            f"filters={skipped_filters}, confirm={skipped_confirm}, cooldown={skipped_cooldown}, maxpos={skipped_maxpos}) "
            f"thresholds(chg1h>={cfg.EXISTING_MIN_CHG_1H}, vol1h>={cfg.EXISTING_MIN_VOL_USD_1H}, "
            f"liq>={cfg.EXISTING_MIN_LIQ_USD}, buys1h>={cfg.EXISTING_MIN_BUYS_1H})"
        )

        await asyncio.sleep(cfg.EXISTING_SCAN_EVERY_SEC)

async def monitor_new_tokens_loop(session: aiohttp.ClientSession, client: AsyncClient, kp: Keypair, cfg: Config, st: State) -> None:
    backoff = 5
    buy_amount_lamports = int(cfg.BUY_SOL * 1_000_000_000)

    while True:
        try:
            async with websockets.connect(cfg.WS_URL, ping_interval=10, ping_timeout=30) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log("Connected to PumpPortal WS. Subscribed to new tokens.")
                backoff = 5

                while True:
                    raw = await ws.recv()
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue

                    mint = data.get("mint")
                    if not mint:
                        continue
                    if not is_valid_solana_mint(mint):
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip invalid mint={mint}")
                        continue

                    if mint in st.held or mint in st.monitored:
                        continue

                    # Optional socials check (based on pumpportal payload)
                    has_socials = bool(data.get("twitter") or data.get("telegram") or data.get("website"))

                    # Warm up so DexScreener has non-zero stats
                    await asyncio.sleep(cfg.NEW_WARMUP_SEC)

                    td = await get_token_data(session, mint)
                    vol1h = float(td.get("volume1hUsd") or 0.0)
                    liq = float(td.get("liquidityUsd") or 0.0)
                    buys1h = int(td.get("buys1h") or 0)
                    px = float(td.get("priceUsd") or 0.0)

                    if px <= 0.0:
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip mint={mint} reason=no_price")
                        continue

                    if vol1h < cfg.NEW_MIN_VOL_USD_1H or liq < cfg.NEW_MIN_LIQ_USD or buys1h < cfg.NEW_MIN_BUYS_1H:
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip mint={mint} reason=filters vol1h={vol1h} liq={liq} buys1h={buys1h}")
                        continue

                    if cfg.NEW_REQUIRE_SOCIALS and not has_socials:
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip mint={mint} reason=no_socials")
                        continue

                    # Risk caps
                    if len(st.monitored) >= cfg.MAX_OPEN_POSITIONS:
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip mint={mint} reason=max_positions")
                        continue
                    if time.time() - st.last_buy_ts < cfg.BUY_COOLDOWN_SEC:
                        if cfg.LOG_SKIPS:
                            log(f"NEW skip mint={mint} reason=cooldown")
                        continue

                    log(f"NEW candidate {mint} | vol1h=${vol1h:.2f} liq=${liq:.2f} buys1h={buys1h} px=${px:.8f} socials={has_socials}")

                    sig = await send_swap(session, client, kp, "buy", mint, buy_amount_lamports, cfg)
                    if not sig:
                        continue

                    st.last_buy_ts = time.time()
                    st.held.add(mint)
                    st.monitored[mint] = px

        except websockets.exceptions.ConnectionClosedError as e:
            log(f"WS closed: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log(f"WS error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def heartbeat_loop(st: State) -> None:
    while True:
        await asyncio.sleep(60)
        log(f"Heartbeat: monitored={len(st.monitored)}")

def print_boot(cfg: Config, pubkey: str) -> None:
    log("BOOT CONFIG:")
    log(f"  WALLET={pubkey}")
    log(f"  WS_URL={cfg.WS_URL}")
    log(f"  BUY_SOL={cfg.BUY_SOL} SLIPPAGE_BPS={cfg.SLIPPAGE_BPS} PRIORITY_FEE_MICRO_LAMPORTS={cfg.PRIORITY_FEE_MICRO_LAMPORTS} SKIP_PREFLIGHT={cfg.SKIP_PREFLIGHT}")
    log(f"  CONFIRM: timeout={cfg.CONFIRM_TIMEOUT_SEC}s poll={cfg.CONFIRM_POLL_SEC}s")
    log(f"  NEW: warmup={cfg.NEW_WARMUP_SEC}s minVol1h={cfg.NEW_MIN_VOL_USD_1H} minLiq={cfg.NEW_MIN_LIQ_USD} minBuys1h={cfg.NEW_MIN_BUYS_1H} requireSocials={cfg.NEW_REQUIRE_SOCIALS}")
    log(f"  EXISTING: queries={','.join(cfg.EXISTING_QUERIES)} limitPerQuery={cfg.EXISTING_LIMIT_PER_QUERY} scanEvery={cfg.EXISTING_SCAN_EVERY_SEC}s")
    log(f"           minChg1h={cfg.EXISTING_MIN_CHG_1H}% minVol1h={cfg.EXISTING_MIN_VOL_USD_1H} minLiq={cfg.EXISTING_MIN_LIQ_USD} minBuys1h={cfg.EXISTING_MIN_BUYS_1H}")
    log(f"  RISK: MAX_BUYS_PER_SCAN={cfg.MAX_BUYS_PER_SCAN} BUY_COOLDOWN_SEC={cfg.BUY_COOLDOWN_SEC} MAX_OPEN_POSITIONS={cfg.MAX_OPEN_POSITIONS}")
    log(f"  SELL: tp={cfg.TAKE_PROFIT_X}x sl={cfg.STOP_LOSS_X}x poll={cfg.SELL_POLL_SEC}s")
    log(f"  LOG_SKIPS={cfg.LOG_SKIPS}")

async def main() -> None:
    cfg = load_config()

    if not cfg.RPC_URL:
        raise SystemExit("RPC_URL is required.")
    if not cfg.PRIVATE_KEY_B58:
        raise SystemExit("PRIVATE_KEY_B58 (or PRIVATE_KEY_ / PRIVATE_KEY) is required.")

    kp = keypair_from_b58(cfg.PRIVATE_KEY_B58)
    pubkey = str(kp.pubkey())

    print_boot(cfg, pubkey)

    # Use confirmed commitment by default for reads; sends specify preflight_commitment explicitly.
    client = AsyncClient(cfg.RPC_URL, commitment="confirmed")
    st = State()

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            monitor_new_tokens_loop(session, client, kp, cfg, st),
            scan_existing_loop(session, client, kp, cfg, st),
            price_monitor_loop(session, client, kp, cfg, st),
            heartbeat_loop(st),
        )

if __name__ == "__main__":
    asyncio.run(main())
