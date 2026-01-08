import asyncio
import json
import time
import base64
import base58
import requests
import websockets

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

# ===================== CONFIG =====================

PRIVATE_KEY = "5j2MJsbFgCZfHXiAs7uSDPMukt8xKDDpj9vSQxigG9hZUd4aNVaMq4HcF4jBcP2Hxc3qRAuacyQmUs8SkmDq4qU5"
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
WS_URL = "wss://pumpportal.fun/api/data"

SOL_MINT = "So11111111111111111111111111111111111111112"

BUY_AMOUNT_LAMPORTS = int(0.01 * 1e9)  # 0.01 SOL
SLIPPAGE_BPS = 50
PRIORITY_FEE_SOL = 0.0005

PROFIT_TARGET = 3.0      # 3x
STOP_LOSS = 0.5          # -50%

SCAN_INTERVAL = 30
PRICE_POLL_INTERVAL = 5

MIN_VOLUME = 0
MIN_LIQUIDITY = 0
MIN_PRICE_CHANGE_1H = 0

TOKEN_PROGRAM_ID = Pubkey.from_string(
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

# ===================== SETUP =====================

client = AsyncClient(RPC_URL)
wallet = Keypair.from_bytes(base58.b58decode(PRIVATE_KEY))
wallet_pubkey = wallet.pubkey()

monitored_tokens = set()

# ===================== HELPERS =====================

def is_valid_solana_mint(mint: str) -> bool:
    try:
        Pubkey.from_string(mint)
        return not mint.startswith("0x")
    except:
        return False


def get_token_data(mint: str) -> dict:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        r = requests.get(url, timeout=10).json()
        if not r.get("pairs"):
            return {}

        p = r["pairs"][0]
        return {
            "price": float(p.get("priceUsd", 0) or 0),
            "volume": float(p["volume"].get("h1", 0) or 0),
            "liquidity": float(p.get("liquidity", {}).get("usd", 0) or 0),
        }
    except:
        return {}


async def get_token_balance(mint: str) -> int:
    try:
        opts = TokenAccountOpts(
            mint=Pubkey.from_string(mint),
            program_id=TOKEN_PROGRAM_ID
        )
        res = await client.get_token_accounts_by_owner(wallet_pubkey, opts)
        if not res.value:
            return 0

        acct = res.value[0].pubkey
        bal = await client.get_token_account_balance(acct)
        return int(bal.value.amount)
    except:
        return 0


# ===================== TRADING =====================

async def send_swap(action: str, mint: str):
    try:
        if action == "buy":
            input_mint = SOL_MINT
            output_mint = mint
            amount = BUY_AMOUNT_LAMPORTS
        else:
            balance = await get_token_balance(mint)
            if balance == 0:
                return
            input_mint = mint
            output_mint = SOL_MINT
            amount = balance

        quote_url = (
            f"https://quote-api.jup.ag/v6/quote"
            f"?inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount}"
            f"&slippageBps={SLIPPAGE_BPS}"
        )

        quote = requests.get(quote_url).json()
        if "error" in quote:
            print("Quote error:", quote["error"])
            return

        swap_payload = {
            "quoteResponse": quote,
            "userPublicKey": str(wallet_pubkey),
            "wrapAndUnwrapSol": True,
            "computeUnitPriceMicroLamports": int(PRIORITY_FEE_SOL * 1e9),
        }

        swap = requests.post(
            "https://quote-api.jup.ag/v6/swap",
            json=swap_payload
        ).json()

        if "swapTransaction" not in swap:
            print("Swap failed:", swap)
            return

        tx_bytes = base64.b64decode(swap["swapTransaction"])
        tx = VersionedTransaction.from_bytes(tx_bytes)

        sig = wallet.sign_message(tx.message.serialize())
        signed_tx = VersionedTransaction(tx.message, [sig])

        res = await client.send_transaction(signed_tx)
        print(f"{action.upper()} TX → https://solscan.io/tx/{res.value}")

    except Exception as e:
        print("Swap error:", e)


# ===================== PRICE MONITOR =====================

async def price_monitor(mint: str, entry_price: float):
    while True:
        await asyncio.sleep(PRICE_POLL_INTERVAL)

        data = get_token_data(mint)
        price = data.get("price", 0)
        if price == 0:
            continue

        ratio = price / entry_price

        if ratio >= PROFIT_TARGET:
            print(f"PROFIT SELL {mint} @ {ratio:.2f}x")
            await send_swap("sell", mint)
            monitored_tokens.discard(mint)
            return

        if ratio <= STOP_LOSS:
            print(f"STOP LOSS {mint} @ {ratio:.2f}x")
            await send_swap("sell", mint)
            monitored_tokens.discard(mint)
            return


# ===================== EXISTING TOKENS =====================

async def scan_existing_tokens():
    while True:
        try:
            r = requests.get(
                "https://api.dexscreener.com/latest/dex/search?q=SOL"
            ).json()

            for p in r.get("pairs", [])[:200]:
                if p.get("chainId") != "solana":
                    continue

                mint = p["baseToken"]["address"]
                if not is_valid_solana_mint(mint):
                    continue

                if mint in monitored_tokens:
                    continue

                if await get_token_balance(mint) > 0:
                    continue

                change = float(p.get("priceChange", {}).get("h1", 0) or 0)
                volume = float(p.get("volume", {}).get("h1", 0) or 0)
                liquidity = float(p.get("liquidity", {}).get("usd", 0) or 0)

                if (
                    change >= MIN_PRICE_CHANGE_1H
                    and volume >= MIN_VOLUME
                    and liquidity >= MIN_LIQUIDITY
                ):
                    print(f"SNIPING EXISTING → {mint}")
                    await send_swap("buy", mint)

                    price = float(p.get("priceUsd", 0) or 0)
                    if price > 0:
                        monitored_tokens.add(mint)
                        asyncio.create_task(price_monitor(mint, price))

        except Exception as e:
            print("Existing scan error:", e)

        await asyncio.sleep(SCAN_INTERVAL)


# ===================== NEW TOKENS =====================

async def monitor_new_launches():
    backoff = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                print("WS connected")

                async for msg in ws:
                    data = json.loads(msg)
                    mint = data.get("mint")
                    if not mint or not is_valid_solana_mint(mint):
                        continue

                    await asyncio.sleep(60)

                    stats = get_token_data(mint)
                    if not stats:
                        continue

                    if (
                        stats["volume"] >= MIN_VOLUME
                        and stats["liquidity"] >= MIN_LIQUIDITY
                    ):
                        print(f"SNIPING NEW → {mint}")
                        await send_swap("buy", mint)

                        if stats["price"] > 0:
                            monitored_tokens.add(mint)
                            asyncio.create_task(
                                price_monitor(mint, stats["price"])
                            )

            backoff = 5

        except Exception as e:
            print("WS error:", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ===================== MAIN =====================

async def main():
    asyncio.create_task(scan_existing_tokens())
    await monitor_new_launches()

asyncio.run(main())
