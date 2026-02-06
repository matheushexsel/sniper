import os
import time
import math
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

import httpx

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("updown15m_ladder")


# =========================
# SETTINGS
# =========================
@dataclass
class Settings:
    # Hosts
    CLOB_HOST: str = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")

    # Assets
    ASSETS: List[str] = None

    # Discovery
    WINDOW_SEC: int = int(os.getenv("WINDOW_SEC", "900"))
    SLUG_SCAN_BEHIND: int = int(os.getenv("SLUG_SCAN_BEHIND", "12"))
    SLUG_SCAN_AHEAD: int = int(os.getenv("SLUG_SCAN_AHEAD", "12"))

    # Strategy timing
    POLL_SEC: float = float(os.getenv("POLL_SEC", "2.0"))
    EVAL_INTERVAL_SEC: int = int(os.getenv("EVAL_INTERVAL_SEC", "120"))  # every 2 min
    EVAL_COUNT: int = int(os.getenv("EVAL_COUNT", "6"))                  # 6 evaluations
    EXIT_AT_SEC: int = int(os.getenv("EXIT_AT_SEC", "780"))              # 13 min (780s) from start

    # Strategy sizing (USDC notionals per step)
    BASE_USDC: float = float(os.getenv("BASE_USDC", "1.0"))              # buy both sides at start
    STEP_USDC: float = float(os.getenv("STEP_USDC", "1.0"))              # add to winner side each eval

    # Execution controls
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    MAX_SLIPPAGE: float = float(os.getenv("MAX_SLIPPAGE", "0.02"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    COOLDOWN_SEC: float = float(os.getenv("COOLDOWN_SEC", "0.25"))

    # Credentials (your system already uses these in Railway)
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_PROXY_ADDRESS: str = os.getenv("POLY_PROXY_ADDRESS", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")
    POLY_CHAIN_ID: int = int(os.getenv("POLY_CHAIN_ID", "137"))

    def __post_init__(self):
        if self.ASSETS is None:
            assets_raw = os.getenv("ASSETS", "ETH,BTC,SOL,XRP")
            self.ASSETS = [a.strip().upper() for a in assets_raw.split(",") if a.strip()]


S = Settings()


# =========================
# MARKET DISCOVERY (FIX)
# =========================
ASSET_PREFIX = {
    "BTC": "btc",
    "ETH": "eth",
    "SOL": "sol",
    "XRP": "xrp",
}


def epoch_now() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def round_to_window(ts: int, window: int) -> int:
    return (ts // window) * window


def gamma_event_by_slug(gamma_host: str, slug: str) -> Optional[dict]:
    """
    Correct endpoint: /events/slug/{slug}
    Your earlier failures happened because you were calling /events/{slug} (wrong) or /markets?sort=... (422)
    """
    url = f"{gamma_host}/events/slug/{slug}"
    try:
        r = httpx.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def extract_market_tokens_from_event(event_json: dict) -> Tuple[str, str, str]:
    """
    Returns: (market_id_or_condition, yes_token_id, no_token_id)
    Gamma typically returns event_json["markets"][0]["clobTokenIds"] = [YES, NO]
    """
    markets = event_json.get("markets") or []
    if not markets:
        raise RuntimeError("Gamma event JSON has no markets[]")

    m0 = markets[0]

    if "clobTokenIds" in m0 and isinstance(m0["clobTokenIds"], list) and len(m0["clobTokenIds"]) >= 2:
        yes_token_id = m0["clobTokenIds"][0]
        no_token_id = m0["clobTokenIds"][1]
    else:
        # fallback shape
        outcomes = m0.get("outcomes") or []
        if len(outcomes) < 2:
            raise RuntimeError("Gamma market JSON missing clobTokenIds and outcomes[]")
        yes_token_id = outcomes[0].get("clobTokenId")
        no_token_id = outcomes[1].get("clobTokenId")
        if not yes_token_id or not no_token_id:
            raise RuntimeError("Could not extract clobTokenId from outcomes")

    market_id = m0.get("id") or m0.get("conditionId") or event_json.get("id")
    if not market_id:
        market_id = "unknown"

    return str(market_id), str(yes_token_id), str(no_token_id)


def resolve_active_15m_market(asset: str) -> Optional[Dict[str, Any]]:
    """
    Deterministic slug scan (behind + ahead):
      {prefix}-updown-15m-{epoch_rounded_to_15m}

    Picks the first slug that exists AND is still within its open window.
    """
    asset = asset.upper()
    if asset not in ASSET_PREFIX:
        raise ValueError(f"Unsupported asset: {asset}")

    prefix = ASSET_PREFIX[asset]
    now_ts = epoch_now()
    base = round_to_window(now_ts, S.WINDOW_SEC)

    # scan behind + ahead (e.g. -12..+12 windows)
    for k in range(-S.SLUG_SCAN_BEHIND, S.SLUG_SCAN_AHEAD + 1):
        start_ts = base + (k * S.WINDOW_SEC)
        if start_ts <= 0:
            continue
        slug = f"{prefix}-updown-15m-{start_ts}"

        ev = gamma_event_by_slug(S.GAMMA_HOST, slug)
        if not ev:
            continue

        # validate still open
        if now_ts < start_ts + S.WINDOW_SEC:
            market_id, yes_token, no_token = extract_market_tokens_from_event(ev)
            return {
                "asset": asset,
                "slug": slug,
                "start_ts": start_ts,
                "end_ts": start_ts + S.WINDOW_SEC,
                "market_id": market_id,
                "yes_token_id": yes_token,
                "no_token_id": no_token,
            }

    return None


# =========================
# TRADING ADAPTERS (YOU MUST MAP THESE TO YOUR EXISTING FUNCTIONS)
# =========================
class TradingClient:
    """
    This is a thin adapter shell.
    Replace internals with YOUR working py_clob_client init + methods.
    """
    def __init__(self):
        # TODO: initialize your actual client here
        # example (depends on your py_clob_client version):
        # from py_clob_client.client import ClobClient
        # self.client = ClobClient(host=S.CLOB_HOST, chain_id=S.POLY_CHAIN_ID, ...)
        self.client = None

    def get_best_ask(self, token_id: str) -> Optional[float]:
        # TODO: return best ask price for token_id
        # must be in [0,1] price terms
        raise NotImplementedError

    def get_best_bid(self, token_id: str) -> Optional[float]:
        # TODO: return best bid price for token_id
        raise NotImplementedError

    def buy_usdc_notional(self, token_id: str, usdc: float, max_slippage: float) -> None:
        """
        Buy shares spending ~usdc.
        Implementation: compute shares = usdc / best_ask, place marketable limit.
        """
        raise NotImplementedError

    def sell_all(self, token_id: str) -> None:
        raise NotImplementedError


# =========================
# STRATEGY CORE (UNCHANGED INTENT)
# =========================
@dataclass
class PositionState:
    yes_token: str
    no_token: str
    start_ts: int
    end_ts: int
    slug: str

    # tracking
    base_done: bool = False
    eval_idx: int = 0
    next_eval_ts: int = 0
    exit_ts: int = 0

    # last prices
    last_yes: Optional[float] = None
    last_no: Optional[float] = None


def compute_winner_side(last_yes: float, last_no: float, now_yes: float, now_no: float) -> str:
    """
    "Winner" = side whose price increased more in absolute delta.
    If tie -> buy none.
    """
    dy = now_yes - last_yes
    dn = now_no - last_no
    if dy > dn and dy > 0:
        return "YES"
    if dn > dy and dn > 0:
        return "NO"
    return "NONE"


def run_market(client: TradingClient, m: Dict[str, Any]) -> None:
    state = PositionState(
        yes_token=m["yes_token_id"],
        no_token=m["no_token_id"],
        start_ts=m["start_ts"],
        end_ts=m["end_ts"],
        slug=m["slug"],
        next_eval_ts=m["start_ts"] + S.EVAL_INTERVAL_SEC,
        exit_ts=m["start_ts"] + S.EXIT_AT_SEC
    )

    logger.info(f"[MARKET] {m['asset']} slug={state.slug} start={state.start_ts} end={state.end_ts}")
    logger.info(f"[TOKENS] yes={state.yes_token} no={state.no_token}")

    while True:
        now = epoch_now()

        # stop if past exit time
        if now >= state.exit_ts:
            logger.info(f"[EXIT] now>=exit_ts ({now} >= {state.exit_ts}) selling all")
            if not S.DRY_RUN:
                # sell both
                try:
                    client.sell_all(state.yes_token)
                except Exception as e:
                    logger.warning(f"[SELL] YES failed: {e}")
                try:
                    client.sell_all(state.no_token)
                except Exception as e:
                    logger.warning(f"[SELL] NO failed: {e}")
            else:
                logger.info("[DRY_RUN] skip selling")
            break

        # get prices
        try:
            yes_ask = client.get_best_ask(state.yes_token)
            no_ask = client.get_best_ask(state.no_token)
        except Exception as e:
            logger.warning(f"[PRICES] failed: {e}")
            time.sleep(S.POLL_SEC)
            continue

        if yes_ask is None or no_ask is None:
            logger.info("[PRICES] missing best ask; retry")
            time.sleep(S.POLL_SEC)
            continue

        logger.info(f"[PRICES] {m['asset']} ya={yes_ask:.4f} na={no_ask:.4f}")

        # Base entry immediately once we have prices
        if not state.base_done:
            logger.info(f"[BASE] buying {S.BASE_USDC:.2f} USDC each side")
            if not S.DRY_RUN:
                client.buy_usdc_notional(state.yes_token, S.BASE_USDC, S.MAX_SLIPPAGE)
                client.buy_usdc_notional(state.no_token, S.BASE_USDC, S.MAX_SLIPPAGE)
            else:
                logger.info("[DRY_RUN] base buys skipped")
            state.base_done = True
            state.last_yes = yes_ask
            state.last_no = no_ask

        # Evaluate every 2 minutes, 6 times total
        if state.eval_idx < S.EVAL_COUNT and now >= state.next_eval_ts:
            winner = compute_winner_side(state.last_yes, state.last_no, yes_ask, no_ask)
            logger.info(f"[EVAL {state.eval_idx+1}/{S.EVAL_COUNT}] winner={winner} yes={yes_ask:.4f} no={no_ask:.4f}")

            if winner == "YES":
                if not S.DRY_RUN:
                    client.buy_usdc_notional(state.yes_token, S.STEP_USDC, S.MAX_SLIPPAGE)
                else:
                    logger.info("[DRY_RUN] step buy YES skipped")
            elif winner == "NO":
                if not S.DRY_RUN:
                    client.buy_usdc_notional(state.no_token, S.STEP_USDC, S.MAX_SLIPPAGE)
                else:
                    logger.info("[DRY_RUN] step buy NO skipped")
            else:
                logger.info("[EVAL] no clear winner; no buy")

            state.eval_idx += 1
            state.next_eval_ts = state.start_ts + (state.eval_idx + 1) * S.EVAL_INTERVAL_SEC
            state.last_yes = yes_ask
            state.last_no = no_ask

        time.sleep(S.POLL_SEC)


# =========================
# MAIN LOOP
# =========================
def main():
    logger.info("=== UPDOWN 15M LADDER BOT START ===")
    logger.info(f"CLOB_HOST={S.CLOB_HOST} CHAIN_ID={S.POLY_CHAIN_ID} GAMMA_HOST={S.GAMMA_HOST}")
    logger.info(f"ASSETS={S.ASSETS}")
    logger.info(f"POLL_SEC={S.POLL_SEC} RESOLVE_EVERY_SEC=15.0")
    logger.info(f"LADDER: BASE_USDC={S.BASE_USDC:.2f} STEP_USDC={S.STEP_USDC:.2f} "
                f"EVAL_INTERVAL_SEC={S.EVAL_INTERVAL_SEC} EVAL_COUNT={S.EVAL_COUNT} EXIT_AT_SEC={S.EXIT_AT_SEC}")
    logger.info(f"EXEC: MAX_SLIPPAGE={S.MAX_SLIPPAGE:.3f} MAX_RETRIES={S.MAX_RETRIES} COOLDOWN_SEC={S.COOLDOWN_SEC}")
    logger.info(f"DRY_RUN={S.DRY_RUN}")
    logger.info(f"SLUG_SCAN: behind={S.SLUG_SCAN_BEHIND} ahead={S.SLUG_SCAN_AHEAD}")

    # init trading client
    client = TradingClient()

    # loop forever: for each asset find an active market and run it
    while True:
        for asset in S.ASSETS:
            try:
                m = resolve_active_15m_market(asset)
                if not m:
                    logger.info(f"[MARKET] {asset} no active 15m market found")
                    continue

                # only trade if within first 13 minutes window (we exit at 780s)
                now = epoch_now()
                if now >= m["start_ts"] + S.EXIT_AT_SEC:
                    logger.info(f"[SKIP] {asset} market too late in window (now={now}, start={m['start_ts']})")
                    continue

                run_market(client, m)

            except Exception as e:
                logger.warning(f"[ERROR] {asset}: {e}")

        time.sleep(2.0)


if __name__ == "__main__":
    main()
