# trade_executor.py
"""
Trade Executor

Executes trades on Polymarket CLOB when edge is identified.
Uses Gamma API prices for edge calculation (not thin CLOB asks).
Posts maker limit orders (0% fees) at our target price.
"""

import logging
import os
from typing import Dict, List, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs
from py_clob_client.order_builder.constants import BUY

logger = logging.getLogger(__name__)


class TradeExecutor:
    """Execute trades on Polymarket"""

    # --- Profitability filters ---
    MAX_BUY_PRICE = 0.85   # Never buy above 85¢ (need room for profit)
    MIN_BUY_PRICE = 0.02   # Ignore dust / nearly-resolved markets
    MIN_EDGE_ROC = 0.05    # Minimum 5% return-on-capital
    MAX_BELIEVABLE_EDGE = 0.50  # Cap edge at 50% — above this, assume model error
    MIN_GAMMA_PRICE = 0.02  # Skip markets with gamma < 2% (basically resolved NO)
    MAX_GAMMA_PRICE = 0.98  # Skip markets with gamma > 98% (basically resolved YES)

    def __init__(
        self,
        private_key: str,
        funder: str,
        host: str = "https://clob.polymarket.com",
        chain_id: int = 137,
        signature_type: int = 2,
        derive_creds_if_missing: bool = True,
    ):
        """
        IMPORTANT:
        - Trading requires USER (L2) API creds, not Builder creds.
        - py_clob_client expects ApiCreds (object), not a dict.

        Env vars supported (choose one convention and stick to it):
          Preferred:
            CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE
          Backwards compatible:
            PM_API_KEY, PM_API_SECRET, PM_API_PASSPHRASE
        """

        if not private_key:
            raise ValueError("Missing private_key")
        if not funder:
            raise ValueError("Missing funder (proxy wallet / funder address)")

        # Create client WITHOUT creds first
        self.client = ClobClient(
            host=host,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
        )

        # Load USER (L2) creds if provided
        api_key = os.getenv("CLOB_API_KEY") or os.getenv("PM_API_KEY")
        api_secret = os.getenv("CLOB_SECRET") or os.getenv("PM_API_SECRET")
        api_passphrase = os.getenv("CLOB_PASS_PHRASE") or os.getenv("PM_API_PASSPHRASE")

        if api_key and api_secret and api_passphrase:
            # FIX: must be ApiCreds object (not dict) so client can access .api_key etc.
            self.client.set_api_creds(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
            )
            logger.info("Loaded USER API creds from env vars.")
        else:
            if not derive_creds_if_missing:
                raise ValueError(
                    "Missing USER API creds. Set CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE "
                    "(or PM_API_KEY/PM_API_SECRET/PM_API_PASSPHRASE), or enable derivation."
                )
            # Recommended flow: derive USER creds for this signer/funder/signature_type context
            derived = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(derived)
            logger.info("Derived USER API creds via private key.")

        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
        logger.info(
            f"TradeExecutor initialized (DRY_RUN={self.dry_run}, signature_type={signature_type}, chain_id={chain_id})"
        )

    def _get_gamma_prices(self, market: Dict) -> Optional[Dict]:
        """
        Get market prices from Gamma API data (already fetched by scanner).

        These are the REAL market-implied prices that Polymarket displays,
        NOT the thin CLOB orderbook asks.
        """
        outcome_prices = market.get("outcome_prices", [])
        outcomes = market.get("outcomes", [])
        clob_token_ids = market.get("clob_token_ids", [])

        if len(outcome_prices) < 2 or len(clob_token_ids) < 2:
            return None

        yes_price = outcome_prices[0]
        no_price = outcome_prices[1]

        # Sanity check: prices should roughly sum to 1.0
        if yes_price + no_price < 0.5 or yes_price + no_price > 1.5:
            logger.warning(f"Suspicious gamma prices: YES={yes_price}, NO={no_price}")
            return None

        # Skip if both prices are 0 (no market data yet)
        if yes_price == 0 and no_price == 0:
            return None

        # Skip nearly-resolved markets (one side is basically done)
        if yes_price < self.MIN_GAMMA_PRICE and no_price > self.MAX_GAMMA_PRICE:
            logger.debug(f"Skipping resolved market: YES={yes_price:.3f}")
            return None
        if no_price < self.MIN_GAMMA_PRICE and yes_price > self.MAX_GAMMA_PRICE:
            logger.debug(f"Skipping resolved market: NO={no_price:.3f}")
            return None

        return {
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_token_id": clob_token_ids[0],
            "no_token_id": clob_token_ids[1],
            "outcomes": outcomes,
        }

    async def calculate_edge(self, market: Dict, forecast_probability: float) -> Optional[Dict]:
        """
        Calculate edge using Gamma API prices (the actual market-implied probability).

        Edge = expected_return_pct on capital risked.

        For buying YES at price P with win probability W:
            EV  = W * 1.0 - P
            Edge = EV / P
        """
        prices = self._get_gamma_prices(market)

        if not prices:
            logger.debug(f"No gamma prices for: {market.get('question', '')[:50]}")
            return None

        yes_price = prices["yes_price"]
        no_price = prices["no_price"]

        # --- YES side: buy YES, win if forecast is right ---
        ev_yes = forecast_probability * 1.0 - yes_price
        edge_yes = ev_yes / yes_price if yes_price > 0 else -999
        yes_tradeable = (
            self.MIN_BUY_PRICE <= yes_price <= self.MAX_BUY_PRICE
            and edge_yes > 0
        )

        # --- NO side: buy NO, win if forecast is wrong ---
        ev_no = (1 - forecast_probability) * 1.0 - no_price
        edge_no = ev_no / no_price if no_price > 0 else -999
        no_tradeable = (
            self.MIN_BUY_PRICE <= no_price <= self.MAX_BUY_PRICE
            and edge_no > 0
        )

        logger.info(
            f"   γ YES={yes_price:.3f} edge={edge_yes:+.1%} {'✓' if yes_tradeable else '✗'} | "
            f"γ NO={no_price:.3f} edge={edge_no:+.1%} {'✓' if no_tradeable else '✗'} | "
            f"forecast={forecast_probability:.1%}"
        )

        # Pick the better tradeable side
        if yes_tradeable and no_tradeable:
            if edge_yes >= edge_no:
                side, edge, price, token_id = "YES", edge_yes, yes_price, prices["yes_token_id"]
            else:
                side, edge, price, token_id = "NO", edge_no, no_price, prices["no_token_id"]
        elif yes_tradeable:
            side, edge, price, token_id = "YES", edge_yes, yes_price, prices["yes_token_id"]
        elif no_tradeable:
            side, edge, price, token_id = "NO", edge_no, no_price, prices["no_token_id"]
        else:
            logger.info("   ⛔ No tradeable side")
            return None

        # Edge must meet minimum threshold
        if edge < self.MIN_EDGE_ROC:
            logger.info(f"   ⛔ Edge {edge:.1%} below minimum {self.MIN_EDGE_ROC:.0%}")
            return None

        # Sanity cap: if edge is implausibly large, our model is probably wrong
        if edge > self.MAX_BELIEVABLE_EDGE:
            logger.info(
                f"   ⚠️  Edge {edge:.1%} exceeds believable max — capping to "
                f"{self.MAX_BELIEVABLE_EDGE:.0%} (model may be wrong)"
            )
            edge = self.MAX_BELIEVABLE_EDGE

        return {
            "market": market,
            "side": side,
            "edge": edge,
            "gamma_price": price,
            "token_id": token_id,
            "forecast_prob": forecast_probability,
            "market_prob": yes_price,
        }

    def _calculate_bid_price(self, gamma_price: float, forecast_prob: float, side: str, edge: float) -> float:
        """
        Calculate our limit bid price.

        Strategy:
        - High confidence (edge 5-20%): bid close to gamma, get filled more often
        - Medium confidence (edge 20-40%): bid below gamma, be patient
        - Capped edge (model may be wrong): bid well below gamma, only fill if truly cheap
        """
        if side == "YES":
            fair_value = forecast_prob
        else:
            fair_value = 1 - forecast_prob

        # Dynamic spread based on confidence
        if edge >= self.MAX_BELIEVABLE_EDGE:
            spread = 0.05
        elif edge > 0.20:
            spread = 0.03
        else:
            spread = 0.02

        our_bid = min(gamma_price, fair_value - spread)
        our_bid = round(our_bid, 2)
        our_bid = max(self.MIN_BUY_PRICE, min(self.MAX_BUY_PRICE, our_bid))
        return our_bid

    async def execute_trade(self, token_id: str, side: str, price: float, size_usd: float) -> Optional[str]:
        """
        Post a GTC limit order (maker, 0% fees).
        """
        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would BUY {side} ${size_usd:.2f} at {price:.3f} on token {token_id[:16]}..."
            )
            return f"dry_run_{token_id[:8]}"

        try:
            if price <= 0:
                raise ValueError(f"Invalid price={price}")

            size_shares = size_usd / price

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size_shares,
                side=BUY,
            )

            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order)

            # Some versions return orderID, others orderId; handle both.
            order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            logger.info(f"✅ Posted {side} limit ${size_usd:.2f} at {price:.3f} → Order {order_id}")

            return order_id

        except Exception as e:
            logger.error(f"Error posting order: {e}")
            return None

    async def execute_opportunities(
        self,
        opportunities: List[Dict],
        position_size: float,
        min_edge: float = 0.05,
        max_positions: int = 10,
    ) -> List[Dict]:
        """
        Execute trades for all opportunities with sufficient edge.

        Uses Gamma API prices for edge calculation.
        Posts GTC limit orders at our target bid price (maker, 0% fees).
        """
        executed: List[Dict] = []

        def _opportunity_score(opp: Dict) -> float:
            hours = opp.get("hours_until_settlement", 24)
            gamma_prices = opp["market"].get("outcome_prices", [])

            time_score = min(hours / 24.0, 1.5)

            if gamma_prices and len(gamma_prices) >= 2:
                max_price = max(gamma_prices)
                min_price = min(gamma_prices)
                liquidity_score = min(min_price, 1 - max_price) * 10
            else:
                liquidity_score = 0.5

            return time_score + liquidity_score

        opportunities.sort(key=_opportunity_score, reverse=True)

        for opp in opportunities:
            if len(executed) >= max_positions:
                logger.info(f"Reached max positions ({max_positions}), stopping")
                break

            try:
                edge_analysis = await self.calculate_edge(
                    market=opp["market"],
                    forecast_probability=opp["forecast_probability"],
                )
                if not edge_analysis:
                    continue

                edge = edge_analysis["edge"]
                if edge < min_edge:
                    logger.info(f"Edge {edge:.1%} below threshold {min_edge:.1%}, skipping")
                    continue

                bid_price = self._calculate_bid_price(
                    gamma_price=edge_analysis["gamma_price"],
                    forecast_prob=opp["forecast_probability"],
                    side=edge_analysis["side"],
                    edge=edge,
                )

                logger.info(f"📊 {opp['market']['question'][:60]}")
                logger.info(
                    f"   Forecast: {opp['forecast_probability']:.1%} | "
                    f"Market: {edge_analysis['market_prob']:.1%} | "
                    f"Edge: {edge:+.1%} | "
                    f"Bid: {bid_price:.2f}"
                )

                order_id = await self.execute_trade(
                    token_id=edge_analysis["token_id"],
                    side=edge_analysis["side"],
                    price=bid_price,
                    size_usd=position_size,
                )

                if order_id:
                    executed.append(
                        {
                            "order_id": order_id,
                            "market": opp["market"]["question"],
                            "side": edge_analysis["side"],
                            "edge": edge,
                            "gamma_price": edge_analysis["gamma_price"],
                            "bid_price": bid_price,
                            "size_usd": position_size,
                            "forecast_prob": opp["forecast_probability"],
                        }
                    )

            except Exception as e:
                logger.error(f"Error processing opportunity: {e}")
                continue

        logger.info(f"\n{'='*80}")
        logger.info(f"Executed {len(executed)} trades")
        logger.info(f"{'='*80}\n")

        return executed


async def test_executor():
    """Test the trade executor"""
    from dotenv import load_dotenv
    from market_scanner import MarketScanner
    from probability_calculator import ProbabilityCalculator

    load_dotenv()

    scanner = MarketScanner()
    markets = await scanner.get_weather_markets(max_hours_until_settlement=24)

    calc = ProbabilityCalculator()
    opportunities = await calc.find_opportunities(markets[:5])

    executor = TradeExecutor(
        private_key=os.getenv("PM_PRIVATE_KEY"),
        funder=os.getenv("PM_FUNDER"),
        host=os.getenv("CLOB_HOST", "https://clob.polymarket.com"),
        chain_id=int(os.getenv("CHAIN_ID", "137")),
        signature_type=int(os.getenv("SIGNATURE_TYPE", "2")),
    )

    executed = await executor.execute_opportunities(
        opportunities=opportunities,
        position_size=float(os.getenv("POSITION_SIZE_USD", "10.0")),
        min_edge=float(os.getenv("MIN_EDGE_PERCENT", "10.0")) / 100,
        max_positions=int(os.getenv("MAX_POSITIONS", "10")),
    )

    print(f"\nExecuted {len(executed)} trades")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_executor())
