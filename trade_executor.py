"""
Trade Executor

Executes trades on Polymarket CLOB when edge is identified
"""

import logging
import os
from typing import Dict, List, Optional
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

logger = logging.getLogger(__name__)


class TradeExecutor:
    """Execute trades on Polymarket"""
    
    def __init__(self, private_key: str, funder: str, host: str = "https://clob.polymarket.com"):
        self.client = ClobClient(
            host=host,
            key=private_key,
            chain_id=137,  # Polygon
            signature_type=2,
            funder=funder,
        )
        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
        logger.info(f"TradeExecutor initialized (DRY_RUN={self.dry_run})")
    
    async def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """Get orderbook for a token"""
        try:
            book = self.client.get_order_book(token_id)
            return book
        except Exception as e:
            logger.error(f"Error fetching orderbook for {token_id}: {e}")
            return None
    
    async def get_market_prices(self, market: Dict) -> Optional[Dict]:
        """
        Get current market prices for YES and NO.
        
        Returns:
            Dict with yes_price, no_price, yes_token_id, no_token_id
        """
        clob_token_ids = market.get("clob_token_ids", [])
        outcomes = market.get("outcomes", [])
        
        # Basic validation
        if len(clob_token_ids) < 2 or len(outcomes) < 2:
            return None
        
        # Get token IDs
        yes_token_id = clob_token_ids[0]
        no_token_id = clob_token_ids[1]
        
        # Get orderbooks
        yes_book = await self.get_orderbook(yes_token_id)
        no_book = await self.get_orderbook(no_token_id)
        
        if not yes_book or not no_book:
            return None
        
        # Get best prices
        yes_asks = yes_book.get("asks", [])
        no_asks = no_book.get("asks", [])
        
        if not yes_asks or not no_asks:
            return None
        
        # Best ask price is what we'd pay to buy
        yes_price = float(yes_asks[0]["price"]) if yes_asks else None
        no_price = float(no_asks[0]["price"]) if no_asks else None
        
        if yes_price is None or no_price is None:
            return None
        
        return {
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
            "outcomes": outcomes,
        }
    
    async def calculate_edge(self, market: Dict, forecast_probability: float) -> Optional[Dict]:
        """
        Calculate edge for a market.
        
        Args:
            market: Market dict
            forecast_probability: True probability from weather forecast
            
        Returns:
            Dict with edge analysis or None
        """
        prices = await self.get_market_prices(market)
        
        if not prices:
            return None
        
        yes_price = prices["yes_price"]
        no_price = prices["no_price"]
        
        # Implied probability from market price
        market_prob_yes = yes_price
        market_prob_no = 1 - yes_price  # or could use no_price
        
        # Calculate edge for YES and NO
        edge_yes = forecast_probability - market_prob_yes
        edge_no = (1 - forecast_probability) - (1 - yes_price)
        
        # Determine best side to bet
        if abs(edge_yes) > abs(edge_no):
            side = "YES"
            edge = edge_yes
            price = yes_price
            token_id = prices["yes_token_id"]
        else:
            side = "NO"
            edge = edge_no
            price = no_price
            token_id = prices["no_token_id"]
        
        return {
            "market": market,
            "side": side,
            "edge": edge,
            "price": price,
            "token_id": token_id,
            "forecast_prob": forecast_probability,
            "market_prob": market_prob_yes,
        }
    
    async def execute_trade(
        self, 
        token_id: str, 
        side: str, 
        price: float, 
        size_usd: float
    ) -> Optional[str]:
        """
        Execute a trade.
        
        Args:
            token_id: Token to trade
            side: "YES" or "NO"
            price: Limit price (0.0 to 1.0)
            size_usd: Size in USD
            
        Returns:
            Order ID or None if failed
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would {side} ${size_usd:.2f} at {price:.3f} on token {token_id[:16]}...")
            return f"dry_run_{token_id[:8]}"
        
        try:
            # Calculate size in shares
            # For YES: shares = usd / price
            # For NO: shares = usd / (1 - price)
            if side == "YES":
                size_shares = size_usd / price
            else:
                size_shares = size_usd / (1 - price)
            
            # Build order
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size_shares,
                side=BUY,
                order_type=OrderType.GTC,  # Good-til-cancel for 0% fees
            )
            
            # Create and sign order
            signed_order = self.client.create_order(order_args)
            
            # Post order
            resp = self.client.post_order(signed_order)
            
            order_id = resp.get("orderID")
            
            logger.info(f"✅ Executed {side} ${size_usd:.2f} at {price:.3f} → Order {order_id}")
            
            return order_id
            
        except Exception as e:
            logger.error(f"Error executing trade: {e}")
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
        
        Args:
            opportunities: List from probability_calculator
            position_size: USD per position
            min_edge: Minimum edge required (default 5%)
            max_positions: Maximum number of positions to open
            
        Returns:
            List of executed trades
        """
        executed = []
        
        for opp in opportunities:
            if len(executed) >= max_positions:
                logger.info(f"Reached max positions ({max_positions}), stopping")
                break
            
            try:
                # Calculate edge with current prices
                edge_analysis = await self.calculate_edge(
                    market=opp["market"],
                    forecast_probability=opp["forecast_probability"],
                )
                
                if not edge_analysis:
                    continue
                
                edge = edge_analysis["edge"]
                
                # Check if edge meets threshold
                if abs(edge) < min_edge:
                    logger.info(f"Edge {edge:.1%} below threshold {min_edge:.1%}, skipping")
                    continue
                
                # Execute trade
                logger.info(f"📊 {opp['market']['question'][:60]}")
                logger.info(f"   Forecast: {opp['forecast_probability']:.1%} | Market: {edge_analysis['market_prob']:.1%} | Edge: {edge:+.1%}")
                
                order_id = await self.execute_trade(
                    token_id=edge_analysis["token_id"],
                    side=edge_analysis["side"],
                    price=edge_analysis["price"],
                    size_usd=position_size,
                )
                
                if order_id:
                    executed.append({
                        "order_id": order_id,
                        "market": opp["market"]["question"],
                        "side": edge_analysis["side"],
                        "edge": edge,
                        "price": edge_analysis["price"],
                        "size_usd": position_size,
                        "forecast_prob": opp["forecast_probability"],
                    })
                
            except Exception as e:
                logger.error(f"Error processing opportunity: {e}")
                continue
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Executed {len(executed)} trades")
        logger.info(f"{'='*80}\n")
        
        return executed


async def test_executor():
    """Test the trade executor"""
    import os
    from dotenv import load_dotenv
    from market_scanner import MarketScanner
    from probability_calculator import ProbabilityCalculator
    
    load_dotenv()
    
    # Get opportunities
    scanner = MarketScanner()
    markets = await scanner.get_weather_markets(max_hours_until_settlement=24)
    
    calc = ProbabilityCalculator()
    opportunities = await calc.find_opportunities(markets[:5])  # Test with first 5
    
    # Execute
    executor = TradeExecutor(
        private_key=os.getenv("PM_PRIVATE_KEY"),
        funder=os.getenv("PM_FUNDER"),
    )
    
    executed = await executor.execute_opportunities(
        opportunities=opportunities,
        position_size=float(os.getenv("POSITION_SIZE_USD", "10.0")),
        min_edge=float(os.getenv("MIN_EDGE_PERCENT", "5.0")) / 100,
        max_positions=int(os.getenv("MAX_POSITIONS", "10")),
    )
    
    print(f"\nExecuted {len(executed)} trades")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_executor())
