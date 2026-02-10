"""
Weather Arbitrage Bot - Main Entry Point

Runs continuously, scanning for opportunities and executing trades
"""

import asyncio
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

from market_scanner import MarketScanner
from probability_calculator import ProbabilityCalculator
from trade_executor import TradeExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


class WeatherArbBot:
    """Main bot orchestrator"""
    
    def __init__(self):
        # Load config
        load_dotenv()
        
        self.position_size = float(os.getenv("POSITION_SIZE_USD", "10.0"))
        self.max_positions = int(os.getenv("MAX_POSITIONS", "10"))
        self.min_edge = float(os.getenv("MIN_EDGE_PERCENT", "5.0")) / 100
        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
        
        # Initialize components
        self.scanner = MarketScanner()
        self.calculator = ProbabilityCalculator()
        self.executor = TradeExecutor(
            private_key=os.getenv("PM_PRIVATE_KEY"),
            funder=os.getenv("PM_FUNDER"),
        )
        
        logger.info("="*80)
        logger.info("WEATHER ARBITRAGE BOT")
        logger.info("="*80)
        logger.info(f"Position Size: ${self.position_size}")
        logger.info(f"Max Positions: {self.max_positions}")
        logger.info(f"Min Edge: {self.min_edge:.1%}")
        logger.info(f"DRY RUN: {self.dry_run}")
        logger.info("="*80)
    
    async def run_scan(self):
        """Run one complete scan cycle"""
        logger.info(f"\n🔍 Starting scan at {datetime.now().strftime('%H:%M:%S')}")
        
        try:
            # Step 1: Find weather markets settling within 24 hours
            markets = await self.scanner.get_weather_markets(max_hours_until_settlement=24)
            
            if not markets:
                logger.info("No weather markets found")
                return
            
            logger.info(f"Found {len(markets)} weather markets")
            
            # Step 2: Calculate probabilities from weather forecasts
            opportunities = await self.calculator.find_opportunities(markets)
            
            if not opportunities:
                logger.info("No opportunities with weather data")
                return
            
            logger.info(f"Analyzed {len(opportunities)} markets with forecast data")
            
            # Step 3: Execute trades where edge exists
            executed = await self.executor.execute_opportunities(
                opportunities=opportunities,
                position_size=self.position_size,
                min_edge=self.min_edge,
                max_positions=self.max_positions,
            )
            
            if executed:
                logger.info(f"✅ Executed {len(executed)} trades:")
                for trade in executed:
                    logger.info(f"   • {trade['side']} ${trade['size_usd']:.0f} @ {trade['price']:.3f} | Edge: {trade['edge']:+.1%}")
                    logger.info(f"     {trade['market'][:70]}")
            else:
                logger.info("No trades executed (insufficient edge or max positions reached)")
        
        except Exception as e:
            logger.error(f"Error during scan: {e}", exc_info=True)
    
    async def run_forever(self, scan_interval_minutes: int = 60):
        """
        Run bot continuously.
        
        Args:
            scan_interval_minutes: How often to scan (default 60 min = hourly)
        """
        logger.info(f"🚀 Bot started - scanning every {scan_interval_minutes} minutes")
        
        while True:
            try:
                await self.run_scan()
                
                # Wait for next scan
                logger.info(f"\n⏸️  Sleeping for {scan_interval_minutes} minutes...")
                await asyncio.sleep(scan_interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("\n👋 Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Fatal error: {e}", exc_info=True)
                logger.info("Restarting in 5 minutes...")
                await asyncio.sleep(300)
    
    async def run_once(self):
        """Run a single scan and exit (useful for testing)"""
        await self.run_scan()
        logger.info("\n✅ Single scan complete")


async def main():
    """Main entry point"""
    bot = WeatherArbBot()
    
    # Check if we should run once or continuously
    run_mode = os.getenv("RUN_MODE", "continuous").lower()
    
    if run_mode == "once":
        await bot.run_once()
    else:
        # Run forever with hourly scans
        scan_interval = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
        await bot.run_forever(scan_interval_minutes=scan_interval)


if __name__ == "__main__":
    asyncio.run(main())
