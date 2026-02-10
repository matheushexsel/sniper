"""
Probability Calculator

Compares NWS/weather forecast probabilities to Polymarket prices
to identify arbitrage opportunities
"""

import logging
from typing import Dict, Optional, List
from datetime import datetime
from nws_fetcher import NWSFetcher, OpenMeteoFetcher
from city_coords import get_city_info

logger = logging.getLogger(__name__)


class ProbabilityCalculator:
    """Calculate true probabilities and compare to market prices"""
    
    def __init__(self):
        self.nws = NWSFetcher()
        self.open_meteo = OpenMeteoFetcher()
    
    async def analyze_market(self, market: Dict) -> Optional[Dict]:
        """
        Analyze a market and return edge calculation.
        
        Args:
            market: Market dict from scanner with parsed metadata
            
        Returns:
            Dict with analysis or None if can't analyze
        """
        city = market.get("city")
        if not city:
            logger.warning(f"No city found for market: {market['question'][:50]}")
            return None
        
        # Get city coordinates
        city_info = get_city_info(city)
        if not city_info:
            logger.warning(f"No coordinates for city: {city}")
            return None
        
        # Convert temperature to Fahrenheit if needed
        temp_min = market["temp_min"]
        temp_max = market["temp_max"]
        temp_unit = market["temp_unit"]
        
        if temp_unit == "C":
            temp_min_f = self._celsius_to_fahrenheit(temp_min)
            temp_max_f = self._celsius_to_fahrenheit(temp_max)
        else:
            temp_min_f = temp_min
            temp_max_f = temp_max
        
        # Get weather forecast probability
        target_date = market["end_date"]
        
        if city_info["type"] == "nws":
            # US city - use NWS
            probability = await self.nws.get_temperature_probability(
                office=city_info["office"],
                grid_x=city_info["grid_x"],
                grid_y=city_info["grid_y"],
                target_date=target_date,
                temp_min_f=temp_min_f,
                temp_max_f=temp_max_f,
            )
        else:
            # International city - use Open-Meteo
            probability = await self.open_meteo.get_temperature_probability(
                lat=city_info["lat"],
                lon=city_info["lon"],
                target_date=target_date,
                temp_min_f=temp_min_f,
                temp_max_f=temp_max_f,
            )
        
        if probability is None:
            logger.warning(f"Could not get forecast for {city}")
            return None
        
        # Get market prices
        # For now, we'll need to fetch orderbook to get actual prices
        # This is a simplified version - we'll enhance in trade_executor.py
        
        return {
            "market": market,
            "city": city,
            "temp_range": f"{temp_min}-{temp_max}°{temp_unit}",
            "forecast_probability": probability,
            "settlement_date": target_date,
            "hours_until_settlement": market["hours_until_settlement"],
        }
    
    async def find_opportunities(
        self, 
        markets: List[Dict], 
        min_edge: float = 0.05
    ) -> List[Dict]:
        """
        Analyze multiple markets and return those with edge.
        
        Args:
            markets: List of market dicts from scanner
            min_edge: Minimum edge required (default 5%)
            
        Returns:
            List of opportunities sorted by edge
        """
        opportunities = []
        
        for market in markets:
            try:
                analysis = await self.analyze_market(market)
                
                if not analysis:
                    continue
                
                # For now, we'll add all analyzed markets
                # Real edge calculation happens in trade_executor when we have prices
                opportunities.append(analysis)
                
            except Exception as e:
                logger.error(f"Error analyzing market: {e}")
                continue
        
        logger.info(f"Analyzed {len(markets)} markets, found {len(opportunities)} with forecasts")
        
        return opportunities
    
    def _celsius_to_fahrenheit(self, celsius: float) -> float:
        """Convert Celsius to Fahrenheit"""
        return (celsius * 9/5) + 32
    
    def _fahrenheit_to_celsius(self, fahrenheit: float) -> float:
        """Convert Fahrenheit to Celsius"""
        return (fahrenheit - 32) * 5/9


async def test_calculator():
    """Test the probability calculator"""
    from market_scanner import MarketScanner
    
    # Get markets
    scanner = MarketScanner()
    markets = await scanner.get_weather_markets(max_hours_until_settlement=24)
    
    # Analyze them
    calc = ProbabilityCalculator()
    opportunities = await calc.find_opportunities(markets)
    
    print(f"\n{'='*80}")
    print(f"Found {len(opportunities)} markets with forecast data")
    print(f"{'='*80}\n")
    
    for i, opp in enumerate(opportunities[:5], 1):
        print(f"{i}. {opp['market']['question'][:70]}")
        print(f"   City: {opp['city']}")
        print(f"   Range: {opp['temp_range']}")
        print(f"   Forecast Probability: {opp['forecast_probability']:.1%}")
        print(f"   Settles in: {opp['hours_until_settlement']:.1f}h")
        print()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_calculator())
