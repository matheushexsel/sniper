# probability_calculator.py
"""
Probability Calculator

Compares weather forecast probabilities to Polymarket prices.
Now passes timezone and settlement timing info for dynamic σ calculation.
"""

import logging
from typing import Dict, Optional, List
from datetime import datetime
from nws_fetcher import NWSFetcher, OpenMeteoFetcher, reset_forecast_cache
from city_coords import get_city_info

logger = logging.getLogger(__name__)


class ProbabilityCalculator:
    """Calculate true probabilities and compare to market prices"""
    
    def __init__(self):
        self.nws = NWSFetcher()
        self.open_meteo = OpenMeteoFetcher()
    
    async def analyze_market(self, market: Dict) -> Optional[Dict]:
        """
        Analyze a single market and return forecast probability.
        """
        city = market.get("city")
        if not city:
            logger.warning(f"No city found for market: {market['question'][:50]}")
            return None
        
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
        
        target_date = market["end_date"]
        hours_until = market.get("hours_until_settlement", 24.0)
        timezone_str = city_info.get("timezone", "UTC")
        
        if city_info["type"] == "nws":
            probability = await self.nws.get_temperature_probability(
                office=city_info["office"],
                grid_x=city_info["grid_x"],
                grid_y=city_info["grid_y"],
                target_date=target_date,
                temp_min_f=temp_min_f,
                temp_max_f=temp_max_f,
                timezone_str=timezone_str,
                hours_until_settlement=hours_until,
            )
        else:
            probability = await self.open_meteo.get_temperature_probability(
                lat=city_info["lat"],
                lon=city_info["lon"],
                target_date=target_date,
                temp_min_f=temp_min_f,
                temp_max_f=temp_max_f,
                timezone_str=timezone_str,
                hours_until_settlement=hours_until,
            )
        
        if probability is None:
            logger.warning(f"Could not get forecast for {city}")
            return None
        
        return {
            "market": market,
            "city": city,
            "temp_range": f"{temp_min}-{temp_max}°{temp_unit}",
            "forecast_probability": probability,
            "settlement_date": target_date,
            "hours_until_settlement": hours_until,
        }
    
    async def find_opportunities(
        self, 
        markets: List[Dict], 
        min_edge: float = 0.05
    ) -> List[Dict]:
        """
        Analyze multiple markets and return those with forecasts.
        Resets forecast cache at the start of each scan.
        """
        # Reset cache so each scan gets fresh data
        reset_forecast_cache()
        
        opportunities = []
        
        for market in markets:
            try:
                analysis = await self.analyze_market(market)
                if analysis:
                    opportunities.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing market: {e}")
                continue
        
        logger.info(f"Analyzed {len(markets)} markets, found {len(opportunities)} with forecasts")
        
        return opportunities
    
    def _celsius_to_fahrenheit(self, celsius: float) -> float:
        return (celsius * 9/5) + 32
    
    def _fahrenheit_to_celsius(self, fahrenheit: float) -> float:
        return (fahrenheit - 32) * 5/9
