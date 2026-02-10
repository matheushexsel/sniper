"""
Polymarket Market Scanner

Scans for temperature markets settling within 24 hours
"""

import httpx
import logging
import re
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import dateutil.parser

logger = logging.getLogger(__name__)


class MarketScanner:
    """Find and filter weather markets on Polymarket"""
    
    GAMMA_BASE = "https://gamma-api.polymarket.com"
    
    async def fetch_active_markets(self, limit: int = 200) -> List[Dict]:
        """Fetch active markets from Gamma API"""
        url = f"{self.GAMMA_BASE}/markets"
        params = {
            "limit": str(limit),
            "active": "true",
            "closed": "false",
        }
        
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    markets = response.json()
                    logger.info(f"Fetched {len(markets)} active markets from Gamma")
                    return markets
                else:
                    logger.error(f"Gamma API error: {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []
    
    async def get_weather_markets(self, max_hours_until_settlement: int = 24) -> List[Dict]:
        """
        Get all temperature markets settling within specified hours.
        
        Args:
            max_hours_until_settlement: Only return markets settling within this many hours
            
        Returns:
            List of market dicts with parsed metadata
        """
        all_markets = await self.fetch_active_markets(limit=200)
        
        weather_markets = []
        now = datetime.now()
        cutoff = now + timedelta(hours=max_hours_until_settlement)
        
        for market in all_markets:
            try:
                question = market.get("question", "")
                
                # Filter for temperature markets
                if not self._is_temperature_market(question):
                    continue
                
                # Check settlement time
                end_date_str = market.get("endDate") or market.get("end_date_iso")
                if not end_date_str:
                    continue
                
                try:
                    end_date = dateutil.parser.parse(end_date_str)
                except Exception:
                    continue
                
                # Skip if settling too far in future
                if end_date > cutoff:
                    continue
                
                # Skip if already settled
                if end_date < now:
                    continue
                
                # Parse temperature range from question
                temp_range = self._parse_temperature_range(question)
                if not temp_range:
                    continue
                
                # Parse city from question
                city = self._parse_city(question)
                
                # Build enriched market object
                enriched = {
                    "question": question,
                    "slug": market.get("slug", ""),
                    "market_id": market.get("id", ""),
                    "clob_token_ids": market.get("clobTokenIds", []),
                    "outcomes": market.get("outcomes", []),
                    "end_date": end_date,
                    "volume": market.get("volume", 0),
                    "liquidity": market.get("liquidity", 0),
                    "enable_order_book": market.get("enableOrderBook", True),
                    
                    # Parsed metadata
                    "city": city,
                    "temp_min": temp_range["min"],
                    "temp_max": temp_range["max"],
                    "temp_unit": temp_range["unit"],
                    "hours_until_settlement": (end_date - now).total_seconds() / 3600,
                }
                
                weather_markets.append(enriched)
                
            except Exception as e:
                logger.warning(f"Error parsing market: {e}")
                continue
        
        logger.info(f"Found {len(weather_markets)} temperature markets settling within {max_hours_until_settlement}h")
        
        # Sort by settlement time (soonest first)
        weather_markets.sort(key=lambda m: m["end_date"])
        
        return weather_markets
    
    def _is_temperature_market(self, question: str) -> bool:
        """Check if question is about temperature"""
        q_lower = question.lower()
        
        # Must contain temperature-related keywords
        temp_keywords = ["temperature", "°f", "°c", "degrees"]
        if not any(kw in q_lower for kw in temp_keywords):
            return False
        
        # Must contain location keywords
        location_keywords = ["in", "on", "at"]
        if not any(kw in q_lower for kw in location_keywords):
            return False
        
        # Exclude non-weather markets
        exclude_keywords = ["cpu", "processor", "gaming", "oven", "water"]
        if any(kw in q_lower for kw in exclude_keywords):
            return False
        
        return True
    
    def _parse_temperature_range(self, question: str) -> Optional[Dict]:
        """
        Parse temperature range from question.
        
        Examples:
            "Highest temperature in NYC on February 10?" with outcome "26-27°F"
            "Will NYC hit 85°F+ on Feb 15?"
            "Temperature below 0°C in London"
        
        Returns:
            Dict with min, max, unit or None
        """
        # Pattern 1: Range format "XX-YY°F" or "XX-YY°C"
        range_pattern = r'(\d+)-(\d+)\s*°([FC])'
        match = re.search(range_pattern, question)
        if match:
            min_temp = float(match.group(1))
            max_temp = float(match.group(2))
            unit = match.group(3)
            return {"min": min_temp, "max": max_temp, "unit": unit}
        
        # Pattern 2: Single temp with operator "85°F+" or "below 0°C"
        single_pattern = r'(\d+)\s*°([FC])\s*(\+|or\s+above|or\s+higher)'
        match = re.search(single_pattern, question, re.IGNORECASE)
        if match:
            temp = float(match.group(1))
            unit = match.group(2)
            return {"min": temp, "max": temp + 100, "unit": unit}  # Upper bound for "+"
        
        below_pattern = r'(below|under)\s+(\d+)\s*°([FC])'
        match = re.search(below_pattern, question, re.IGNORECASE)
        if match:
            temp = float(match.group(2))
            unit = match.group(3)
            return {"min": -100, "max": temp, "unit": unit}  # Lower bound for "below"
        
        # Pattern 3: Exact temp "33°F"
        exact_pattern = r'(\d+)\s*°([FC])(?!\s*(?:-|\+|or))'
        match = re.search(exact_pattern, question)
        if match:
            temp = float(match.group(1))
            unit = match.group(2)
            # For exact temps, assume ±1 degree bucket
            return {"min": temp, "max": temp + 1, "unit": unit}
        
        return None
    
    def _parse_city(self, question: str) -> Optional[str]:
        """Extract city name from question"""
        # Common cities we support
        cities = [
            "NYC", "New York", "Chicago", "Seattle", "Dallas", "Miami",
            "Los Angeles", "Atlanta", "Boston", "Denver", "Phoenix",
            "London", "Seoul", "Toronto", "Buenos Aires", "Wellington",
            "Ankara"
        ]
        
        q_lower = question.lower()
        
        for city in cities:
            if city.lower() in q_lower:
                # Normalize to our standard city names
                if city in ["New York", "NYC"]:
                    return "NYC"
                return city
        
        return None


async def test_scanner():
    """Test the market scanner"""
    scanner = MarketScanner()
    markets = await scanner.get_weather_markets(max_hours_until_settlement=48)
    
    print(f"\n{'='*80}")
    print(f"Found {len(markets)} weather markets")
    print(f"{'='*80}\n")
    
    for i, market in enumerate(markets[:10], 1):
        print(f"{i}. {market['question'][:70]}")
        print(f"   City: {market['city']}")
        print(f"   Range: {market['temp_min']}-{market['temp_max']}°{market['temp_unit']}")
        print(f"   Settles: {market['end_date'].strftime('%Y-%m-%d %H:%M')} ({market['hours_until_settlement']:.1f}h)")
        print(f"   Volume: ${market['volume']:,.0f}")
        print()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_scanner())
