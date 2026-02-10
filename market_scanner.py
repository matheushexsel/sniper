# market_scanner.py
"""
Polymarket Market Scanner

Scans for temperature markets settling within 48 hours
"""

import httpx
import logging
import re
import json  # Added for clob_token_ids parsing
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import dateutil.parser

logger = logging.getLogger(__name__)


class MarketScanner:
    """Find and filter weather markets on Polymarket"""
    
    GAMMA_BASE = "https://gamma-api.polymarket.com"
    
    async def fetch_weather_events_by_slug(self) -> List[Dict]:
        """
        Fetch temperature events by constructing slugs directly.
        Pattern: highest-temperature-in-{city}-on-{month}-{day}-{year}
        """
        # Cities with their slug names
        cities = [
            "nyc", "new-york", "london", "chicago", "seattle", 
            "dallas", "miami", "atlanta", "boston", "denver", 
            "phoenix", "seoul", "toronto", "buenos-aires", 
            "ankara", "wellington", "los-angeles", "la"
        ]
        
        # Get today + next 2 days
        today = datetime.now()
        dates = [(today + timedelta(days=i)) for i in range(3)]
        
        events = []
        seen_ids = set()
        
        for city in cities:
            for date in dates:
                # Format: highest-temperature-in-{city}-on-february-10-2026
                month_name = date.strftime("%B").lower()
                day = date.day
                year = date.year
                
                slug = f"highest-temperature-in-{city}-on-{month_name}-{day}-{year}"
                
                url = f"{self.GAMMA_BASE}/events/slug/{slug}"
                
                try:
                    async with httpx.AsyncClient(timeout=10) as client:
                        response = await client.get(url)
                        
                        if response.status_code == 200:
                            event = response.json()
                            event_id = event.get("id")
                            
                            # Deduplicate (NYC and new-york might return same event)
                            if event_id and event_id not in seen_ids:
                                seen_ids.add(event_id)
                                events.append(event)
                                logger.info(f"✅ {slug}")
                        elif response.status_code == 404:
                            # Event doesn't exist, skip quietly
                            pass
                        else:
                            logger.warning(f"❌ {slug}: HTTP {response.status_code}")
                            
                except Exception as e:
                    logger.warning(f"❌ {slug}: {e}")
                    continue
        
        logger.info(f"\n🎯 Fetched {len(events)} unique weather events")
        return events
    
    async def get_weather_markets(self, max_hours_until_settlement: int = 48) -> List[Dict]:
        """
        Get all temperature markets settling within specified hours.
        
        Fetches events by constructing slugs directly - much faster!
        
        Returns:
            List of market dicts with parsed metadata
        """
        # Fetch events by slug (fast and targeted)
        all_events = await self.fetch_weather_events_by_slug()
        
        weather_markets = []
        now = datetime.now()
        cutoff = now + timedelta(hours=max_hours_until_settlement)
        
        logger.info(f"Processing {len(all_events)} weather events...")
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"Cutoff time: {cutoff.strftime('%Y-%m-%d %H:%M')}")
        
        temp_event_count = 0
        
        for event in all_events:
            try:
                event_title = event.get("title", "")
                
                if not event_title:
                    continue
                
                temp_event_count += 1
                logger.info(f"Processing event #{temp_event_count}: {event_title}")
                
                # Check settlement time
                end_date_str = event.get("endDate") or event.get("end_date_iso")
                if not end_date_str:
                    logger.warning(f"  ❌ No end date")
                    continue
                
                try:
                    end_date = dateutil.parser.parse(end_date_str)
                    # Convert to naive datetime for comparison
                    if end_date.tzinfo is not None:
                        end_date = end_date.replace(tzinfo=None)
                except Exception as e:
                    logger.warning(f"  ❌ Could not parse date '{end_date_str}': {e}")
                    continue
                
                # Skip if settling too far in future
                if end_date > cutoff:
                    logger.info(f"  ⏭️  Too far: settles {end_date.strftime('%Y-%m-%d %H:%M')} (>{max_hours_until_settlement}h)")
                    continue
                
                # Skip if already settled
                if end_date < now:
                    logger.info(f"  ⏭️  Already settled: {end_date.strftime('%Y-%m-%d %H:%M')}")
                    continue
                
                # Parse city from title
                city = self._parse_city(event_title)
                
                # Get all markets (outcomes) from this event
                markets = event.get("markets", [])
                logger.info(f"  ✅ Valid event! City={city}, {len(markets)} markets, settles in {(end_date - now).total_seconds() / 3600:.1f}h")
                
                # Process each market (outcome) in the event
                for market in markets:
                    try:
                        question = market.get("question", "")
                        
                        # Parse temperature range from question
                        temp_range = self._parse_temperature_range(question)
                        if not temp_range:
                            logger.warning(f"    Could not parse temp from: {question[:60]}")
                            continue
                        
                        # Extract token IDs - handle both possible field names
                        clob_token_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []
                        
                        # Parse if it's a JSON string
                        if isinstance(clob_token_ids, str):
                            try:
                                clob_token_ids = json.loads(clob_token_ids)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse clob_token_ids as JSON: {e} - Raw: {clob_token_ids}")
                                clob_token_ids = []
                        
                        # Basic validation - just check they exist
                        if not clob_token_ids or len(clob_token_ids) < 2:
                            logger.warning(f"    ⚠️  Market missing token IDs: {question[:40]}")
                            continue
                        
                        # Build enriched market object
                        enriched = {
                            "event_title": event_title,
                            "question": question,
                            "slug": market.get("slug", ""),
                            "market_id": market.get("id", ""),
                            "clob_token_ids": clob_token_ids,
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
                        logger.info(f"    ✅ {question[:60]} ({temp_range['min']}-{temp_range['max']}{temp_range['unit']})")
                        
                    except Exception as e:
                        logger.warning(f"    Error parsing market: {e}")
                        continue
                
            except Exception as e:
                logger.warning(f"Error parsing event: {e}")
                continue
        
        logger.info(f"\n🎯 Found {len(weather_markets)} temperature markets from {temp_event_count} events")
        
        # Sort by settlement time (soonest first)
        weather_markets.sort(key=lambda m: m["end_date"])
        
        return weather_markets
    
    def _is_temperature_event(self, title: str) -> bool:
        """Check if event title is about temperature"""
        if not title:
            return False
            
        t_lower = title.lower()
        
        # Be VERY permissive - match any temp-related keywords
        temp_keywords = [
            "temperature", "°f", "°c", "degrees",
            "high", "highest", "low", "lowest",
            "hot", "cold", "warm", "cool"
        ]
        
        has_temp = any(kw in t_lower for kw in temp_keywords)
        
        # Also check for city names (indicates it's probably weather)
        cities = [
            "nyc", "new york", "seattle", "chicago", "dallas", 
            "miami", "atlanta", "boston", "denver", "phoenix",
            "london", "seoul", "toronto", "buenos aires", "ankara"
        ]
        has_city = any(city in t_lower for city in cities)
        
        return has_temp and has_city
    
    def _is_temperature_market(self, question: str) -> bool:
        """Check if question is about temperature"""
        q_lower = question.lower()
        
        # Must contain temperature-related keywords
        temp_keywords = ["temperature", "°f", "°c", "degrees", "high", "highest", "low", "lowest"]
        if not any(kw in q_lower for kw in temp_keywords):
            return False
        
        # Exclude non-weather markets
        exclude_keywords = ["cpu", "processor", "gaming", "oven", "water", "stock", "price"]
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
            "33°F or below"
            "44°F or higher"
        
        Returns:
            Dict with min, max, unit or None
        """
        # Pattern 1: Range format "XX-YY°F" or "XX-YY°C" or "between XX-YY°F"
        range_pattern = r'(?:between\s+)?(\d+)\s*-\s*(\d+)\s*°([FC])'
        match = re.search(range_pattern, question)
        if match:
            min_temp = float(match.group(1))
            max_temp = float(match.group(2))
            unit = match.group(3)
            return {"min": min_temp, "max": max_temp, "unit": unit}
        
        # Pattern 2: "XX°F or higher" / "XX°C or above"
        higher_pattern = r'(\d+)\s*°([FC])\s*(?:or\s+)?(?:higher|above)'
        match = re.search(higher_pattern, question, re.IGNORECASE)
        if match:
            temp = float(match.group(1))
            unit = match.group(2)
            return {"min": temp, "max": temp + 100, "unit": unit}
        
        # Pattern 3: "XX°F or below" / "XX°C or lower"  
        lower_pattern = r'(\d+)\s*°([FC])\s*(?:or\s+)?(?:below|lower|under)'
        match = re.search(lower_pattern, question, re.IGNORECASE)
        if match:
            temp = float(match.group(1))
            unit = match.group(2)
            return {"min": -100, "max": temp, "unit": unit}
        
        # Pattern 4: "below XX°F" / "under XX°C"
        below_pattern = r'(?:below|under)\s+(\d+)\s*°([FC])'
        match = re.search(below_pattern, question, re.IGNORECASE)
        if match:
            temp = float(match.group(1))
            unit = match.group(2)
            return {"min": -100, "max": temp, "unit": unit}
        
        # Pattern 5: Exact temp "33°F" (single degree)
        exact_pattern = r'(?:be\s+)?(\d+)\s*°([FC])\s+on'
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
