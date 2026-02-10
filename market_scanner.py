"""
Polymarket Market Scanner

Scans for temperature markets settling within 48 hours
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
    
    async def fetch_weather_events_via_search(self) -> List[Dict]:
        """Use search API to find temperature events"""
        url = f"{self.GAMMA_BASE}/search"
        
        # Search for temperature-related terms
        search_terms = ["highest temperature", "temperature in", "weather"]
        all_events = []
        
        for term in search_terms:
            params = {"_q": term}
            
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.get(url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        events = data.get("events", [])
                        logger.info(f"Search '{term}': found {len(events)} events")
                        all_events.extend(events)
                    else:
                        logger.warning(f"Search API error for '{term}': {response.status_code}")
                        
            except Exception as e:
                logger.error(f"Error searching for '{term}': {e}")
                continue
        
        # Deduplicate by event ID
        seen = set()
        unique_events = []
        for event in all_events:
            event_id = event.get("id")
            if event_id and event_id not in seen:
                seen.add(event_id)
                unique_events.append(event)
        
        logger.info(f"Total unique events from search: {len(unique_events)}")
        return unique_events
    
    async def fetch_active_events(self, limit: int = 500, offset: int = 0) -> List[Dict]:
        """Fetch active EVENTS from Gamma API"""
        url = f"{self.GAMMA_BASE}/events"
        params = {
            "limit": str(limit),
            "offset": str(offset),
            "active": "true",
            "closed": "false",
        }
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    events = response.json()
                    logger.info(f"Fetched {len(events)} active events from Gamma")
                    return events
                else:
                    logger.error(f"Gamma API error: {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []
    
    async def get_weather_markets(self, max_hours_until_settlement: int = 48) -> List[Dict]:
        """
        Get all temperature markets settling within specified hours.
        
        Uses search API to find temperature events, then extracts all markets.
        
        Returns:
            List of market dicts with parsed metadata
        """
        # Use search API to find temperature events
        all_events = await self.fetch_weather_events_via_search()
        
        weather_markets = []
        now = datetime.now()
        cutoff = now + timedelta(hours=max_hours_until_settlement)
        
        logger.info(f"Scanning {len(all_events)} events for temperature markets...")
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"Cutoff time: {cutoff.strftime('%Y-%m-%d %H:%M')}")
        
        temp_event_count = 0
        
        for event in all_events:
            try:
                event_title = event.get("title", "")
                
                # Debug: Log if no title
                if not event_title:
                    logger.warning(f"Event has no title! Keys: {list(event.keys())[:10]}")
                    continue
                
                # Filter for temperature events
                if not self._is_temperature_event(event_title):
                    continue
                
                temp_event_count += 1
                logger.info(f"Found temp event #{temp_event_count}: {event_title}")
                
                # Check settlement time
                end_date_str = event.get("endDate") or event.get("end_date_iso")
                if not end_date_str:
                    logger.warning(f"  No end date")
                    continue
                
                try:
                    end_date = dateutil.parser.parse(end_date_str)
                except Exception as e:
                    logger.warning(f"  Could not parse date: {e}")
                    continue
                
                # Skip if settling too far in future
                if end_date > cutoff:
                    logger.info(f"  Settles too far: {end_date.strftime('%Y-%m-%d %H:%M')}")
                    continue
                
                # Skip if already settled
                if end_date < now:
                    logger.info(f"  Already settled")
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
                        
                        # Build enriched market object
                        enriched = {
                            "event_title": event_title,
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
