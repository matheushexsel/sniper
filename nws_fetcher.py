"""
National Weather Service Data Fetcher

Fetches hourly temperature forecasts from NWS API (FREE, no key needed)
"""

import httpx
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)


class NWSFetcher:
    """Fetch temperature forecasts from National Weather Service"""
    
    BASE_URL = "https://api.weather.gov"
    
    def __init__(self):
        self.headers = {
            "User-Agent": "(Weather Arbitrage Bot, contact@example.com)",
            "Accept": "application/geo+json"
        }
    
    async def get_hourly_forecast(self, office: str, grid_x: int, grid_y: int) -> Optional[Dict]:
        """
        Get hourly temperature forecast for a location.
        
        Args:
            office: NWS office code (e.g., "OKX" for NYC)
            grid_x: Grid X coordinate
            grid_y: Grid Y coordinate
            
        Returns:
            Dict with hourly forecasts or None if error
        """
        url = f"{self.BASE_URL}/gridpoints/{office}/{grid_x},{grid_y}/forecast/hourly"
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_forecast(data)
                else:
                    logger.warning(f"NWS API error {response.status_code} for {office}/{grid_x},{grid_y}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching NWS data: {e}")
            return None
    
    def _parse_forecast(self, data: Dict) -> Dict:
        """Parse NWS forecast response into usable format"""
        properties = data.get("properties", {})
        periods = properties.get("periods", [])
        
        hourly_temps = []
        
        for period in periods[:48]:  # Next 48 hours
            try:
                time_str = period.get("startTime")
                temp_f = period.get("temperature")
                
                if time_str and temp_f:
                    dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    hourly_temps.append({
                        "time": dt,
                        "temp_f": temp_f,
                    })
            except Exception:
                continue
        
        return {
            "hourly": hourly_temps,
            "updated": datetime.now(pytz.UTC),
        }
    
    async def get_temperature_probability(
        self, 
        office: str, 
        grid_x: int, 
        grid_y: int,
        target_date: datetime,
        temp_min_f: float,
        temp_max_f: float,
    ) -> Optional[float]:
        """
        Calculate probability of temperature being in range on target date.
        
        Args:
            office, grid_x, grid_y: NWS location
            target_date: Date to check (should be within next 48 hours)
            temp_min_f: Minimum temperature in Fahrenheit
            temp_max_f: Maximum temperature in Fahrenheit
            
        Returns:
            Probability (0.0 to 1.0) or None if error
        """
        forecast = await self.get_hourly_forecast(office, grid_x, grid_y)
        
        if not forecast:
            return None
        
        # Get forecasts for the target date
        target_temps = []
        
        for hour in forecast["hourly"]:
            hour_time = hour["time"]
            # Check if this hour is on the target date
            if hour_time.date() == target_date.date():
                target_temps.append(hour["temp_f"])
        
        if not target_temps:
            logger.warning(f"No forecast data for target date {target_date.date()}")
            return None
        
        # Count how many hours fall in the target range
        in_range = sum(1 for temp in target_temps if temp_min_f <= temp <= temp_max_f)
        total = len(target_temps)
        
        # Simple probability: fraction of hours in range
        probability = in_range / total if total > 0 else 0.0
        
        logger.info(f"Temperature {temp_min_f}-{temp_max_f}°F: {in_range}/{total} hours in range = {probability:.1%}")
        
        return probability


class OpenMeteoFetcher:
    """Fetch international weather data from Open-Meteo (FREE alternative to NWS)"""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    
    async def get_hourly_forecast(self, lat: float, lon: float) -> Optional[Dict]:
        """Get hourly forecast for international locations"""
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "forecast_days": 2,
        }
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(self.BASE_URL, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_forecast(data)
                else:
                    logger.warning(f"Open-Meteo API error {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching Open-Meteo data: {e}")
            return None
    
    def _parse_forecast(self, data: Dict) -> Optional[Dict]:
        """Parse Open-Meteo response"""
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        
        if not times or not temps:
            logger.warning(f"Open-Meteo returned no data")
            return None
        
        hourly_temps = []
        
        for time_str, temp_f in zip(times, temps):
            try:
                # Handle timezone-aware or naive datetimes
                if 'T' in time_str:
                    dt = datetime.fromisoformat(time_str)
                    # Make timezone-aware if it isn't already
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=pytz.UTC)
                else:
                    continue
                    
                hourly_temps.append({
                    "time": dt,
                    "temp_f": temp_f,
                })
            except Exception as e:
                logger.debug(f"Error parsing time {time_str}: {e}")
                continue
        
        if not hourly_temps:
            logger.warning(f"Failed to parse any forecast data from Open-Meteo")
            return None
        
        return {
            "hourly": hourly_temps,
            "updated": datetime.now(pytz.UTC),
        }
    
    async def get_temperature_probability(
        self,
        lat: float,
        lon: float,
        target_date: datetime,
        temp_min_f: float,
        temp_max_f: float,
    ) -> Optional[float]:
        """Calculate temperature probability for international cities"""
        forecast = await self.get_hourly_forecast(lat, lon)
        
        if not forecast:
            return None
        
        target_temps = []
        
        for hour in forecast["hourly"]:
            if hour["time"].date() == target_date.date():
                target_temps.append(hour["temp_f"])
        
        if not target_temps:
            return None
        
        in_range = sum(1 for temp in target_temps if temp_min_f <= temp <= temp_max_f)
        total = len(target_temps)
        
        probability = in_range / total if total > 0 else 0.0
        
        logger.info(f"Temperature {temp_min_f}-{temp_max_f}°F: {in_range}/{total} hours = {probability:.1%}")
        
        return probability
