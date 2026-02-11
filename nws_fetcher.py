# nws_fetcher.py
"""
Weather Data Fetcher with Smart Probability Modeling

Key improvements over v1:
1. Time-aware σ: uncertainty shrinks as the day progresses in local time
2. Open-Meteo ensemble API: 50 model runs → real probability distribution
3. Observed high floor: if it's already 65°F at 2pm, high ≥ 65°F
4. Per-city-date forecast caching: 1 HTTP call per city-date, not per market
"""

import httpx
import logging
import math
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pytz
from dateutil.parser import isoparse

logger = logging.getLogger(__name__)

# ─── Math helpers ───────────────────────────────────────────────────────────

def _normal_cdf(x: float) -> float:
    """Standard normal CDF using the error function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _prob_in_range(forecast_high: float, temp_min: float, temp_max: float, 
                   std_dev: float, observed_high: Optional[float] = None) -> float:
    """
    Probability that the actual daily high falls within [temp_min, temp_max].
    
    If observed_high is provided (i.e., we know the high so far today),
    we model the REMAINING uncertainty: the final high is at least observed_high,
    so we use a truncated normal distribution.
    """
    if std_dev <= 0.01:
        # Essentially no uncertainty — point estimate
        actual = observed_high if observed_high else forecast_high
        return 1.0 if temp_min <= actual < temp_max else 0.0
    
    # If we have an observed high that exceeds the forecast, shift our mean up
    effective_mean = forecast_high
    if observed_high and observed_high > forecast_high:
        # The actual high will be at least observed_high
        # Shift mean to observed_high since our forecast was too low
        effective_mean = observed_high
    
    if observed_high and observed_high > temp_max:
        # Already exceeded this range — probability is 0
        return 0.0
    
    if observed_high and temp_max <= observed_high:
        # Range is entirely below what's already observed — probability is 0
        return 0.0
    
    # Standard normal probability for the range
    z_low = (temp_min - effective_mean) / std_dev
    z_high = (temp_max - effective_mean) / std_dev
    prob = _normal_cdf(z_high) - _normal_cdf(z_low)
    
    if observed_high and observed_high > temp_min:
        # The actual high is at least observed_high, so we're in a truncated distribution
        # P(high in [temp_min, temp_max] | high >= observed_high)
        # = P(high in [observed_high, temp_max]) / P(high >= observed_high)
        z_obs = (observed_high - effective_mean) / std_dev
        prob_above_obs = 1.0 - _normal_cdf(z_obs)
        
        z_high_trunc = (temp_max - effective_mean) / std_dev
        prob_in_range_above_obs = _normal_cdf(z_high_trunc) - _normal_cdf(z_obs)
        
        if prob_above_obs > 0.001:
            prob = prob_in_range_above_obs / prob_above_obs
        else:
            prob = 1.0 if observed_high >= temp_min and observed_high < temp_max else 0.0
    
    return max(0.0, min(1.0, prob))


def _get_dynamic_sigma(hours_until_settlement: float, local_hour: float, 
                       is_same_day: bool, base_sigma: float = 3.5) -> float:
    """
    Calculate dynamic σ based on how much of the day has elapsed.
    
    The daily high typically occurs between 2-5 PM local time.
    As we approach and pass that window, uncertainty drops dramatically.
    
    Args:
        hours_until_settlement: Hours until market settles
        local_hour: Current local hour (0-23) in the city's timezone
        is_same_day: Whether we're forecasting for today vs tomorrow
        base_sigma: Starting uncertainty (°F)
    
    Returns:
        Adjusted σ in °F
    """
    if not is_same_day:
        # Tomorrow's forecast — use time-based decay from base
        if hours_until_settlement > 36:
            return base_sigma * 1.2  # Far out, more uncertainty
        elif hours_until_settlement > 24:
            return base_sigma * 1.0
        else:
            return base_sigma * 0.9
    
    # Same-day forecast — σ depends on local time
    # Daily high usually happens between 14:00-17:00 local
    if local_hour < 6:
        # Pre-dawn: still significant uncertainty
        return base_sigma * 0.8
    elif local_hour < 10:
        # Morning: temperatures rising, some info gained
        return base_sigma * 0.6
    elif local_hour < 13:
        # Late morning: getting closer
        return base_sigma * 0.4
    elif local_hour < 16:
        # Early-mid afternoon: high is being set NOW
        return base_sigma * 0.25
    elif local_hour < 18:
        # Late afternoon: high is almost certainly recorded
        return base_sigma * 0.1
    else:
        # Evening/night: high IS recorded, nearly zero uncertainty
        return base_sigma * 0.05  # ~0.15°F for NWS


# ─── Forecast Cache ─────────────────────────────────────────────────────────

class ForecastCache:
    """
    Cache forecasts per city-date to avoid redundant API calls.
    One city-date pair = one HTTP call, shared across all 7 temperature buckets.
    """
    def __init__(self):
        self._cache: Dict[str, Dict] = {}
    
    def _key(self, source: str, location_id: str, date_str: str) -> str:
        return f"{source}:{location_id}:{date_str}"
    
    def get(self, source: str, location_id: str, date_str: str) -> Optional[Dict]:
        key = self._key(source, location_id, date_str)
        return self._cache.get(key)
    
    def set(self, source: str, location_id: str, date_str: str, data: Dict):
        key = self._key(source, location_id, date_str)
        self._cache[key] = data
    
    def clear(self):
        self._cache.clear()


# Global cache instance — reset each scan cycle
_forecast_cache = ForecastCache()

def reset_forecast_cache():
    """Call at the start of each scan cycle."""
    _forecast_cache.clear()


# ─── NWS Fetcher (US cities) ───────────────────────────────────────────────

class NWSFetcher:
    """Fetch temperature forecasts from National Weather Service"""
    
    BASE_URL = "https://api.weather.gov"
    
    def __init__(self):
        self.headers = {
            "User-Agent": "(Weather Arbitrage Bot, contact@example.com)",
            "Accept": "application/geo+json"
        }
    
    async def get_hourly_forecast(self, office: str, grid_x: int, grid_y: int) -> Optional[Dict]:
        """Get hourly temperature forecast for a location."""
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
        
        for period in periods[:48]:
            try:
                time_str = period.get("startTime")
                temp_f = period.get("temperature")
                
                if time_str and temp_f is not None:
                    dt = isoparse(time_str)
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
        timezone_str: str = "America/New_York",
        hours_until_settlement: float = 24.0,
    ) -> Optional[float]:
        """
        Calculate probability with time-aware uncertainty.
        """
        cache_key = f"{office}/{grid_x},{grid_y}"
        date_str = target_date.date().isoformat()
        
        # Check cache
        cached = _forecast_cache.get("nws", cache_key, date_str)
        if cached:
            forecast = cached
        else:
            forecast = await self.get_hourly_forecast(office, grid_x, grid_y)
            if not forecast:
                return None
            _forecast_cache.set("nws", cache_key, date_str, forecast)
        
        # Get temps for target date
        target_temps = []
        for hour in forecast["hourly"]:
            hour_time = hour["time"]
            if hour_time.date() == target_date.date():
                target_temps.append(hour["temp_f"])
        
        if not target_temps:
            logger.warning(f"No forecast data for target date {target_date.date()}")
            return None
        
        forecast_high = max(target_temps)
        
        # Get current local time for the city
        tz = pytz.timezone(timezone_str)
        now_local = datetime.now(tz)
        local_hour = now_local.hour + now_local.minute / 60.0
        is_same_day = (now_local.date() == target_date.date())
        
        # Get observed high so far (hours that have already passed)
        observed_high = None
        if is_same_day:
            now_utc = datetime.now(pytz.UTC)
            past_temps = [
                h["temp_f"] for h in forecast["hourly"]
                if h["time"].date() == target_date.date() 
                and h["time"] <= now_utc
            ]
            if past_temps:
                observed_high = max(past_temps)
        
        # Dynamic σ based on time of day
        std_dev = _get_dynamic_sigma(
            hours_until_settlement=hours_until_settlement,
            local_hour=local_hour,
            is_same_day=is_same_day,
            base_sigma=3.5,  # NWS base accuracy
        )
        
        probability = _prob_in_range(
            forecast_high, temp_min_f, temp_max_f, 
            std_dev=std_dev, observed_high=observed_high
        )
        
        obs_str = f" obs_high={observed_high:.0f}°F" if observed_high else ""
        logger.info(
            f"Forecast high: {forecast_high:.0f}°F | Range: {temp_min_f:.0f}-{temp_max_f:.0f}°F | "
            f"σ={std_dev:.2f} | local={local_hour:.1f}h | P={probability:.1%}{obs_str}"
        )
        
        return probability


# ─── Open-Meteo Fetcher (International) ────────────────────────────────────

class OpenMeteoFetcher:
    """Fetch international weather data from Open-Meteo with ensemble support"""
    
    ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
    POINT_URL = "https://api.open-meteo.com/v1/forecast"
    
    async def get_ensemble_forecast(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Get ensemble forecast (50 model runs) for real probability distribution.
        This is the gold standard — gives us actual spread of outcomes.
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "forecast_days": 3,
            "timezone": "UTC",
        }
        
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(self.ENSEMBLE_URL, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_ensemble(data)
                else:
                    logger.debug(f"Ensemble API unavailable ({response.status_code}), falling back to point forecast")
                    return None
                    
        except Exception as e:
            logger.debug(f"Ensemble API error: {e}")
            return None
    
    def _parse_ensemble(self, data: Dict) -> Optional[Dict]:
        """
        Parse ensemble response. Returns hourly data with all 50 member values.
        
        The response has hourly.temperature_2m_member0 through member49.
        """
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        
        if not times:
            return None
        
        # Collect all ensemble members
        members = []
        for i in range(51):  # member0 through member50
            key = f"temperature_2m_member{i}"
            if key in hourly:
                members.append(hourly[key])
        
        if not members:
            return None
        
        n_members = len(members)
        
        # Build hourly records with all member values
        hourly_data = []
        for t_idx, time_str in enumerate(times):
            try:
                dt = datetime.fromisoformat(time_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.UTC)
                
                member_temps = [members[m][t_idx] for m in range(n_members) 
                               if t_idx < len(members[m]) and members[m][t_idx] is not None]
                
                if member_temps:
                    hourly_data.append({
                        "time": dt,
                        "temp_f": sum(member_temps) / len(member_temps),  # mean
                        "member_temps": member_temps,
                    })
            except Exception:
                continue
        
        if not hourly_data:
            return None
        
        return {
            "hourly": hourly_data,
            "has_ensemble": True,
            "n_members": n_members,
            "updated": datetime.now(pytz.UTC),
        }
    
    async def get_hourly_forecast(self, lat: float, lon: float) -> Optional[Dict]:
        """Get standard point forecast as fallback."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "forecast_days": 3,
            "timezone": "UTC",
        }
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(self.POINT_URL, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_point_forecast(data)
                else:
                    logger.warning(f"Open-Meteo API error {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching Open-Meteo data: {e}")
            return None
    
    def _parse_point_forecast(self, data: Dict) -> Optional[Dict]:
        """Parse standard Open-Meteo point forecast."""
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        
        if not times or not temps:
            return None
        
        hourly_temps = []
        for time_str, temp_f in zip(times, temps):
            try:
                if 'T' in time_str:
                    dt = datetime.fromisoformat(time_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=pytz.UTC)
                    hourly_temps.append({
                        "time": dt,
                        "temp_f": temp_f,
                    })
            except Exception:
                continue
        
        if not hourly_temps:
            return None
        
        return {
            "hourly": hourly_temps,
            "has_ensemble": False,
            "updated": datetime.now(pytz.UTC),
        }
    
    async def get_temperature_probability(
        self,
        lat: float,
        lon: float,
        target_date: datetime,
        temp_min_f: float,
        temp_max_f: float,
        timezone_str: str = "UTC",
        hours_until_settlement: float = 24.0,
    ) -> Optional[float]:
        """
        Calculate temperature probability using ensemble data if available.
        
        With ensemble: count how many of 50 model runs produce a daily high 
        in the target range → direct empirical probability.
        
        Without ensemble: fall back to normal distribution with dynamic σ.
        """
        cache_key = f"{lat},{lon}"
        date_str = target_date.date().isoformat()
        
        # Check cache
        cached = _forecast_cache.get("openmeteo", cache_key, date_str)
        if cached:
            forecast = cached
        else:
            # Try ensemble first, fall back to point forecast
            forecast = await self.get_ensemble_forecast(lat, lon)
            if not forecast:
                forecast = await self.get_hourly_forecast(lat, lon)
            if not forecast:
                return None
            _forecast_cache.set("openmeteo", cache_key, date_str, forecast)
        
        # Get local time info
        tz = pytz.timezone(timezone_str)
        now_local = datetime.now(tz)
        local_hour = now_local.hour + now_local.minute / 60.0
        is_same_day = (now_local.date() == target_date.date())
        
        # Filter to target date
        target_hours = [h for h in forecast["hourly"] if h["time"].date() == target_date.date()]
        
        if not target_hours:
            return None
        
        # ── Ensemble path: empirical probability from 50 model runs ──
        if forecast.get("has_ensemble") and target_hours[0].get("member_temps"):
            return self._ensemble_probability(
                target_hours, temp_min_f, temp_max_f,
                local_hour, is_same_day, hours_until_settlement, timezone_str
            )
        
        # ── Point forecast path: normal distribution with dynamic σ ──
        forecast_high = max(h["temp_f"] for h in target_hours)
        
        # Observed high so far
        observed_high = None
        if is_same_day:
            now_utc = datetime.now(pytz.UTC)
            past_temps = [h["temp_f"] for h in target_hours if h["time"] <= now_utc]
            if past_temps:
                observed_high = max(past_temps)
        
        std_dev = _get_dynamic_sigma(
            hours_until_settlement=hours_until_settlement,
            local_hour=local_hour,
            is_same_day=is_same_day,
            base_sigma=4.0,  # Open-Meteo slightly less accurate than NWS
        )
        
        probability = _prob_in_range(
            forecast_high, temp_min_f, temp_max_f,
            std_dev=std_dev, observed_high=observed_high
        )
        
        obs_str = f" obs_high={observed_high:.0f}°F" if observed_high else ""
        logger.info(
            f"Forecast high: {forecast_high:.0f}°F | Range: {temp_min_f:.0f}-{temp_max_f:.0f}°F | "
            f"σ={std_dev:.2f} | local={local_hour:.1f}h | P={probability:.1%}{obs_str}"
        )
        
        return probability
    
    def _ensemble_probability(
        self, target_hours: List[Dict], temp_min_f: float, temp_max_f: float,
        local_hour: float, is_same_day: bool, hours_until_settlement: float,
        timezone_str: str,
    ) -> float:
        """
        Calculate probability from ensemble members.
        
        For each of the 50 ensemble members, compute that member's daily high,
        then count what fraction fall in [temp_min, temp_max].
        """
        # Collect daily highs per ensemble member
        # Each hour has member_temps[0..49], we need max across hours per member
        n_members = len(target_hours[0]["member_temps"])
        
        # If same day and afternoon, only use hours from now onward + observed max
        now_utc = datetime.now(pytz.UTC)
        
        member_highs = []
        for member_idx in range(n_members):
            member_temps_for_day = []
            for hour_data in target_hours:
                if member_idx < len(hour_data.get("member_temps", [])):
                    temp = hour_data["member_temps"][member_idx]
                    if temp is not None:
                        member_temps_for_day.append(temp)
            
            if member_temps_for_day:
                member_highs.append(max(member_temps_for_day))
        
        if not member_highs:
            return 0.0
        
        # If same-day, incorporate observed floor
        if is_same_day:
            past_mean_temps = [
                h["temp_f"] for h in target_hours if h["time"] <= now_utc
            ]
            if past_mean_temps:
                observed_high = max(past_mean_temps)
                # Each member's high must be at least the observed high
                member_highs = [max(mh, observed_high) for mh in member_highs]
        
        # Count members in range
        in_range = sum(1 for mh in member_highs if temp_min_f <= mh < temp_max_f)
        probability = in_range / len(member_highs)
        
        # For the "or higher" / "or below" open-ended ranges, adjust comparison
        # temp_max is set to temp_min + 100 for open ranges, so < check handles it
        
        mean_high = sum(member_highs) / len(member_highs)
        spread = max(member_highs) - min(member_highs)
        
        logger.info(
            f"ENSEMBLE high: {mean_high:.0f}°F (spread={spread:.1f}) | "
            f"Range: {temp_min_f:.0f}-{temp_max_f:.0f}°F | "
            f"{in_range}/{len(member_highs)} members | P={probability:.1%} | "
            f"local={local_hour:.1f}h"
        )
        
        return probability
