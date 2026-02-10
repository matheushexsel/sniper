# city_coords.py
"""
NWS Grid Point Coordinates for Major Cities

NWS API requires grid coordinates instead of lat/lon.
These are pre-calculated for the cities we're trading.
"""

# Format: city_name: (office, gridX, gridY, timezone, lat, lon)
CITY_COORDS = {
    "NYC": ("OKX", 33, 37, "America/New_York", 40.7128, -74.0060),
    "Chicago": ("LOT", 76, 73, "America/Chicago", 41.8781, -87.6298),
    "Seattle": ("SEW", 124, 67, "America/Los_Angeles", 47.6062, -122.3321),
    "Dallas": ("FWD", 78, 107, "America/Chicago", 32.7767, -96.7970),
    "Miami": ("MFL", 110, 50, "America/New_York", 25.7617, -80.1918),
    "Los Angeles": ("LOX", 154, 45, "America/Los_Angeles", 34.0522, -118.2437),
    "Atlanta": ("FFC", 52, 87, "America/New_York", 33.7490, -84.3880),
    "Boston": ("BOX", 71, 90, "America/New_York", 42.3601, -71.0589),
    "Denver": ("BOU", 62, 61, "America/Denver", 39.7392, -104.9903),
    "Phoenix": ("PSR", 158, 58, "America/Phoenix", 33.4484, -112.0740),
}

# International cities (use alternative weather API)
INTL_CITIES = {
    "London": ("London", 51.5074, -0.1278, "Europe/London"),
    "Seoul": ("Seoul", 37.5665, 126.9780, "Asia/Seoul"),
    "Toronto": ("Toronto", 43.6532, -79.3832, "America/Toronto"),
    "Buenos Aires": ("Buenos Aires", -34.6037, -58.3816, "America/Argentina/Buenos_Aires"),
    "Ankara": ("Ankara", 39.9334, 32.8597, "Europe/Istanbul"),
    "Wellington": ("Wellington", -41.2865, 174.7762, "Pacific/Auckland"),
}


def get_city_info(city_name: str):
    """Get NWS grid coordinates for a US city or coords for international city"""
    # Try US cities first
    if city_name in CITY_COORDS:
        office, grid_x, grid_y, tz, lat, lon = CITY_COORDS[city_name]
        return {
            "type": "nws",
            "office": office,
            "grid_x": grid_x,
            "grid_y": grid_y,
            "timezone": tz,
            "lat": lat,
            "lon": lon,
        }
    
    # Try international cities
    if city_name in INTL_CITIES:
        name, lat, lon, tz = INTL_CITIES[city_name]
        return {
            "type": "international",
            "name": name,
            "lat": lat,
            "lon": lon,
            "timezone": tz,
        }
    
    return None


def extract_city_from_question(question: str) -> str:
    """Extract city name from market question"""
    question_lower = question.lower()
    
    # Check US cities
    for city in CITY_COORDS.keys():
        if city.lower() in question_lower:
            return city
    
    # Check international cities
    for city in INTL_CITIES.keys():
        if city.lower() in question_lower:
            return city
    
    return None
