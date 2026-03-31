"""
Domain dictionary for avalanche prediction NL2SQL system.
Maps abbreviations, scientific terms, and vague user language to precise column/SQL context.

Covers: Military abbreviations + Satellite + Seismic + Meteorological + Snow science + Risk terms
"""

# Scientific & Military abbreviations → full forms
ABBREVIATIONS = {
    # Satellite / Remote Sensing
    "ndsi": "normalized difference snow index",
    "lst": "land surface temperature",
    "smp": "soil moisture profile",
    "ssm": "surface soil moisture",
    "modis": "moderate resolution imaging spectroradiometer",
    "era5": "ECMWF reanalysis 5th generation",
    "swe": "snow water equivalent",
    # Seismic
    "pgv": "peak ground velocity",
    "pga": "peak ground acceleration",
    "pgd": "peak ground displacement",
    "cav": "cumulative absolute velocity",
    "rms": "root mean square ground motion",
    # Terrain
    "tri": "terrain ruggedness index",
    "dem": "digital elevation model",
    "asl": "above sea level",
    # Weather / Snow
    "ros": "rain on snow",
    "pdd": "positive degree days",
    "ft": "freeze thaw",
    # Risk
    "csw": "critical slope wind",
    # Military (retained from original for backward compatibility)
    "col": "colonel",
    "maj": "major",
    "capt": "captain",
    "lt": "lieutenant",
    "gen": "general",
    "brig": "brigadier",
    "hq": "headquarters",
    "regt": "regiment",
    "bn": "battalion",
    "pno": "personal number",
    "dob": "date of birth",
}

# Business/domain terms → SQL context hints for the LLM
BUSINESS_TERM_HINTS = {
    # Avalanche prediction core
    "dangerous": "Filter by high avalanche_probability (>0.6) or risk_scale >= 4 or compound_risk_score > 60",
    "risky": "Filter by risk_scale >= 3 or compound_risk_score > 40",
    "safe": "Filter by prediction = 0 or safe_probability > 0.7 or risk_scale <= 2",
    "high risk": "Filter by risk_scale >= 4 or compound_risk_score > 60",
    "low risk": "Filter by risk_scale <= 2 or compound_risk_score < 20",
    "extreme": "Filter by risk_scale = 5 or compound_risk_score > 80",
    "avalanche": "Look at prediction, avalanche_probability, risk_scale, compound_risk_score columns",
    # Weather
    "cold": "Filter by temp_2m_celsius < -15 or temp_2m < 258",
    "warm": "Filter by temp_2m_celsius > -5 or temp_positive = 1",
    "freezing": "Filter by temp_2m_celsius < 0 or temp_positive = 0",
    "windy": "Filter by wind_speed > 15 or wind_speed_max_6h > 20",
    "storm": "Filter by wind_speed > 20 AND total_precipitation > 5",
    "calm": "Filter by wind_speed < 5",
    "heavy snowfall": "Filter by snowfall_24h > 30 or snowfall > 5",
    "light snow": "Filter by snowfall_24h BETWEEN 1 AND 10",
    "rain on snow": "Filter by rain_on_snow_ratio > 0.3 or warm_precip_flag = 1",
    "blizzard": "Filter by wind_speed > 15 AND snowfall > 3",
    # Snow conditions
    "deep snow": "Filter by snowdepth > 100 or snow_depth > 1.0",
    "fresh snow": "Filter by days_since_snowfall <= 2 or snowfall_24h > 5",
    "old snow": "Filter by days_since_snowfall > 7 or snow_albedo < 0.5",
    "wet snow": "Filter by temp_positive = 1 AND snowdepth > 20",
    "compacted": "Filter by snow_density > 300",
    "powder": "Filter by snow_density < 100 AND days_since_snowfall <= 1",
    "melting": "Filter by snowmelt > 0 or temp_positive = 1",
    # Terrain
    "steep": "Filter by slope > 35",
    "gentle": "Filter by slope < 20",
    "high altitude": "Filter by elevation > 4500 or elevation_zone = 'Nival'",
    "low altitude": "Filter by elevation < 3500 or elevation_zone = 'Sub-Alpine'",
    "south facing": "Filter by south_facing = 1 or (aspect BETWEEN 135 AND 225)",
    "north facing": "Filter by aspect < 45 OR aspect > 315",
    "ridge": "Filter by distance_to_ridge < 200",
    # Seismic
    "earthquake": "Look at pga, pgv, seismic_energy, seismic_risk_index columns",
    "seismic": "Look at pga, pgv, cav, arias_intensity, seismic_risk_index columns",
    "ground shaking": "Filter by pga > 0.05 or seismic_risk_index > 3",
    "tremor": "Filter by pga > 0.01 AND pga < 0.1",
    # Temperature trends
    "warming": "Filter by temp_change_24h > 3 or temp_trend_3d > 5",
    "cooling": "Filter by temp_change_24h < -3 or temp_trend_3d < -5",
    "rapid warming": "Filter by temp_change_rate_6h > 1",
    "freeze thaw": "Filter by freeze_thaw_flag = 1 or freeze_thaw_cycles_7d > 3",
    # Time
    "today": "Filter by prediction_date = current date",
    "this week": "Filter by prediction_date within last 7 days",
    "last week": "Filter by prediction_date within previous 7 days",
    "recently": "ORDER BY prediction_date DESC LIMIT 20",
    "latest": "ORDER BY prediction_date DESC LIMIT 10",
    # Location
    "station": "Group by or filter by encrypted_lat, encrypted_lon",
    "location": "Group by encrypted_lat, encrypted_lon",
    "all stations": "Include all distinct encrypted_lat values",
    # Aggregation patterns
    "average": "Use AVG() function",
    "total": "Use SUM() function",
    "count": "Use COUNT() function",
    "maximum": "Use MAX() function",
    "minimum": "Use MIN() function",
    "trend": "ORDER BY prediction_date and look at temporal columns",
}
