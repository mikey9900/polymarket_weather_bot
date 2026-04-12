# =============================================================
# stations/station_resolver.py
#
# PURPOSE:
#   Maps a parsed market location (e.g. "New York City" or "JFK")
#   to the specific NOAA weather station that Polymarket uses
#   as its official data source for that location.
#
# WHY THIS MATTERS:
#   Polymarket specifies in each market which exact weather station
#   they use to resolve the bet. If you use a different station,
#   your forecast might be off. This file maps city names and
#   airport codes to their correct NOAA station IDs.
#
# HOW TO FIND STATION IDs:
#   Go to: https://www.ncdc.noaa.gov/cdo-web/datatools/findstation
#   Or look at the "resolution source" in a Polymarket market's
#   description/rules — they usually name the exact station.
#
# HOW TO ADD NEW CITIES:
#   Add an entry to CITY_TO_STATION following the same format.
#   The key should be lowercase (matching is case-insensitive).
# =============================================================

from typing import Optional, Dict


# -------------------------------------------------------------
# CITY → NOAA STATION mapping
#
# Keys are lowercase city names. Values are station dicts with:
#   station_id  – NOAA's unique station identifier
#   name        – human-readable station name (for logging)
#   provider    – always "NOAA" for now (future-proofing)
# -------------------------------------------------------------
CITY_TO_STATION = {
    "new york city": {
        "station_id": "USW00094728",
        "name": "Central Park Weather Station",
        "provider": "NOAA",
    },
    "los angeles": {
        "station_id": "USW00023174",
        "name": "Los Angeles Intl Airport",
        "provider": "NOAA",
    },
    "mexico city": {
        "station_id": "MXN00002090",
        "name": "Mexico City Weather Station",
        "provider": "NOAA",
    },
    # ✅ Add more cities here as you expand coverage
    # "chicago": {
    #     "station_id": "USW00094846",
    #     "name": "Chicago O'Hare Intl Airport",
    #     "provider": "NOAA",
    # },
    # "phoenix": {
    #     "station_id": "USW00023183",
    #     "name": "Phoenix Sky Harbor Intl Airport",
    #     "provider": "NOAA",
    # },
}


# -------------------------------------------------------------
# AIRPORT CODE → NOAA STATION mapping
#
# Airport codes take priority over city names in resolve_station()
# because they're more specific (NYC has multiple stations;
# JFK is unambiguous).
# -------------------------------------------------------------
AIRPORT_CODE_TO_STATION = {
    "jfk": {
        "station_id": "USW00094789",
        "name": "John F. Kennedy Intl Airport",
        "provider": "NOAA",
    },
    "lax": {
        "station_id": "USW00023174",
        "name": "Los Angeles Intl Airport",
        "provider": "NOAA",
    },
    # ✅ Add more airports here
    # "ord": {
    #     "station_id": "USW00094846",
    #     "name": "Chicago O'Hare Intl Airport",
    #     "provider": "NOAA",
    # },
    # "phx": {
    #     "station_id": "USW00023183",
    #     "name": "Phoenix Sky Harbor Intl Airport",
    #     "provider": "NOAA",
    # },
}


def resolve_station(parsed_market: Dict) -> Optional[Dict]:
    """
    Takes a parsed market dict (output of weather_parser.py) and
    returns the matching NOAA station dict, or None if unknown.

    This is intentionally conservative — it returns None if we're
    not confident about the station match, rather than guessing
    and giving you bad forecast data.

    Args:
        parsed_market (dict): Must contain a "location" key
                              (e.g. "New York City" or "JFK Airport")

    Returns:
        dict with station_id, name, provider
        OR None if no match found
    """

    # Get the location string from the parsed market
    location = parsed_market.get("location")
    if not location:
        # No location info — can't resolve a station
        return None

    # Lowercase for case-insensitive matching
    location_lower = location.lower()

    # --- Check airport codes first (more specific) ---
    # e.g. if location contains "jfk", use the JFK station
    for code, station in AIRPORT_CODE_TO_STATION.items():
        if code in location_lower:
            print(f"  📍 Resolved '{location}' → {station['name']} (airport code match)")
            return station

    # --- Fall back to city name substring match ---
    # e.g. if location contains "new york city", use Central Park station
    for city, station in CITY_TO_STATION.items():
        if city in location_lower:
            print(f"  📍 Resolved '{location}' → {station['name']} (city match)")
            return station

    # No match found — return None rather than guessing
    print(f"  ⚠️  Could not resolve station for location: '{location}'")
    return None