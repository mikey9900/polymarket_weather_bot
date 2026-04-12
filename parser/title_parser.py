# =============================================================
# parser/title_parser.py
#
# PURPOSE:
#   Parses the title/question text of a Polymarket RAIN market
#   and extracts structured information: location, date window.
#
# NOTE:
#   This handles RAIN markets specifically (not temperature).
#   Temperature markets are handled by weather_parser.py.
#
# EXAMPLE INPUT:
#   "Will it rain in New York City on April 14?"
#   "Will there be measurable precipitation at JFK tomorrow?"
#
# EXAMPLE OUTPUT:
#   {
#     "weather_type": "rain",
#     "location": "new york city",
#     "window_start_local": datetime(2026, 4, 14, 0, 0, 0),
#     "window_end_local":   datetime(2026, 4, 14, 23, 59, 59),
#     "raw_title": "Will it rain in New York City on April 14?"
#   }
# =============================================================

import re
from datetime import datetime, timedelta


# -------------------------------------------------------------
# RAIN KEYWORDS
#
# These are the words/phrases Polymarket uses in rain market
# titles. We check for any of these to confirm it's a rain market.
# -------------------------------------------------------------
RAIN_KEYWORDS = [
    "rain",
    "rainfall",
    "precipitation",
    "precip",
    "measurable precipitation",
    "recorded precipitation",
    "precipitation recorded",
]


def parse_rain_title(title: str):
    """
    Parses a Polymarket rain market title and returns structured data.

    Args:
        title (str): The market question, e.g.
                     "Will it rain in New York City on April 14?"

    Returns:
        dict with keys: weather_type, location, window_start_local,
                        window_end_local, raw_title
        OR None if this doesn't look like a rain market, or if we
        can't confidently extract the location and date.
    """

    # Work in lowercase to make all matching case-insensitive
    t = title.lower()

    # ----------------------------------------------------------
    # STEP 1: Confirm this is a rain market
    # If none of our rain keywords appear, it's not a rain market.
    # ----------------------------------------------------------
    if not any(k in t for k in RAIN_KEYWORDS):
        return None  # not a rain market

    # ----------------------------------------------------------
    # STEP 2: Extract the date
    #
    # Polymarket uses a few common phrasings:
    #   "...tomorrow"
    #   "...on April 14"
    #   "...on May 3"
    #   etc.
    #
    # We handle "tomorrow" precisely, and month-name dates as a
    # safe placeholder (just using today's date for now — this
    # should be improved to parse the actual day number).
    # ----------------------------------------------------------
    now = datetime.now()

    if "tomorrow" in t:
        # Clear: one day from now
        date = now + timedelta(days=1)

    elif re.search(
        r"on\s+(january|february|march|april|may|june|july|"
        r"august|september|october|november|december)",
        t
    ):
        # Month name found — use today as a safe placeholder
        # TODO: parse the actual day number from the title
        # e.g. "April 14" → datetime(2026, 4, 14)
        date = now

    else:
        # Can't determine the date — return None rather than guess
        return None

    # ----------------------------------------------------------
    # STEP 3: Extract the location
    #
    # Polymarket rain titles use either:
    #   "...in [City Name]..."
    #   "...at [Airport Name]..."
    #
    # We use a regex to grab whatever follows "in" or "at".
    # ----------------------------------------------------------
    location_match = re.search(r"(in|at)\s+([a-z\s]+)", t)
    if not location_match:
        # No location found — can't use this market
        return None

    # group(2) captures everything after "in " or "at "
    # .strip() removes any trailing whitespace
    location = location_match.group(2).strip()

    # ----------------------------------------------------------
    # STEP 4: Return structured result
    # The window covers the full calendar day (midnight to 11:59pm)
    # ----------------------------------------------------------
    return {
        "weather_type": "rain",
        "location": location,

        # Start of the day: midnight
        "window_start_local": date.replace(hour=0, minute=0, second=0, microsecond=0),

        # End of the day: 11:59:59 PM
        "window_end_local": date.replace(hour=23, minute=59, second=59, microsecond=0),

        # Keep the original title for logging / debugging
        "raw_title": title,
    }