# =============================================================
# alerts/scan_cache.py
#
# PURPOSE:
#   Stores the last few scan results in memory so that Telegram
#   inline button callbacks can retrieve and filter them.
#
#   Scan IDs are short strings like "s001", "s002", etc.
#   Only the last 3 scans are kept to avoid memory bloat.
# =============================================================

_cache: dict = {}   # scan_id -> list of discrepancy dicts
_counter: int = 0


def store_scan(discrepancies: list) -> str:
    """Store a scan's discrepancies and return the scan ID."""
    global _counter
    _counter += 1
    scan_id = f"s{_counter:03d}"

    # Keep only the last 3 scans
    if len(_cache) >= 3:
        oldest = sorted(_cache.keys())[0]
        del _cache[oldest]

    _cache[scan_id] = list(discrepancies)
    return scan_id


def get_scan(scan_id: str) -> list:
    """Retrieve discrepancies for a scan ID. Returns [] if not found."""
    return _cache.get(scan_id, [])


def get_edge(scan_id: str, index: int) -> dict:
    """Retrieve a single edge by scan ID and index. Returns {} if not found."""
    edges = get_scan(scan_id)
    if 0 <= index < len(edges):
        return edges[index]
    return {}
