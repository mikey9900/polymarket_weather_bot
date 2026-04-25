"""Legacy compatibility wrapper for the platformized temperature scan."""

from __future__ import annotations

from weather_bot.bootstrap import get_application


def run_weather_scan(limit: int = 300):
    app = get_application()
    batch, results = app.runtime.run_temperature_scan(send_alerts=True, limit=limit)
    print(
        f"Temperature scan finished: events={batch.total_events} "
        f"signals={len(batch.signals)} opened={sum(1 for item in results if item.position)}"
    )
    return batch, results


if __name__ == "__main__":
    run_weather_scan()
