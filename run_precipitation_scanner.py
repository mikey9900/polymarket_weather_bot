"""Legacy compatibility wrapper for the platformized precipitation scan."""

from __future__ import annotations

from weather_bot.bootstrap import get_application
from weather_bot.messages import format_scan_summary, format_signal_message


def run_precipitation_scan() -> list[dict]:
    app = get_application()
    batch, results = app.runtime.run_precipitation_scan(send_alerts=False)
    items = [
        {
            "text": format_scan_summary(
                batch,
                accepted_count=sum(1 for item in results if item.decision.accepted),
                opened_count=sum(1 for item in results if item.position),
            ),
            "url": None,
        }
    ]
    for result in results[:12]:
        items.append(
            {
                "text": format_signal_message(result.signal),
                "url": _event_url(result.signal.event_slug),
            }
        )
    return items


def _event_url(event_slug: str) -> str | None:
    if not event_slug:
        return None
    return f"https://polymarket.com/event/{event_slug}"


if __name__ == "__main__":
    for item in run_precipitation_scan():
        print(item["text"])
