"""Main process entrypoint for the weather platform."""

from __future__ import annotations

import time

from .bootstrap import get_application
from .telegram_listener import TelegramBackupService


def main() -> None:
    app = get_application()
    telegram_backup = TelegramBackupService(app)
    app.start_background_services()
    telegram_backup.start()
    try:
        while True:
            time.sleep(1.0)
    finally:
        telegram_backup.stop()
        app.stop_background_services()


if __name__ == "__main__":
    main()
