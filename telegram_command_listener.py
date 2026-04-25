"""Manual Telegram backup controller entrypoint."""

from __future__ import annotations

from weather_bot.bootstrap import get_application
from weather_bot.telegram_listener import TelegramBackupService


def main():
    app = get_application()
    app.start_background_services()
    try:
        TelegramBackupService(app).run_forever(require_credentials=True)
    finally:
        app.stop_background_services()


if __name__ == "__main__":
    main()
