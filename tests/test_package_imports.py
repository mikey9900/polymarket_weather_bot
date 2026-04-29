from __future__ import annotations

import json
import subprocess
import sys


def test_weather_bot_package_import_is_lazy():
    script = """
import importlib
import json
import sys

package = importlib.import_module("weather_bot")
result = {
    "bootstrap_loaded_initially": "weather_bot.bootstrap" in sys.modules,
    "get_application_callable": callable(package.get_application),
    "temperature_module_name": package.temperature.__name__,
    "bootstrap_loaded_after_temperature": "weather_bot.bootstrap" in sys.modules,
}
print(json.dumps(result))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout.strip())

    assert payload["bootstrap_loaded_initially"] is False
    assert payload["get_application_callable"] is True
    assert payload["temperature_module_name"] == "weather_bot.temperature"
    assert payload["bootstrap_loaded_after_temperature"] is False
