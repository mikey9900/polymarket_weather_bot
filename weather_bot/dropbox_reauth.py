"""Interactive helper to generate a Dropbox refresh token for the weather bot."""

from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request


def exchange_code(app_key: str, app_secret: str, code: str) -> dict[str, str]:
    payload = urllib.parse.urlencode(
        {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": app_key,
            "client_secret": app_secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url="https://api.dropboxapi.com/oauth2/token",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8")
    payload_json = json.loads(raw) if raw else {}
    if not isinstance(payload_json, dict):
        raise RuntimeError("Dropbox token exchange returned a non-object response.")
    return {str(key): str(value) for key, value in payload_json.items()}


def main() -> int:
    print("=== Weather Bot Dropbox Re-Authorization ===\n")
    print("Use this once to generate a long-lived refresh token for the weather bot.\n")
    print("1. In Dropbox App Console, make sure your app has file read/write and sharing scopes.\n")

    app_key = input("Dropbox App Key: ").strip()
    app_secret = input("Dropbox App Secret: ").strip()
    if not app_key or not app_secret:
        print("App key and app secret are required.")
        return 1

    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={urllib.parse.quote(app_key)}"
        "&response_type=code"
        "&token_access_type=offline"
    )
    print("\n2. Open this URL, approve access, and copy the authorization code:\n")
    print(f"   {auth_url}\n")
    code = input("3. Paste the authorization code here: ").strip()
    if not code:
        print("No authorization code entered.")
        return 1

    print("\nExchanging code for tokens...\n")
    try:
        result = exchange_code(app_key, app_secret, code)
    except Exception as exc:  # pragma: no cover - interactive helper
        print(f"Dropbox token exchange failed: {exc}")
        return 1

    refresh_token = str(result.get("refresh_token") or "").strip()
    access_token = str(result.get("access_token") or "").strip()
    if not refresh_token:
        print(f"No refresh token returned: {json.dumps(result, indent=2)}")
        return 1

    print("=== Success ===\n")
    print("Paste these into the Home Assistant add-on Configuration tab:\n")
    print(f'dropbox_refresh_token: "{refresh_token}"')
    print(f'dropbox_app_key: "{app_key}"')
    print(f'dropbox_app_secret: "{app_secret}"')
    print('dropbox_root: "/"')
    if access_token:
        print("\nOptional quick-test access token:")
        print(f'  dropbox_token: "{access_token}"')
    print("\nThen save the add-on config and restart the add-on.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
