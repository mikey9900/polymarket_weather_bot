"""Dropbox helpers for latest analysis bundle export and local sync."""

from __future__ import annotations

import json
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any

import requests

from .paths import DROPBOX_SYNC_ROOT, REPO_ROOT

DROPBOX_OAUTH_URL = "https://api.dropboxapi.com/oauth2/token"
DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
DROPBOX_DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download"
DROPBOX_SHARED_LINK_CREATE_URL = "https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings"
DROPBOX_SHARED_LINK_LIST_URL = "https://api.dropboxapi.com/2/sharing/list_shared_links"


def dropbox_settings_from_env_or_options() -> dict[str, str]:
    options = _load_ha_options()
    return {
        "dropbox_token": str(os.getenv("WEATHER_DROPBOX_TOKEN") or options.get("dropbox_token") or "").strip(),
        "dropbox_refresh_token": str(
            os.getenv("WEATHER_DROPBOX_REFRESH_TOKEN") or options.get("dropbox_refresh_token") or ""
        ).strip(),
        "dropbox_app_key": str(os.getenv("WEATHER_DROPBOX_APP_KEY") or options.get("dropbox_app_key") or "").strip(),
        "dropbox_app_secret": str(
            os.getenv("WEATHER_DROPBOX_APP_SECRET") or options.get("dropbox_app_secret") or ""
        ).strip(),
        "dropbox_root": str(os.getenv("WEATHER_DROPBOX_ROOT") or options.get("dropbox_root") or "/").strip() or "/",
    }


def build_dropbox_auth(
    *,
    dropbox_token: str | None = None,
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
) -> dict[str, Any] | None:
    auth = {
        "access_token": str(dropbox_token or "").strip(),
        "refresh_token": str(dropbox_refresh_token or "").strip(),
        "app_key": str(dropbox_app_key or "").strip(),
        "app_secret": str(dropbox_app_secret or "").strip(),
        "_cached_access_token": None,
        "_cached_expires_at": None,
    }
    if auth["refresh_token"]:
        if not auth["app_key"] or not auth["app_secret"]:
            raise RuntimeError("Dropbox refresh token requires both dropbox_app_key and dropbox_app_secret.")
        return auth
    if auth["access_token"]:
        return auth
    return None


def normalize_dropbox_root(dropbox_root: str | None) -> str:
    root = str(dropbox_root or "/").strip().replace("\\", "/")
    if not root:
        return "/"
    if not root.startswith("/"):
        root = f"/{root}"
    while "//" in root:
        root = root.replace("//", "/")
    return root.rstrip("/") or "/"


def safe_archive_label(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(label or ""))
    cleaned = cleaned.strip("-_")
    return cleaned or "WEATHER-BOT"


def resolve_dropbox_access_token(dropbox_auth: dict[str, Any]) -> str:
    cached_token = dropbox_auth.get("_cached_access_token")
    cached_expires_at = dropbox_auth.get("_cached_expires_at")
    if cached_token and isinstance(cached_expires_at, (int, float)) and float(cached_expires_at) > time.time() + 60:
        return str(cached_token)

    refresh_token = str(dropbox_auth.get("refresh_token") or "").strip()
    if refresh_token:
        response = requests.post(
            DROPBOX_OAUTH_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": str(dropbox_auth.get("app_key") or ""),
                "client_secret": str(dropbox_auth.get("app_secret") or ""),
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json() if response.text else {}
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("Dropbox OAuth refresh succeeded but returned no access_token.")
        expires_in = int(payload.get("expires_in") or 0)
        dropbox_auth["_cached_access_token"] = access_token
        dropbox_auth["_cached_expires_at"] = time.time() + max(0, expires_in)
        return access_token

    access_token = str(dropbox_auth.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Dropbox authentication is not configured.")
    return access_token


def dropbox_upload_file(local_path: str | Path, dropbox_path: str, dropbox_auth: dict[str, Any]) -> dict[str, Any]:
    path = Path(local_path)
    response = _dropbox_request(
        url=DROPBOX_UPLOAD_URL,
        dropbox_auth=dropbox_auth,
        headers={
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": json.dumps(
                {"path": str(dropbox_path), "mode": "overwrite", "autorename": False, "mute": True}
            ),
        },
        data=path.read_bytes(),
        timeout=300,
    )
    if not response.get("ok"):
        return response
    payload = response.get("payload") or {}
    return {
        "ok": True,
        "status": response.get("status"),
        "bytes": path.stat().st_size,
        "path": str(dropbox_path),
        "payload": payload,
    }


def dropbox_download_file(dropbox_path: str, dropbox_auth: dict[str, Any], local_path: str | Path) -> dict[str, Any]:
    path = Path(local_path)
    response = _dropbox_request(
        url=DROPBOX_DOWNLOAD_URL,
        dropbox_auth=dropbox_auth,
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "Dropbox-API-Arg": json.dumps({"path": str(dropbox_path)}),
        },
        data=b"",
        timeout=300,
    )
    if not response.get("ok"):
        response["path"] = str(path)
        return response
    body = response.get("body") or b""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return {"ok": True, "status": response.get("status"), "bytes": len(body), "path": str(path)}


def dropbox_create_or_get_shared_link(dropbox_path: str, dropbox_auth: dict[str, Any]) -> str | None:
    create_response = _dropbox_request_json(
        DROPBOX_SHARED_LINK_CREATE_URL,
        dropbox_auth,
        {"path": str(dropbox_path)},
        allow_409=True,
    )
    if create_response.get("ok"):
        payload = create_response.get("payload") or {}
        return payload.get("url")
    if int(create_response.get("status") or 0) != 409:
        return None
    payload = create_response.get("payload") or {}
    metadata = ((payload.get("error") or {}) if isinstance(payload, dict) else {}).get("metadata")
    if isinstance(metadata, dict) and metadata.get("url"):
        return str(metadata.get("url"))
    list_response = _dropbox_request_json(
        DROPBOX_SHARED_LINK_LIST_URL,
        dropbox_auth,
        {"path": str(dropbox_path), "direct_only": True},
    )
    if not list_response.get("ok"):
        return None
    payload = list_response.get("payload") or {}
    links = payload.get("links") or []
    if links:
        return str((links[0] or {}).get("url") or "")
    return None


def sync_dropbox_latest_bundle_to_local(
    *,
    dropbox_token: str | None = None,
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
    dropbox_root: str = "/",
    output_dir: str | Path | None = None,
    label: str = "WEATHER-BOT",
) -> dict[str, Any]:
    dropbox_auth = build_dropbox_auth(
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
    )
    if not dropbox_auth:
        raise RuntimeError("Dropbox authentication is not configured.")

    safe_label = safe_archive_label(label)
    root = normalize_dropbox_root(dropbox_root)
    default_output_dir = DROPBOX_SYNC_ROOT if DROPBOX_SYNC_ROOT.is_absolute() else REPO_ROOT / "dropbox_sync"
    out = Path(output_dir) if output_dir else default_output_dir
    out.mkdir(parents=True, exist_ok=True)

    filenames = {
        "latest_bundle_zip": f"{safe_label}_latest_bundle.zip",
        "latest_index_json": f"{safe_label}_latest_index.json",
    }
    downloads = {}
    for key, filename in filenames.items():
        remote_path = f"{root}/latest/{filename}" if root != "/" else f"/latest/{filename}"
        local_path = out / filename
        downloads[key] = dropbox_download_file(remote_path, dropbox_auth, local_path)
        downloads[key]["remote_path"] = remote_path
    extracted_bundle_dir = None
    extraction_error = None
    bundle_download = downloads.get("latest_bundle_zip") or {}
    bundle_path = out / filenames["latest_bundle_zip"]
    if bundle_download.get("ok") and bundle_path.exists():
        extracted_dir = out / f"{safe_label}_latest_bundle"
        if extracted_dir.exists():
            shutil.rmtree(extracted_dir)
        extracted_dir.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(bundle_path) as archive:
                archive.extractall(extracted_dir)
            extracted_bundle_dir = str(extracted_dir)
        except Exception as exc:
            extraction_error = f"{type(exc).__name__}: {exc}"
    return {
        "ok": all(bool(item.get("ok")) for item in downloads.values()) and extraction_error is None,
        "output_dir": str(out),
        "downloads": downloads,
        "extracted_bundle_dir": extracted_bundle_dir,
        "extraction_error": extraction_error,
    }


def _load_ha_options() -> dict[str, Any]:
    path = Path("/data/options.json")
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dropbox_request(
    *,
    url: str,
    dropbox_auth: dict[str, Any],
    headers: dict[str, str],
    data: bytes,
    timeout: int,
) -> dict[str, Any]:
    try:
        token = resolve_dropbox_access_token(dropbox_auth)
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", **headers},
            data=data,
            timeout=timeout,
        )
        content_type = str(response.headers.get("Content-Type") or "")
        payload: Any = {}
        if "application/json" in content_type and response.text:
            payload = response.json()
        if response.status_code >= 400:
            error_text = response.text or f"HTTP {response.status_code}"
            return {
                "ok": False,
                "status": response.status_code,
                "error": error_text,
                "error_details": _dropbox_error_details(error_text),
                "payload": payload if isinstance(payload, dict) else {},
            }
        return {
            "ok": True,
            "status": response.status_code,
            "payload": payload if isinstance(payload, dict) else {},
            "body": response.content,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": None,
            "error": str(exc),
            "error_details": _dropbox_error_details(str(exc)),
        }


def _dropbox_request_json(
    url: str,
    dropbox_auth: dict[str, Any],
    payload: dict[str, Any],
    *,
    allow_409: bool = False,
) -> dict[str, Any]:
    try:
        token = resolve_dropbox_access_token(dropbox_auth)
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        body = response.text or ""
        parsed = response.json() if body else {}
        if response.status_code >= 400 and not (allow_409 and response.status_code == 409):
            return {
                "ok": False,
                "status": response.status_code,
                "error": body or f"HTTP {response.status_code}",
                "error_details": _dropbox_error_details(body),
                "payload": parsed if isinstance(parsed, dict) else {},
            }
        return {
            "ok": 200 <= response.status_code < 300,
            "status": response.status_code,
            "payload": parsed if isinstance(parsed, dict) else {},
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": None,
            "error": str(exc),
            "error_details": _dropbox_error_details(str(exc)),
            "payload": {},
        }


def _dropbox_error_details(raw_error: str) -> dict[str, Any]:
    text = str(raw_error or "").strip()
    details: dict[str, Any] = {"raw": text}
    if not text:
        return details
    try:
        payload = json.loads(text)
    except Exception:
        if "required scope '" in text:
            scope = text.split("required scope '", 1)[1].split("'", 1)[0]
            details["reason"] = "missing_scope"
            details["scope"] = scope
            details["friendly"] = f"Dropbox token is missing the '{scope}' permission."
        return details
    error_obj = payload.get("error") if isinstance(payload, dict) else None
    summary = payload.get("error_summary") if isinstance(payload, dict) else None
    if summary:
        details["summary"] = summary
    if isinstance(error_obj, dict):
        details["error"] = error_obj
    details["friendly"] = details.get("friendly") or summary or text
    return details
