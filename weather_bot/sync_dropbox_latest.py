"""Pull the latest analysis bundle from Dropbox into the local workspace."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .analysis_bundle import DEFAULT_ANALYSIS_BUNDLE_LABEL
from .dropbox_exports import dropbox_settings_from_env_or_options, sync_dropbox_latest_bundle_to_local


def build_parser() -> argparse.ArgumentParser:
    settings = dropbox_settings_from_env_or_options()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="", help="Local destination directory. Defaults to repo-local dropbox_sync/.")
    parser.add_argument("--label", default=DEFAULT_ANALYSIS_BUNDLE_LABEL, help="Stable bundle label prefix.")
    parser.add_argument("--dropbox-root", default=settings.get("dropbox_root") or "/", help="Dropbox root folder.")
    parser.add_argument("--dropbox-token", default=settings.get("dropbox_token") or "", help="Dropbox access token.")
    parser.add_argument(
        "--dropbox-refresh-token",
        default=settings.get("dropbox_refresh_token") or "",
        help="Dropbox refresh token for auto-renewed auth.",
    )
    parser.add_argument("--dropbox-app-key", default=settings.get("dropbox_app_key") or "", help="Dropbox app key.")
    parser.add_argument("--dropbox-app-secret", default=settings.get("dropbox_app_secret") or "", help="Dropbox app secret.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = sync_dropbox_latest_bundle_to_local(
        dropbox_token=args.dropbox_token or None,
        dropbox_refresh_token=args.dropbox_refresh_token or None,
        dropbox_app_key=args.dropbox_app_key or None,
        dropbox_app_secret=args.dropbox_app_secret or None,
        dropbox_root=args.dropbox_root,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        label=args.label,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
