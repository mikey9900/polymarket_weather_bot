from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
import re


VERSION_RE = re.compile(r'^\s*version\s*:\s*"?(?P<version>[^"\r\n#]+)"?\s*$')
ZERO_REFS = {"", "0" * 40}


@dataclass(frozen=True)
class AddonPolicy:
    name: str
    directory: str
    config_path: str


@dataclass(frozen=True)
class PolicyOutcome:
    policy: AddonPolicy
    changed: bool
    passed: bool
    detail: str
    before_version: str | None = None
    after_version: str | None = None


POLICIES = (
    AddonPolicy(
        name="Polymarket Weather Bot",
        directory="weather-bot",
        config_path="weather-bot/config.yaml",
    ),
    AddonPolicy(
        name="Weather Codex Runner",
        directory="weather-codex",
        config_path="weather-codex/config.yaml",
    ),
)


def normalize_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def extract_version(config_text: str, config_path: str) -> str:
    for line in config_text.splitlines():
        match = VERSION_RE.match(line.strip())
        if match:
            return match.group("version").strip()
    raise ValueError(f"Could not find a version entry in {config_path}")


def parse_version(version: str) -> tuple[int, ...]:
    parts = version.strip().split(".")
    parsed: list[int] = []
    for part in parts:
        if not part.isdigit():
            raise ValueError(
                f"Unsupported version '{version}'. Expected dot-separated integers."
            )
        parsed.append(int(part))
    return tuple(parsed)


def policy_changed_paths(policy: AddonPolicy, changed_paths: list[str]) -> list[str]:
    return [path for path in changed_paths if path != policy.config_path]


def evaluate_policy(
    policy: AddonPolicy,
    changed_paths: list[str],
    before_config: str | None,
    after_config: str | None,
) -> PolicyOutcome:
    matching_paths = policy_changed_paths(policy, changed_paths)
    if not matching_paths:
        return PolicyOutcome(
            policy=policy,
            changed=False,
            passed=True,
            detail=f"{policy.name}: no shipped repo files changed beyond {policy.config_path}.",
        )

    if not after_config:
        return PolicyOutcome(
            policy=policy,
            changed=True,
            passed=False,
            detail=(
                f"{policy.name}: addon files changed but {policy.config_path} "
                "is missing in the new revision."
            ),
        )

    after_version = extract_version(after_config, policy.config_path)
    if before_config is None:
        return PolicyOutcome(
            policy=policy,
            changed=True,
            passed=True,
            detail=(
                f"{policy.name}: new addon detected with version {after_version}."
            ),
            after_version=after_version,
        )

    before_version = extract_version(before_config, policy.config_path)
    if parse_version(after_version) <= parse_version(before_version):
        return PolicyOutcome(
            policy=policy,
            changed=True,
            passed=False,
            detail=(
                f"{policy.name}: shipped repo files changed ({', '.join(matching_paths)}) "
                f"but version stayed at {after_version}. "
                f"Bump {policy.config_path} above {before_version}."
            ),
            before_version=before_version,
            after_version=after_version,
        )

    return PolicyOutcome(
        policy=policy,
        changed=True,
        passed=True,
        detail=(
            f"{policy.name}: version bumped from {before_version} to {after_version}."
        ),
        before_version=before_version,
        after_version=after_version,
    )


def run_git(
    repo_root: Path,
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=check,
    )


def resolve_base_ref(repo_root: Path, base_ref: str | None, head_ref: str) -> str | None:
    candidate = (base_ref or "").strip()
    if candidate and candidate not in ZERO_REFS:
        return candidate

    result = run_git(repo_root, ["rev-list", "--max-count=1", f"{head_ref}^"], check=False)
    resolved = result.stdout.strip()
    if result.returncode != 0 or not resolved:
        return None
    return resolved


def changed_paths_between(repo_root: Path, base_ref: str, head_ref: str) -> list[str]:
    result = run_git(repo_root, ["diff", "--name-only", base_ref, head_ref])
    return [normalize_path(path) for path in result.stdout.splitlines() if path.strip()]


def git_show_file(repo_root: Path, ref: str, path: str) -> str | None:
    result = run_git(repo_root, ["show", f"{ref}:{path}"], check=False)
    if result.returncode != 0:
        return None
    return result.stdout


def check_policies(repo_root: Path, base_ref: str, head_ref: str) -> list[PolicyOutcome]:
    changed_paths = changed_paths_between(repo_root, base_ref, head_ref)
    outcomes: list[PolicyOutcome] = []
    for policy in POLICIES:
        before_config = git_show_file(repo_root, base_ref, policy.config_path)
        after_config = git_show_file(repo_root, head_ref, policy.config_path)
        outcomes.append(evaluate_policy(policy, changed_paths, before_config, after_config))
    return outcomes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Require Home Assistant addon version bumps when shipped repo files change."
    )
    parser.add_argument("--base", help="Base git ref or commit SHA to compare against.")
    parser.add_argument("--head", default="HEAD", help="Head git ref or commit SHA.")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parent.parent),
        help="Repository root to run git commands in.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    head_ref = args.head
    base_ref = resolve_base_ref(repo_root, args.base, head_ref)
    if not base_ref:
        print("HA addon version check skipped: no base revision was available.")
        return 0

    outcomes = check_policies(repo_root, base_ref, head_ref)
    relevant_outcomes = [outcome for outcome in outcomes if outcome.changed]
    failures = [outcome for outcome in relevant_outcomes if not outcome.passed]

    if not relevant_outcomes:
        print("HA addon version check passed: no shipped repo files changed beyond addon version files.")
        return 0

    for outcome in relevant_outcomes:
        print(outcome.detail)

    if failures:
        print("\nHA addon version check failed.")
        return 1

    print("\nHA addon version check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
