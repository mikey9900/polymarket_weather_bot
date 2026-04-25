from weather_bot.ha_version_guard import AddonPolicy, evaluate_policy, extract_version, parse_version


POLICY = AddonPolicy(
    name="Polymarket Weather Bot",
    directory="weather-bot",
    config_path="weather-bot/config.yaml",
)


def test_extract_version_reads_yaml_value() -> None:
    config_text = 'name: "Polymarket Weather Bot"\nversion: "3.2.11"\n'

    assert extract_version(config_text, POLICY.config_path) == "3.2.11"


def test_parse_version_uses_numeric_semver_order() -> None:
    assert parse_version("3.2.11") < parse_version("3.2.12")


def test_evaluate_policy_requires_version_bump_for_addon_changes() -> None:
    outcome = evaluate_policy(
        POLICY,
        ["weather-bot/run.sh"],
        'version: "3.2.11"\n',
        'version: "3.2.11"\n',
    )

    assert outcome.changed is True
    assert outcome.passed is False
    assert "Bump weather-bot/config.yaml above 3.2.11." in outcome.detail


def test_evaluate_policy_accepts_bumped_version_for_addon_changes() -> None:
    outcome = evaluate_policy(
        POLICY,
        ["weather-bot/run.sh", "weather-bot/config.yaml"],
        'version: "3.2.11"\n',
        'version: "3.2.12"\n',
    )

    assert outcome.changed is True
    assert outcome.passed is True
    assert outcome.before_version == "3.2.11"
    assert outcome.after_version == "3.2.12"


def test_evaluate_policy_ignores_non_addon_changes() -> None:
    outcome = evaluate_policy(
        POLICY,
        ["weather_bot/runtime.py"],
        'version: "3.2.11"\n',
        'version: "3.2.11"\n',
    )

    assert outcome.changed is False
    assert outcome.passed is True
