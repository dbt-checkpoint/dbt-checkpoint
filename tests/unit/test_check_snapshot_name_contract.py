import pytest

from dbt_checkpoint.check_snapshot_name_contract import main

# Input args, valid manifest, valid_config, expected return value
TESTS = (
    (["aa/bb/some_snapshot.sql", "--is_test"], ".*_snapshot",  True, True, 0),
    (["aa/bb/some_snapshot.sql", "--is_test"], "snapshot_.*",  True, True, 1),
    (["aa/bb/some_snapshot.sql", "--is_test"], "snapshot_.*", False, True, 1),
    (["aa/bb/some_snapshot.sql", "--is_test"], ".*_snapshot", False, True, 1),
    (["aa/bb/some_snapshot.sql", "--is_test"], "snapshot_.*", True, False, 1),
)

@pytest.mark.parametrize(
    (
        "input_args",
        "pattern",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_snapshot_name_contract(
    input_args,
    pattern,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    input_args.extend(["--pattern", pattern])

    status_code = main(input_args)
    assert status_code == expected_status_code
