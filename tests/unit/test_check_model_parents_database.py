import pytest

from dbt_checkpoint.check_model_parents_database import main

# Input schema, input_args, valid_manifest, valid_config, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/parent_child.sql", "--is_test"], [], True, True, 1),
    (["aa/bb/parent_child.sql", "--is_test"], [], True, True, 1),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "prod", "prod2", "core"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "prod", "prod2"],
        True,
        True,
        1,
    ),
    (["aa/bb/parent_child.sql", "--is_test"], ["--whitelist", "prod"], True, True, 1),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "dev", "core"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "prod", "prod2", "core"],
        False,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--blacklist", "dev", "dev1"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--blacklist", "prod", "prod2", "dev1"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--blacklist", "prod", "prod2", "core"],
        True,
        True,
        1,
    ),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "core"], True, True, 1),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "prod2"], True, True, 1),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "prod"], True, True, 1),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "dev"], True, True, 0),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "dev"], True, False, 0),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "input_args",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_parents_database(
    input_schema,
    input_args,
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

    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
