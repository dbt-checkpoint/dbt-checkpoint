import pytest

from dbt_checkpoint.check_column_name_contract import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix_version_v1.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        False,
        True,
        True,
        1,
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        False,
        True,
        1,
    ),
    (
        ["aa/bb/with_boolean_column_without_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        True,
        1,
    ),
    (
        ["aa/bb/without_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        True,
        1,
    ),
    (
        ["aa/bb/without_boolean_column_without_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True,
        True,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "pattern",
        "dtype",
        "valid_manifest",
        "valid_catalog",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_column_name_contract(
    input_args,
    pattern,
    dtype,
    valid_manifest,
    valid_catalog,
    valid_config,
    expected_status_code,
    catalog_path_str,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    input_args.extend(["--pattern", pattern])
    input_args.extend(["--dtype", dtype])
    status_code = main(input_args)
    assert status_code == expected_status_code
