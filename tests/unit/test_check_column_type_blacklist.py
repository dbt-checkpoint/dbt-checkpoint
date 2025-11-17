import pytest

from dbt_checkpoint.check_column_type_blacklist import main

# Input args, valid manifest, valid_catalog, valid_config, expected return value
TESTS = (
    # Test with no blacklisted types present
    (
        ["aa/bb/catalog_cols.sql", "--types", "timestamp_ntz", "timestamp_ltz", "--is_test"],
        True,
        True,
        True,
        0,
    ),
    # Test with blacklisted type present (col1 is TEXT, we blacklist it)
    (
        ["aa/bb/catalog_cols.sql", "--types", "text", "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test with multiple blacklisted types, one present
    (
        ["aa/bb/catalog_cols.sql", "--types", "text", "timestamp_ntz", "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test case insensitive matching
    (
        ["aa/bb/catalog_cols.sql", "--types", "TEXT", "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test without catalog
    (
        ["aa/bb/catalog_cols.sql", "--types", "timestamp_ntz", "--is_test"],
        True,
        False,
        True,
        1,
    ),
    # Test without manifest
    (
        ["aa/bb/catalog_cols.sql", "--types", "timestamp_ntz", "--is_test"],
        False,
        True,
        True,
        1,
    ),
    # Test model not in catalog
    (
        ["aa/bb/without_catalog.sql", "--types", "timestamp_ntz", "--is_test"],
        True,
        True,
        True,
        1,
    ),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "valid_manifest",
        "valid_catalog",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_column_type_blacklist(
    input_args,
    valid_manifest,
    valid_catalog,
    valid_config,
    expected_status_code,
    manifest_path_str,
    catalog_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code


def test_missing_types_argument(manifest_path_str, catalog_path_str, config_path_str):
    """Test that the hook fails when --types argument is missing"""
    input_args = [
        "aa/bb/catalog_cols.sql",
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--catalog",
        catalog_path_str,
        "--config",
        config_path_str,
    ]
    with pytest.raises(SystemExit):
        main(input_args)