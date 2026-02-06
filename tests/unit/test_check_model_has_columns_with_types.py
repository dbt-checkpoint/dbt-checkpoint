import pytest

from dbt_checkpoint.check_model_has_columns_with_types import main

# Input args, valid manifest, valid_catalog, valid_config, expected return value
TESTS = (
    # Test with matching columns and types
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "text"}]', "--is_test"],
        True,
        True,
        True,
        0,
    ),
    # Test with missing column
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "missing_col", "type": "text"}]', "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test with wrong type
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "integer"}]', "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test with multiple columns - all correct
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "text"}, {"name": "col2", "type": "text"}]', "--is_test"],
        True,
        True,
        True,
        0,
    ),
    # Test with multiple columns - one missing, one wrong type
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "integer"}, {"name": "missing_col", "type": "text"}]', "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test without catalog
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "text"}]', "--is_test"],
        True,
        False,
        True,
        1,
    ),
    # Test without manifest
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "col1", "type": "text"}]', "--is_test"],
        False,
        True,
        True,
        1,
    ),
    # Test model not in catalog
    (
        ["aa/bb/without_catalog.sql", "--columns", '[{"name": "col1", "type": "text"}]', "--is_test"],
        True,
        True,
        True,
        1,
    ),
    # Test case insensitive matching
    (
        ["aa/bb/catalog_cols.sql", "--columns", '[{"name": "COL1", "type": "TEXT"}]', "--is_test"],
        True,
        True,
        True,
        0,
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
def test_check_model_has_columns_with_types(
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


def test_invalid_json_columns(manifest_path_str, catalog_path_str, config_path_str):
    input_args = [
        "aa/bb/catalog_cols.sql",
        "--columns",
        "invalid json",
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--catalog",
        catalog_path_str,
        "--config",
        config_path_str,
    ]
    status_code = main(input_args)
    assert status_code == 1


def test_non_array_columns(manifest_path_str, catalog_path_str, config_path_str):
    input_args = [
        "aa/bb/catalog_cols.sql",
        "--columns",
        '{"name": "col1", "type": "text"}',
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--catalog",
        catalog_path_str,
        "--config",
        config_path_str,
    ]
    status_code = main(input_args)
    assert status_code == 1


def test_invalid_column_object(manifest_path_str, catalog_path_str, config_path_str):
    input_args = [
        "aa/bb/catalog_cols.sql",
        "--columns",
        '[{"name": "col1"}]',  # Missing type
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--catalog",
        catalog_path_str,
        "--config",
        config_path_str,
    ]
    status_code = main(input_args)
    assert status_code == 1