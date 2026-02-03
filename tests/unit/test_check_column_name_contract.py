import pytest
import yaml
import subprocess
import sys

from dbt_checkpoint.check_column_name_contract import main

TESTS = (
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, True, True, 0, None,
    ),
    (
        ["aa/bb/without_boolean_column_without_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, True, True, 0, None,
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, True, False, 0, None,
    ),
    (["aa/bb/with_float_column.sql", "is_test"], ".*_amount", "numeric", True, True, True, 0, None),
    (["aa/bb/with_array_column.sql", "is_test"], ".*_ids", "complex", True, True, True, 0, None),
    (["aa/bb/with_struct_column.sql", "is_test"], ".*_details", "complex", True, True, True, 0, None),
    (
        ["aa/bb/with_array_column.sql", "is_test"], ".*_ids", "struct", True, True, True, 0,
        {"check-column-name-contract": {"type_mappings": {"struct": ["array"]}}},
    ),
    # Test a file that is not a model, should pass
    (["aa/bb/not_a_model.sql", "is_test"], ".*", "string", True, True, True, 0, None),

    # Test a model in manifest but not in catalog, should pass
    (["aa/bb/manifest_only_model.sql", "is_test"], ".*", "string", True, True, True, 0, None),
    
    # Test a model with weird/missing column types, should fail
    (["aa/bb/column_with_edge_case_types.sql", "is_test"], ".*", "string", True, True, True, 1, None),
    
    # Test using a dtype that is not in the default mappings, should fail
    (["aa/bb/with_float_column.sql", "is_test"], "total_amount", "money", True, True, True, 1, None),

    # --- Failing Tests ---
    (
        ["aa/bb/with_boolean_column_without_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, True, True, 1, None,
    ),
    (
        ["aa/bb/without_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, True, True, 1, None,
    ),
    (["aa/bb/with_float_column.sql", "is_test"], ".*_amount", "string", True, True, True, 1, None),

    # --- Coverage Increase: Test for missing manifest/catalog ---
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        False, True, True, 1, None,  # Missing manifest
    ),
    (
        ["aa/bb/with_boolean_column_with_prefix.sql", "is_test"],
        "is_.*",
        "boolean",
        True, False, True, 1, None,  # Missing catalog
    ),
)

@pytest.fixture
def custom_config_path(tmpdir):
    """Fixture to create a temporary config file for user-defined mappings."""
    def _create_config(config_dict):
        if config_dict is None:
            return None
        # Ensure the config file has the required 'version' key
        config_dict_with_version = {"version": 1, **config_dict}
        config_file = tmpdir.join("custom_config.yaml")
        with open(config_file, "w") as f:
            yaml.dump(config_dict_with_version, f)
        return str(config_file)
    return _create_config


@pytest.mark.parametrize(
    (
        "input_args",
        "pattern",
        "dtype",
        "valid_manifest",
        "valid_catalog",
        "valid_config",
        "expected_status_code",
        "custom_config_dict",
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
    custom_config_dict,
    catalog_path_str,
    manifest_path_str,
    config_path_str,
    custom_config_path,
):
    # Determine which config file to use for this test run
    test_config_path = custom_config_path(custom_config_dict) or config_path_str
    
    # We need to make a copy of input_args to avoid modifying the original list
    current_input_args = input_args[:]

    if valid_manifest:
        current_input_args.extend(["--manifest", manifest_path_str])

    if valid_catalog:
        current_input_args.extend(["--catalog", catalog_path_str])

    if valid_config:
        current_input_args.extend(["--config", test_config_path])

    current_input_args.extend(["--pattern", pattern])
    current_input_args.extend(["--dtype", dtype])
    
    status_code = main(current_input_args)
    assert status_code == expected_status_code


def test_check_column_name_contract_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    # Create dummy files to pass to the script
    sql_file = tmp_path / "model.sql"
    sql_file.write_text("select 1")

    script_path = "dbt_checkpoint/check_column_name_contract.py"

    # We expect this to fail because we are not providing a manifest file
    process = subprocess.run(
        [
            sys.executable,
            script_path,
            str(sql_file),
            "--pattern",
            ".*",
            "--dtypes",
            "string",
        ],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 1
    assert "Unable to load manifest file" in process.stdout