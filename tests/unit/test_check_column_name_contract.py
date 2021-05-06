import pytest

from pre_commit_dbt.check_column_name_contract import main


# Input args, valid manifest, expected return value
TESTS = (
    (["aa/bb/with_boolean_column_with_prefix.sql"], "is_.*", "boolean", True, 0),
    (["aa/bb/with_boolean_column_with_prefix.sql"], "is_.*", "boolean", False, 1),
    (["aa/bb/with_boolean_column_without_prefix.sql"], "is_.*", "boolean", True, 1),
    (["aa/bb/without_boolean_column_with_prefix.sql"], "is_.*", "boolean", True, 1),
    (["aa/bb/without_boolean_column_without_prefix.sql"], "is_.*", "boolean", True, 0),
)


@pytest.mark.parametrize(
    ("input_args", "pattern", "dtype", "valid_catalog", "expected_status_code"), TESTS
)
def test_check_column_name_contract(
    input_args, pattern, dtype, valid_catalog, expected_status_code, catalog_path_str
):
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    input_args.extend(["--pattern", pattern])
    input_args.extend(["--dtype", dtype])
    status_code = main(input_args)
    assert status_code == expected_status_code
