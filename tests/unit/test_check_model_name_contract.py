import pytest

from pre_commit_dbt.check_model_name_contract import main


# Input args, valid manifest, expected return value
TESTS = (
    (["aa/bb/catalog_cols.sql"], "catalog_.*", True, 0),
    (["aa/bb/catalog_cols.sql"], "catalog_.*", False, 1),
    (["aa/bb/catalog_cols.sql"], ".*_cols", True, 0),
    (["aa/bb/catalog_cols.sql"], ".*_col", True, 0),
    (["aa/bb/catalog_cols.sql"], ".*_col$", True, 1),
    (["aa/bb/catalog_cols.sql"], "catalog_cols", True, 0),
    (["aa/bb/catalog_cols.sql"], "cat_.*", True, 1),
)


@pytest.mark.parametrize(
    ("input_args", "pattern", "valid_catalog", "expected_status_code"), TESTS
)
def test_model_name_contract(
    input_args, pattern, valid_catalog, expected_status_code, catalog_path_str
):
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    input_args.extend(["--pattern", pattern])
    status_code = main(input_args)
    assert status_code == expected_status_code
