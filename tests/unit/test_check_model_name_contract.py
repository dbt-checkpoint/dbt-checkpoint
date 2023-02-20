import pytest

from pre_commit_dbt.check_model_name_contract import main


# Input args, valid manifest, valid_config, expected return value
TESTS = (
    (["aa/bb/catalog_cols.sql", "--is_test"], "catalog_.*", True, True, True, 0),
    (["aa/bb/catalog_cols.sql", "--is_test"], "catalog_.*", False, True, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], "catalog_.*", True, False, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], ".*_cols", True, True, True, 0),
    (["aa/bb/catalog_cols.sql", "--is_test"], ".*_col", True, True, True, 0),
    (["aa/bb/catalog_cols.sql", "--is_test"], ".*_col$", True, True, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], "catalog_cols", True, True, True, 0),
    (["aa/bb/catalog_cols.sql", "--is_test"], "cat_.*", True, True, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], "catalog_.*", True, True, False, 0),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "pattern",
        "valid_catalog",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_model_name_contract(
    input_args,
    pattern,
    valid_catalog,
    valid_manifest,
    valid_config,
    expected_status_code,
    catalog_path_str,
    manifest_path_str,
    config_path_str,
):
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    input_args.extend(["--pattern", pattern])

    status_code = main(input_args)
    assert status_code == expected_status_code
