import pytest

from dbt_checkpoint.check_model_has_all_columns import main

# Input args, valid manifest, valid_config, expected return value
TESTS = (
    (["aa/bb/catalog_cols.sql", "--is_test"], True, True, True, 0),
    (["aa/bb/catalog_cols.sql", "--is_test"], False, True, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], True, False, True, 1),
    (["aa/bb/partial_catalog_cols.sql", "--is_test"], True, True, True, 1),
    (["aa/bb/only_catalog_cols.sql", "--is_test"], True, True, True, 1),
    (["aa/bb/only_model_cols.sql", "--is_test"], True, True, True, 1),
    (["aa/bb/without_catalog.sql", "--is_test"], True, True, True, 1),
    (["aa/bb/catalog_cols.sql", "--is_test"], True, True, False, 0),
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
def test_check_model_has_all_columns(
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
