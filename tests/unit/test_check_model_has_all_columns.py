import pytest
import json

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


def test_check_model_has_all_columns_exclude(
    manifest_path_str,
    catalog_path_str,
    config_path_str,
):
    """
    Tests that a file that would normally fail is successfully
    ignored when using the --exclude flag.
    """
    # This input would normally fail with status_code 1
    argv = [
        "aa/bb/only_model_cols.sql",
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--catalog",
        catalog_path_str,
        "--config",
        config_path_str,
        "--exclude",
        "only_model_cols.sql", # Exclude the failing file
    ]
    status_code = main(argv)
    assert status_code == 0


def test_check_model_has_all_columns_include_disabled(tmpdir, config_path_str):
    """
    Tests that disabled models are ignored by default but
    are checked when --include-disabled is passed.
    """
    # 1. Setup a manifest with a disabled model that has column mismatches
    manifest_content = {
        "nodes": {
            "model.test.disabled_model": {
                "unique_id": "model.test.disabled_model",
                "resource_type": "model",
                "path": "disabled_model.sql",
                "name": "disabled_model",
                "columns": {"in_model_only": {}},
                "config": {"enabled": False},
            }
        },
        "macros": {}
    }
    manifest_path = tmpdir.join("manifest.json")
    manifest_path.write_text(json.dumps(manifest_content), "utf-8")

    # 2. Setup a catalog for the disabled model
    catalog_content = {
        "nodes": {
            "model.test.disabled_model": {
                "metadata": {},
                "columns": {"in_catalog_only": {}},
            }
        }
    }
    catalog_path = tmpdir.join("catalog.json")
    catalog_path.write_text(json.dumps(catalog_content), "utf-8")

    sql_path = tmpdir.join("disabled_model.sql")
    sql_path.write_text("select 1", "utf-8")

    # 3. Run WITHOUT the flag (should pass, as it ignores disabled models)
    argv1 = [
        str(sql_path),
        "--is_test",
        "--manifest", str(manifest_path),
        "--catalog", str(catalog_path),
        "--config", config_path_str,
    ]
    status_code1 = main(argv1)
    assert status_code1 == 0

    # 4. Run WITH the flag (should fail)
    argv2 = argv1 + ["--include-disabled"]
    status_code2 = main(argv2)
    assert status_code2 == 1