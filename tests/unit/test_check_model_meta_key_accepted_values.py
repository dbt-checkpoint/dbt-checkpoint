from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_meta_key_accepted_values import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
        ],
        {"models": [{"name": "with_meta", "meta": {"domain": "sales"}}]},
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
        ],
        {"models": [{"name": "with_meta", "meta": {"domain": "invalid"}}]},
        True,
        True,
        1,
    ),
    (
        [
            "aa/bb/without_meta.sql",
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
        ],
        {"models": [{"name": "without_meta"}]},
        True,
        True,
        1,
    ),
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
        ],
        {"models": [{"name": "with_meta", "meta": {"domain": "sales", "other": "value"}}]},
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
        ],
        {"models": [{"name": "with_meta", "meta": {"domain": "hr"}}]},
        True,
        True,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "valid_config", "expected_status_code"),
    TESTS,
)
def test_check_model_meta_key_accepted_values(
    input_args,
    schema,
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
    with patch("builtins.open", mock_open(read_data="data")):
        with patch("dbt_checkpoint.utils.safe_load") as mock_safe_load:
            mock_safe_load.return_value = schema
    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_meta_key_accepted_values_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_meta
    meta:
        domain: sales
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_meta.sql",
            str(yml_file),
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_meta_key_accepted_values_invalid_value(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_meta
    meta:
        domain: invalid_domain
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_meta.sql",
            str(yml_file),
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_meta_key_accepted_values_missing_key(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_meta
    meta:
        other_key: value
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_meta.sql",
            str(yml_file),
            "--meta-key",
            "domain",
            "--accepted-values",
            "sales",
            "finance",
            "hr",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1

