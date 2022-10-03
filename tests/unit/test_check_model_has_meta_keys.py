from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from pre_commit_dbt.check_model_has_meta_keys import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "with_meta", "meta": {"foo": "bar"}}]},
        True,
        0,
    ),
    (
        [
            "aa/bb/with_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "with_meta", "meta": {"foo": "bar"}}]},
        False,
        1,
    ),
    (
        [
            "aa/bb/without_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "without_meta"}]},
        True,
        1,
    ),
)


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_meta_keys(
    input_args, schema, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    with patch("builtins.open", mock_open(read_data="data")):
        with patch("pre_commit_dbt.utils.yaml.safe_load") as mock_safe_load:
            mock_safe_load.return_value = schema
            status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_meta_keys_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_meta
    meta:
        foo: test
        bar: test
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_meta.sql",
            str(yml_file),
            "--meta-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
