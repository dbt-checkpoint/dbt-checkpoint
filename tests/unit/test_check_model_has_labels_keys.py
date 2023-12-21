from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_has_labels_keys import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        [
            "aa/bb/with_labels.sql",
            "--labels-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "with_labels", "config": {"labels": {"foo": "bar"}}}]},
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_labels.sql",
            "--labels-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "with_labels", "config": {"labels": {"foo": "bar"}}}]},
        False,
        True,
        1,
    ),
    (
        [
            "aa/bb/without_labels.sql",
            "--labels-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "without_labels"}]},
        True,
        True,
        1,
    ),
    (
        ["aa/bb/with_labels.sql", "--labels-keys", "foo", "--allow-extra-keys"],
        {"models": [{"name": "with_labels", "config":{"labels": {"foo": "bar", "baz": "test"}}}]},
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_labels.sql", "--labels-keys", "baz"],
        {"models": [{"name": "with_labels", "config":{"labels": {"foo": "bar", "baz": "test"}}}]},
        True,
        True,
        1,
    ),
)


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "valid_config", "expected_status_code"),
    TESTS,
)
def test_check_model_labels_keys(
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
def test_check_model_labels_keys_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_labels
    config:
        labels:
            foo: test
            bar: test
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_labels.sql",
            str(yml_file),
            "--labels-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
