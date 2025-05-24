from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_columns_have_desc import check_column_desc
from dbt_checkpoint.check_model_columns_have_desc import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        ["aa/bb/with_column_description.sql"],
        {
            "models": [
                {
                    "name": "with_description",
                    "description": "test description",
                    "columns": [
                        {"name": "column_1", "description": "description_1"},
                        {"name": "column_2", "description": "description_2"},
                    ],
                }
            ]
        },
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_column_description.sql"],
        {
            "models": [
                {
                    "name": "with_description",
                    "description": "test description",
                    "columns": [
                        {"name": "column_1", "description": "description_1"},
                        {"name": "column_2", "description": "description_2"},
                    ],
                }
            ]
        },
        False,
        True,
        1,
    ),
    (
        ["aa/bb/without_columns_description.sql"],
        {
            "models": [
                {
                    "name": "with_description",
                    "description": "test description",
                    "columns": [
                        {"name": "column_1"},
                        {"name": "column_2"},
                    ],
                }
            ]
        },
        True,
        True,
        1,
    ),
    (
        ["aa/bb/with_some_column_description.sql"],
        {
            "models": [
                {
                    "name": "with_description",
                    "description": "test description",
                    "columns": [
                        {"name": "column_1", "description": "description_1"},
                        {"name": "column_2"},
                    ],
                }
            ]
        },
        True,
        True,
        1,
    ),
)


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "valid_config", "expected_status_code"),
    TESTS,
)
def test_check_model_columns_have_desc(
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
def test_check_model_columns_have_desc_in_schema(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_column_description
    columns:
    -   name: test
        description: aaa
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_column_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_column_desc(extension, tmpdir, manifest):
    schema_yml = """
version: 2

models:
-   name: with_some_column_description
    columns:
    -   name: test1
        description: aaa
    -   name: test2
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    res_stat, missing = check_column_desc(
        ["with_some_column_description.sql", str(yml_file)], manifest
    )
    assert res_stat == 1
    assert missing == {"with_some_column_description": {"test2"}}


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_columns_have_desc_both(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: with_some_column_description
    columns:
    -   name: test4
-   name: without_columns_description
    columns:
    -   name: test
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "with_some_column_description.sql",
            "without_columns_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_columns_have_desc_without(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: without_columns_description
    columns:
    -   name: test1
        description: aa
    -   name: test2
        description: bb
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "without_columns_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
