from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_columns_have_meta_keys import check_column_has_meta_keys
from dbt_checkpoint.check_model_columns_have_meta_keys import main

# Input args, valid manifest, expected return value
TESTS = (
    (
        [
            "aa/bb/with_no_columns.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {"models": [{"name": "with_no_columns"}]},
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_column_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {
            "models": [
                {
                    "name": "with_meta",
                    "description": "test meta",
                    "columns": [
                        {
                            "name": "column_1",
                            "description": "description_1",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                        {
                            "name": "column_2",
                            "description": "description_2",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                    ],
                }
            ]
        },
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_column_meta_and_extra.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {
            "models": [
                {
                    "name": "with_meta",
                    "description": "test meta",
                    "columns": [
                        {
                            "name": "column_1",
                            "description": "description_1",
                            "meta": {"foo": "foo", "bar": "bar", "baz": "baz"},
                        },
                        {
                            "name": "column_2",
                            "description": "description_2",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                    ],
                }
            ]
        },
        True,
        True,
        1,
    ),
    (
        [
            "aa/bb/with_column_meta_and_extra.sql",
            "--meta-keys",
            "foo",
            "bar",
            "--allow-extra-keys",
        ],
        {
            "models": [
                {
                    "name": "with_meta",
                    "description": "test meta",
                    "columns": [
                        {
                            "name": "column_1",
                            "description": "description_1",
                            "meta": {"foo": "foo", "bar": "bar", "baz": "baz"},
                        },
                        {
                            "name": "column_2",
                            "description": "description_2",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                    ],
                }
            ]
        },
        True,
        True,
        0,
    ),
    (
        [
            "aa/bb/with_column_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {
            "models": [
                {
                    "name": "with_meta",
                    "description": "test meta",
                    "columns": [
                        {
                            "name": "column_1",
                            "description": "description_1",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                        {
                            "name": "column_2",
                            "description": "description_2",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
                    ],
                }
            ]
        },
        False,
        True,
        1,
    ),
    (
        [
            "aa/bb/without_columns_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {
            "models": [
                {
                    "name": "with_meta",
                    "description": "test meta",
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
        [
            "aa/bb/with_some_column_meta.sql",
            "--meta-keys",
            "foo",
            "bar",
        ],
        {
            "models": [
                {
                    "name": "with_some_meta",
                    "description": "test meta",
                    "columns": [
                        {
                            "name": "column_1",
                            "description": "description_1",
                            "meta": {"foo": "foo", "bar": "bar"},
                        },
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
def test_check_columns_have_meta_keys(
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
def test_check_model_columns_meta_keys_in_schema(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_column_meta
    columns:
    -   name: test
        meta:
            foo: foo
            bar: bar
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_column_meta.sql",
            str(yml_file),
            "--meta-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_columns_meta_keys_and_extra_keys_in_schema(
    extension, tmpdir, manifest_path_str
):
    schema_yml = """
version: 2

models:
-   name: in_schema_column_meta
    columns:
    -   name: test
        meta:
            foo: foo
            bar: bar
            baz: baz
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_column_meta.sql",
            str(yml_file),
            "--meta-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_column_meta_keys(extension, tmpdir, manifest):
    schema_yml = """
version: 2

models:
-   name: in_schema_some_column_meta
    columns:
    -   name: test1
        meta:
            foo: foo
    -   name: test2
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    res_stat, missing = check_column_has_meta_keys(
        ["in_schema_some_column_meta.sql", str(yml_file)],
        manifest,
        ["foo", "bar"],
        False,
    )
    assert res_stat == 1
    assert missing == {"in_schema_some_column_meta": ["test1", "test2"]}


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_columns_have_meta_keys_both(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_some_column_meta
    columns:
    -   name: test4
        meta:
            foo: foo
            bar: bar
-   name: in_schema_without_columns_meta
    columns:
    -   name: test
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_some_column_meta.sql",
            "in_schema_without_columns_meta.sql",
            str(yml_file),
            "--meta-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_columns_have_meta_keys_without(
    extension, tmpdir, manifest_path_str
):
    schema_yml = """
version: 2

models:
-   name: in_schema_without_columns_meta
    columns:
    -   name: test1
    -   name: test2
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_without_columns_meta.sql",
            str(yml_file),
            "--meta-keys",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 1
