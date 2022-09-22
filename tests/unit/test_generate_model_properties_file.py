from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from pre_commit_dbt.generate_model_properties_file import main

TESTS = (
    (
        ["aa/bb/catalog_cols.sql"],
        {"name": "catalog_cols", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        True,
        1,
        "- name: catalog_cols\n  columns:\n  - name: col1\n  - name: col2\n",
    ),
    (
        ["aa/bb/catalog_cols.sql"],
        {"name": "catalog_cols", "columns": [{"name": "col1"}, {"name": "col2"}]},
        False,
        True,
        1,
        None,
    ),
    (
        ["aa/bb/catalog_cols.sql"],
        {"name": "catalog_cols", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        False,
        1,
        None,
    ),
    (
        ["aa/bb/without_catalog.sql"],
        {
            "name": "without_catalog",
        },
        True,
        True,
        1,
        "- name: without_catalog\n",
    ),
    (
        ["aa/bb/with_schema.sql"],
        {"name": "with_schema", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        True,
        0,
        None,
    ),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "model_prop",
        "valid_manifest",
        "valid_catalog",
        "expected_status_code",
        "expected",
    ),
    TESTS,
)
def test_generate_model_properties_file(
    input_args,
    model_prop,
    valid_manifest,
    valid_catalog,
    expected_status_code,
    expected,
    manifest_path_str,
    catalog_path_str,
    tmpdir_factory,
):
    properties_file = tmpdir_factory.mktemp("data").join("schema.yml")
    input_args.extend(["--properties-file", str(properties_file)])
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if expected_status_code == 1:
        with patch(
            "pre_commit_dbt.generate_model_properties_file.get_model_properties"
        ) as mock_get_model_properties:
            mock_get_model_properties.return_value = model_prop
            status_code = main(input_args)
    else:
        status_code = main(input_args)

    assert status_code == expected_status_code
    if expected:
        prefix = "version: 2\nmodels:\n"
        result_schema = properties_file.read_text(encoding="utf-8")
        assert prefix + expected == result_schema


@pytest.mark.parametrize(
    "schema",
    [
        (
            """version: 2
models:
- name: aa
"""
        ),
        ("""version: 2"""),
    ],
)
@pytest.mark.parametrize(
    (
        "input_args",
        "model_prop",
        "valid_manifest",
        "valid_catalog",
        "expected_status_code",
        "expected",
    ),
    TESTS,
)
def test_generate_model_properties_file_existing_schema(
    schema,
    input_args,
    model_prop,
    valid_manifest,
    valid_catalog,
    expected_status_code,
    expected,
    manifest_path_str,
    catalog_path_str,
    tmpdir,
):
    properties_file = tmpdir.join("/schema.yml")
    properties_file.write(schema)
    input_args.extend(["--properties-file", str(properties_file)])
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    if expected_status_code == 1:
        with patch(
            "pre_commit_dbt.generate_model_properties_file.get_model_properties"
        ) as mock_get_model_properties:
            mock_get_model_properties.return_value = model_prop
            status_code = main(input_args)
    else:
        status_code = main(input_args)
    assert status_code == expected_status_code
    if expected:
        schema = "version: 2\nmodels:\n" if schema == """version: 2""" else schema
        result_schema = properties_file.read_text(encoding="utf-8")
        assert schema + expected == result_schema


@pytest.mark.parametrize(
    ("extension", "expected"), [("yml", 0), ("yaml", 0), ("xxx", 1)]
)
def test_generate_model_properties_file_bad_properties_file(
    extension, expected, manifest_path_str, catalog_path_str, tmpdir
):
    properties_file = tmpdir.join(f"schema.{extension}")
    properties_file.write(
        """version: 2
models:
- name: aa
"""
    )
    input_args = ["aa/bb/with_schema.sql"]
    input_args.extend(["--properties-file", str(properties_file)])
    input_args.extend(["--manifest", manifest_path_str])
    input_args.extend(["--catalog", catalog_path_str])
    status_code = main(input_args)
    assert status_code == expected


def test_generate_model_properties_file_path_template(
    manifest_path_str, catalog_path_str, tmpdir
):
    directory_tmp = "{database}/{schema}/{alias}/{name}.yml"
    directory = "test/test/test/catalog_cols.yml"
    properties_file = tmpdir.join(directory_tmp)
    input_args = ["aa/bb/catalog_cols.sql"]
    input_args.extend(["--properties-file", str(properties_file)])
    input_args.extend(["--manifest", manifest_path_str])
    input_args.extend(["--catalog", catalog_path_str])
    main(input_args)
    ff = tmpdir.join(directory)
    print(ff)
