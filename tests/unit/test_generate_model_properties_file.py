from unittest.mock import patch

import pytest
import json
import yaml
from dbt_checkpoint.utils import get_models

from dbt_checkpoint.generate_model_properties_file import main, get_model_properties

TESTS = (
    (
        ["aa/bb/catalog_cols.sql"],
        {"name": "catalog_cols", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
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
        True,
        1,
        None,
    ),
    (
        ["aa/bb/catalog_cols.sql"],
        {"name": "catalog_cols", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        False,
        True,
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
        True,
        1,
        "- name: without_catalog\n",
    ),
    (
        ["aa/bb/with_schema.sql"],
        {"name": "with_schema", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        True,
        True,
        0,
        None,
    ),
    (
        ["aa/bb/with_schema.sql"],
        {"name": "with_schema", "columns": [{"name": "col1"}, {"name": "col2"}]},
        True,
        True,
        False,
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
        "valid_config",
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
    valid_config,
    expected_status_code,
    expected,
    manifest_path_str,
    catalog_path_str,
    config_path_str,
    tmpdir_factory,
):
    properties_file = tmpdir_factory.mktemp("data").join("schema.yml")
    input_args.extend(["--properties-file", str(properties_file)])
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    if valid_config:
        input_args.extend(["--config", config_path_str])

    if expected_status_code == 1:
        with patch(
            "dbt_checkpoint.generate_model_properties_file.get_model_properties"
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
        "valid_config",
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
    valid_config,
    expected_status_code,
    expected,
    manifest_path_str,
    catalog_path_str,
    config_path_str,
    tmpdir,
):
    properties_file = tmpdir.join("/schema.yml")
    properties_file.write(schema)
    input_args.extend(["--properties-file", str(properties_file), "--is_test"])
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    if valid_config:
        input_args.extend(["--config", config_path_str])
    if expected_status_code == 1:
        with patch(
            "dbt_checkpoint.generate_model_properties_file.get_model_properties"
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
    input_args = ["aa/bb/with_schema.sql", "--is_test"]
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
    input_args = ["aa/bb/catalog_cols.sql", "--is_test"]
    input_args.extend(["--properties-file", str(properties_file)])
    input_args.extend(["--manifest", manifest_path_str])
    input_args.extend(["--catalog", catalog_path_str])
    main(input_args)
    ff = tmpdir.join(directory)
    print(ff)

def test_get_model_properties_logic(capsys):
    """Tests the get_model_properties function directly."""
    from dbt_checkpoint.utils import Model
    
    node1 = {
        "unique_id": "model.test.model_one", "path": "model_one.sql",
        "name": "model_one", "patch_path": None, "resource_type": "model",
        "package_name": "test", "columns": {}, "config": {},
        "fqn": ["test", "model_one"],
    }
    node2 = {
        "unique_id": "model.test.model_two", "path": "model_two.sql",
        "name": "model_two", "patch_path": None, "resource_type": "model",
        "package_name": "test", "columns": {}, "config": {},
        "fqn": ["test", "model_two"],
    }
    catalog = {
        "nodes": {
            "model.test.model_one": {"columns": {"col_a": {}, "col_b": {}}}
        }
    }

    # Manually create Model objects with the correct arguments
    model_one_obj = Model(
        model_id="model.test.model_one",
        model_name="model_one",
        filename="model_one.sql",
        node=node1,
    )
    model_two_obj = Model(
        model_id="model.test.model_two",
        model_name="model_two",
        filename="model_two.sql",
        node=node2,
    )

    # SCENARIO 1: Model is found in the catalog
    props1 = get_model_properties(model_one_obj, catalog["nodes"])
    assert props1 == {"name": "model_one", "columns": [{"name": "col_a"}, {"name": "col_b"}]}
    
    # SCENARIO 2: Model is NOT found in the catalog
    props2 = get_model_properties(model_two_obj, catalog["nodes"])
    assert props2 == {"name": "model_two"}
    
    captured = capsys.readouterr()
    assert "Unable to find model `model.test.model_two` in catalog file" in captured.out

def test_generate_properties_file_include_disabled(tmpdir, catalog_path_str, config_path_str):
    """
    Tests that the --include-disabled flag correctly processes disabled models.
    """
    # 1. Setup a manifest with a disabled model
    manifest_content = {
        "nodes": {
            "model.test.disabled_model": {
                "unique_id": "model.test.disabled_model",
                "resource_type": "model", "path": "disabled_model.sql",
                "name": "disabled_model", "patch_path": None,
                "config": {"enabled": False},
            }
        }, "macros": {}
    }
    manifest_path = tmpdir.join("manifest.json")
    manifest_path.write_text(json.dumps(manifest_content), "utf-8")
    
    properties_file = tmpdir.join("schema.yml")
    sql_file = tmpdir.join("disabled_model.sql")
    sql_file.write_text("select 1", "utf-8")
    
    # 2. Run WITHOUT the flag (should do nothing, status 0)
    argv1 = [
        str(sql_file), "--is_test",
        "--manifest", str(manifest_path), "--catalog", catalog_path_str,
        "--config", config_path_str, "--properties-file", str(properties_file)
    ]
    status_code1 = main(argv1)
    assert status_code1 == 0
    assert not properties_file.exists()

    # 3. Run WITH the flag (should create the file, status 1)
    argv2 = argv1 + ["--include-disabled"]
    status_code2 = main(argv2)
    assert status_code2 == 1
    assert properties_file.exists()