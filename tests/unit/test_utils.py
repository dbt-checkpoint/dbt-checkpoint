import argparse
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from dbt_checkpoint.utils import (
    _discover_prop_files,
    _validate_checkpoint_config_version,
    CalledProcessError,
    CompilationException,
    GenericDbtObject,
    JsonOpenError,
    MacroSchema,
    Model,
    ModelSchema,
    ParseDict,
    ParseJson,
    Source,
    SourceSchema,
    add_default_args,
    add_related_sqls,
    add_related_ymls,
    check_dbt_schema_version,
    cmd_output,
    extend_dbt_project_dir_flag,
    get_config_file,
    get_dbt_catalog,
    get_dbt_manifest,
    get_disabled,
    get_ephemeral,
    get_exposures,
    get_filenames,
    get_flags,
    get_json,
    get_macro_schemas,
    get_macros,
    get_manifest_node_from_file_path,
    get_missing_file_paths,
    get_model_schemas,
    get_models,
    get_parent_childs,
    get_seeds,
    get_snapshot_filenames,
    get_snapshots,
    get_source_schemas,
    get_tests,
    obj_in_deps,
    paths_to_dbt_models,
    raise_invalid_property_yml_version,
    run_dbt_cmd,
    strings_differ_in_case,
    validate_meta_keys,
)


@pytest.fixture
def manifest_obj():
    """A mock manifest object for testing."""
    return {
        "nodes": {
            "snapshot.proj.my_snapshot_real": {
                "config": {"materialized": "snapshot"},
                "name": "my_snapshot_real",
                "resource_type": "snapshot",
            },
            "model.proj.ephemeral_model": {
                "config": {"materialized": "ephemeral"},
                "name": "ephemeral_model",
                "resource_type": "model",
            },
            "test.proj.my_test": {
                "config": {"materialized": "test"},
                "name": "my_test",
                "resource_type": "test",
            },
            "seed.proj.my_seed": {
                "name": "my_seed",
                "resource_type": "seed",
            },
            "model.proj.my_model.v1": {
                "name": "my_model",
                "resource_type": "model",
                "version": 1,
                "config": {"materialized": "table", "enabled": True},
            },
        },
        "disabled": {
            "model.proj.my_disabled_model": {},
        },
    }


def test_cmd_output_error():
    """Tests that cmd_output raises CalledProcessError on non-zero exit codes."""
    with pytest.raises(CalledProcessError):
        cmd_output(sys.executable, "-c", "import sys; sys.exit(1)")


def test_cmd_output_output():
    """Tests that cmd_output returns the command's stdout."""
    ret = cmd_output(sys.executable, "-c", "print('hi')")
    assert ret.strip() == "hi"


@pytest.mark.parametrize(
    "test_input,pre,post,expected",
    [
        (["/aa/bb/cc.txt", "ee"], "", "", ["cc", "ee"]),
        (["/aa/bb/cc.txt"], "+", "", ["+cc"]),
        (["/aa/bb/cc.txt"], "", "+", ["cc+"]),
        (["/aa/bb/cc.txt"], "+", "+", ["+cc+"]),
    ],
)
def test_paths_to_dbt_models(test_input, pre, post, expected):
    result = paths_to_dbt_models(test_input, pre, post)
    assert result == expected


def test_paths_to_dbt_models_default():
    result = paths_to_dbt_models(["/aa/bb/cc.txt", "ee"])
    assert result == ["cc", "ee"]


@pytest.mark.parametrize(
    "obj,child_name,result",
    [
        (Model("aa", "bb", "cc", {}), "aa", True),
        (SourceSchema("aa", "bb", "cc", {}, {}), "source.aa.bb", True),
        (ModelSchema("aa", "model.aa", {}, Path("model.aa")), "model.aa", True),
        (object, "model.aa", False),
    ],
)
def test_obj_in_child(obj, child_name, result):
    assert obj_in_deps(obj, child_name) == result


def test_get_filenames():
    result = get_filenames(["aa/bb/cc.sql", "bb/ee.sql"])
    assert result == {"cc": Path("aa/bb/cc.sql"), "ee": Path("bb/ee.sql")}


def test_get_filenames_empty_list():
    result = get_filenames([])
    assert result == {}


def test_get_json_error():
    """Tests that get_json raises JsonOpenError for a missing file."""
    with pytest.raises(JsonOpenError):
        get_json("non_existent_file.json")


def test_get_model_schemas(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

models:
    - name: aaa
      description: bbb
    """
    )
    result = get_model_schemas([Path(file)], {"aaa"})
    assert next(result).model_name == "aaa"


def test_get_model_schemas_empty_file(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write("")
    result = get_model_schemas([Path(file)], {"aa"})
    assert list(result) == []


def test_get_model_schemas_no_models_key(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2
sources:
    - name: my_source
      description: yyy
    """
    )
    result = get_model_schemas([Path(file)], {"aa"})
    assert list(result) == []


def test_get_model_schemas_malformed_yaml(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write("key: value: another_value")
    with pytest.raises(yaml.YAMLError):
        list(get_model_schemas([Path(file)], {"aa"}))


def test_get_macro_schemas_malformed_schema(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

macros:
    name: aaa
    description: bbb
    """
    )

    result = get_macro_schemas([Path(file)], {"aa"})
    assert list(result) == []


def test_get_macro_schemas_correct_schema(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

macros:
    - name: aaa
      description: bbb
    """
    )
    result = get_macro_schemas([Path(file)], {"aaa"})
    assert list(result) == [
        MacroSchema(
            macro_name="aaa",
            filename="schema",
            schema={"name": "aaa", "description": "bbb"},
            file=Path(file),
            prefix="macro",
        )
    ]


def test_validate_checkpoint_config_version_valid():
    yaml_dct = {"version": 1}
    _validate_checkpoint_config_version("file_path", yaml_dct)


def test_validate_checkpoint_config_version_no_version():
    yaml_dct = {"test": "test"}
    with pytest.raises(CompilationException):
        _validate_checkpoint_config_version("file_path", yaml_dct)


def test_validate_checkpoint_config_version_non_int_version():
    yaml_dct = {"version": "test"}
    with pytest.raises(CompilationException):
        _validate_checkpoint_config_version("file_path", yaml_dct)


def test_validate_checkpoint_config_version_wrong_version():
    yaml_dct = {"version": 2}
    with pytest.raises(CompilationException):
        _validate_checkpoint_config_version("file_path", yaml_dct)


def test_check_dbt_schema_version_valid():
    yaml_dct = {"version": 2}
    check_dbt_schema_version("file_path", yaml_dct)


def test_check_dbt_schema_version_missing():
    yaml_dct = {}
    with pytest.raises(CompilationException):
        check_dbt_schema_version("file_path", yaml_dct)


def test_check_dbt_schema_version_invalid():
    yaml_dct = {"version": 1}
    with pytest.raises(CompilationException):
        check_dbt_schema_version("file_path", yaml_dct)


def test_get_config_file_not_found():
    result = get_config_file("non_existent_file.yaml")
    assert result == {}


def test_get_config_file_suffix_swap(tmpdir):
    config_file = tmpdir.join("test_config.yaml")
    config_file.write("version: 1")
    path_with_wrong_suffix = tmpdir.join("test_config.yml")
    config = get_config_file(str(path_with_wrong_suffix))
    assert config == {"version": 1}


def test_get_source_schemas_disabled(tmpdir):
    """Tests that get_source_schemas respects the include_disabled flag."""
    schema_file = tmpdir.join("schema.yml")
    schema_file.write(
        """
version: 2
sources:
  - name: my_source
    tables:
      - name: enabled_table
      - name: disabled_table
        config:
          enabled: false
    """
    )
    schemas_default = list(get_source_schemas([Path(schema_file)]))
    assert len(schemas_default) == 1
    assert schemas_default[0].table_name == "enabled_table"
    schemas_include_disabled = list(
        get_source_schemas([Path(schema_file)], include_disabled=True)
    )
    assert len(schemas_include_disabled) == 2


# Input files, valid manifest, expected files
TESTS = (
    (
        ["aa/bb/with_description.sql"],
        "bb/with_description.yml",
        True,
        "",
        ["aa/bb/with_description.sql", "bb/with_description.yml"],
    ),
    (
        ["aa/bb/with_description.sql"],
        "",
        False,
        "",
        ["aa/bb/with_description.sql"],
    ),
    (
        ["aa/bb/with_description.sql"],
        "aa/bb/with_description.yml",
        True,
        r"^(.+)\/([^\/]+)$",
        [],
    ),
    (
        ["aa/bb/with_description.yml"],
        "bb/with_description.sql",
        True,
        "",
        ["aa/bb/with_description.yml", "bb/with_description.sql"],
    ),
)


@pytest.mark.parametrize(
    (
        "input_files",
        "discovered_files",
        "valid_manifest",
        "exclude_regex",
        "expected_files",
    ),
    TESTS,
)
def test_get_missing_file_paths(
    input_files,
    discovered_files,
    valid_manifest,
    exclude_regex,
    expected_files,
    manifest_path_str,
):
    manifest = {}
    if valid_manifest:
        import json

        manifest = json.load(open(manifest_path_str))
    with patch("dbt_checkpoint.utils._discover_prop_files") as mock_discover_prop_files:
        with patch(
            "dbt_checkpoint.utils._discover_sql_files"
        ) as mock_discover_sql_files:
            mock_discover_sql_files.return_value = [Path(discovered_files)]
            mock_discover_prop_files.return_value = [Path(discovered_files)]
            resulting_files = get_missing_file_paths(
                paths=input_files, manifest=manifest, exclude_pattern=exclude_regex
            )
    assert set(resulting_files) == set(expected_files)


def test_get_snapshot_filenames(manifest_obj):
    """Tests the get_snapshot_filenames function."""
    result = get_snapshot_filenames(manifest_obj)
    assert result == ["my_snapshot_real"]


def test_get_snapshots(manifest_obj):
    """Tests the get_snapshots function."""
    result = list(get_snapshots(manifest_obj, filenames={"my_snapshot_real"}))
    assert len(result) == 1
    assert result[0].name == "my_snapshot_real"


def test_get_tests(manifest_obj):
    """Tests the get_tests function."""
    result = list(get_tests(manifest_obj, filenames={"my_test"}))
    assert len(result) == 1
    assert result[0].name == "my_test"


def test_get_seeds(manifest_obj):
    """Tests the get_seeds function."""
    result = list(get_seeds(manifest_obj, filenames={"my_seed"}))
    assert len(result) == 1
    assert result[0].name == "my_seed"


def test_get_disabled(manifest_obj):
    """Tests the get_disabled function."""
    result = get_disabled(manifest_obj)
    assert result == ["my_disabled_model"]


def test_get_versioned_models(manifest_obj):
    """Tests that get_models correctly identifies versioned models."""
    result = list(get_models(manifest_obj, filenames={"my_model_v1"}))
    assert len(result) == 1
    assert result[0].model_id == "model.proj.my_model.v1"


@patch("dbt_checkpoint.utils.cmd_output")
def test_run_dbt_cmd_success(mock_cmd_output):
    """Tests that run_dbt_cmd returns 0 on success."""
    mock_cmd_output.return_value = "Success"
    result = run_dbt_cmd(["dbt", "run"])
    assert result == 0
    mock_cmd_output.assert_called_once()


@patch("dbt_checkpoint.utils.cmd_output")
def test_run_dbt_cmd_error(mock_cmd_output):
    """Tests that run_dbt_cmd returns 1 on failure."""
    mock_cmd_output.side_effect = CalledProcessError("dbt run", 1, 1, b"", b"")
    result = run_dbt_cmd(["dbt", "run"])
    assert result == 1
    mock_cmd_output.assert_called_once()


def test_extend_dbt_cmd_flags_with_project_dir():
    cmd = ["dbt", "run"]
    cmd_flags = ["--target", "dev"]
    dbt_project_dir = "/path/to/project"
    expected_result = ["dbt", "run", "--project-dir", "/path/to/project"]
    result = extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)
    assert result == expected_result


def test_extend_dbt_cmd_flags_without_project_dir():
    cmd = ["dbt", "run"]
    cmd_flags = ["--target", "dev"]
    dbt_project_dir = ""
    expected_result = ["dbt", "run"]
    result = extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)
    assert result == expected_result


def test_get_dbt_manifest_with_config_project_dir():
    class Args:
        manifest = "target/manifest.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {"dbt-project-dir": "test_dbt_project"}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            result = get_dbt_manifest(Args())
            assert result == {"key": "value"}


def test_get_dbt_manifest_without_config_project_dir():
    class Args:
        manifest = "target/manifest.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            result = get_dbt_manifest(Args())
            assert result == {"key": "value"}


def test_get_dbt_catalog_with_config_project_dir():
    class Args:
        catalog = "target/catalog.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {"dbt-project-dir": "test_dbt_project"}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            result = get_dbt_catalog(Args())
            assert result == {"key": "value"}


def test_get_dbt_catalog_without_config_project_dir():
    class Args:
        catalog = "target/catalog.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            result = get_dbt_catalog(Args())
            assert result == {"key": "value"}


def test_parse_dict_action():
    """Tests the ParseDict argparse action."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--meta", action=ParseDict, nargs="+")
    args = parser.parse_args(["--meta", "key1=value1", "key2=value2"])
    assert args.meta == {"key1": "value1", "key2": "value2"}


def test_parse_json_action():
    """Tests the ParseJson argparse action."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--vars", action=ParseJson)
    json_string = '{"key": "value", "nested": {"key2": "value2"}}'
    args = parser.parse_args(["--vars", json_string])
    assert args.vars == {"key": "value", "nested": {"key2": "value2"}}


def test_get_config_file_raises_compilation_error(tmpdir):
    """Tests that get_config_file raises an error for an invalid version."""
    config_file = tmpdir.join("test_config.yaml")
    config_file.write("version: 2")
    with pytest.raises(CompilationException):
        get_config_file(str(config_file))


def test_get_versioned_models_by_name(manifest_obj):
    """Tests that get_models finds versioned models by their name without the version suffix."""
    result = list(get_models(manifest_obj, filenames={"my_model"}))
    assert len(result) == 1
    assert result[0].model_id == "model.proj.my_model.v1"


def test_get_dbt_manifest_custom_path():
    """Tests the get_dbt_manifest function with a custom path."""
    class Args:
        manifest = "custom/path/manifest.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            result = get_dbt_manifest(Args())
            mock_get_json.assert_called_once_with("custom/path/manifest.json")
            assert result == {"key": "value"}


def test_raise_invalid_property_yml_version():
    """Tests that the exception raising helper works correctly."""
    with pytest.raises(CompilationException):
        raise_invalid_property_yml_version("file/path.yml", "is invalid")


def test_get_exposures(tmpdir):
    """Tests that get_exposures finds entries in a schema file."""
    schema_file = tmpdir.join("schema.yml")
    schema_file.write(
        """
version: 2
exposures:
  - name: my_exposure
    description: A test exposure.
    """
    )
    exposures = list(get_exposures([Path(schema_file)]))
    assert len(exposures) == 1
    assert exposures[0].name == "my_exposure"


def test_get_parent_childs_source():
    """Tests get_parent_childs for a source dependency."""
    manifest = {
        "sources": {
            "source.proj.raw.users": {
                "source_name": "raw",
                "name": "users",
                "path": "/path/to/schema.yml",
            }
        },
        "nodes": {
            "model.proj.stg_users": {
                "name": "stg_users",
                "path": "stg_users.sql",
            }
        },
        "child_map": {
            "source.proj.raw.users": ["model.proj.stg_users"],
        },
    }
    source_obj = SourceSchema(
        source_name="raw",
        table_name="users",
        filename="schema",
        source_schema={},
        table_schema={},
    )
    children = list(
        get_parent_childs(
            manifest, obj=source_obj, manifest_node="child_map", node_types=["model"]
        )
    )
    assert len(children) == 1
    assert isinstance(children[0], Model)
    assert children[0].model_name == "stg_users"


@pytest.mark.parametrize(
    ("meta", "required_keys", "allow_extra", "expected_code"),
    [
        ({"key1": "v1", "key2": "v2"}, ["key1", "key2"], False, 0),
        ({"key1": "v1"}, ["key1", "key2"], False, 1),
        ({"key1": "v1", "key2": "v2", "extra": "v3"}, ["key1", "key2"], False, 1),
        ({"key1": "v1", "key2": "v2", "extra": "v3"}, ["key1", "key2"], True, 0),
        ({"key1": "v1", "extra": "v3"}, ["key1", "key2"], True, 1),
    ],
)
def test_validate_meta_keys(meta, required_keys, allow_extra, expected_code):
    """Tests all branches of the validate_meta_keys function."""
    obj = GenericDbtObject(
        name="test_object", filename="test.yml", schema={"meta": meta}
    )
    result = validate_meta_keys(obj, required_keys, set(required_keys), allow_extra)
    assert result == expected_code


def test_add_related_ymls_ignores_target_path():
    """Tests that add_related_ymls ignores paths in the target directory."""
    nodes = {
        "model.proj.my_model": {
            "path": "models/my_model.sql",
            "patch_path": "proj/models/schema.yml",
        }
    }
    paths_with_missing = set()
    with patch("dbt_checkpoint.utils._discover_prop_files") as mock_discover:
        mock_discover.return_value = [Path("target/models/schema.yml")]
        add_related_ymls("models/my_model.sql", nodes, paths_with_missing)
    assert len(paths_with_missing) == 0


def test_get_dbt_catalog_config_path():
    """Tests get_dbt_catalog when using the config-provided path."""
    class Args:
        catalog = "target/catalog.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config:
        mock_get_config.return_value = {"dbt-project-dir": "my_dbt_project"}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            get_dbt_catalog(Args())
            mock_get_json.assert_called_once_with("my_dbt_project/target/catalog.json")


def test_get_missing_file_paths_other_extension():
    """Tests that get_missing_file_paths handles non-sql/yml files."""
    manifest = {"nodes": {}}
    paths = ["path/to/some_file.txt"]
    result = get_missing_file_paths(paths, manifest)
    assert result == paths


def test_add_related_sqls_ignores_target_path():
    """Tests that add_related_sqls ignores paths in the target directory."""
    nodes = {
        "model.proj.my_model": {
            "patch_path": "proj/models/my_model.yml",
            "original_file_path": "models/my_model.sql",
        }
    }
    paths_with_missing = set()
    with patch("dbt_checkpoint.utils._discover_sql_files") as mock_discover:
        mock_discover.return_value = [Path("target/models/my_model.sql")]
        add_related_sqls("models/my_model.yml", nodes, paths_with_missing)
    assert len(paths_with_missing) == 0


def test_get_macros(manifest_obj):
    """Tests the get_macros function."""
    manifest_obj["macros"] = {
        "macro.proj.my_macro": {"name": "my_macro", "path": "/path/to/macros", "original_file_path": "my_macro.sql"}
    }
    result = list(get_macros(manifest_obj, filenames={"my_macro"}))
    assert len(result) == 1
    assert result[0].macro_name == "my_macro"


def test_get_flags_no_flags():
    """Tests that get_flags returns an empty list for None input."""
    assert get_flags(None) == []


def test_get_dbt_manifest_default_path():
    """Tests the get_dbt_manifest function's default path."""
    class Args:
        manifest = "target/manifest.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file", return_value={}):
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            get_dbt_manifest(Args())
            mock_get_json.assert_called_once_with("target/manifest.json")


def test_get_missing_file_paths_limited_extensions():
    """Tests get_missing_file_paths when yml files are not in scope."""
    manifest = {"nodes": {"model.proj.my_model": {"patch_path": "proj/models/schema.yml"}}}
    paths = ["models/my_model.sql"]
    result = get_missing_file_paths(paths, manifest, extensions=[".sql"])
    assert result == ["models/my_model.sql"]


def test_add_related_sqls_adds_path():
    """Tests that add_related_sqls adds a valid path."""
    nodes = {
        "model.proj.my_model": {
            "patch_path": "proj/models/my_model.yml",
            "original_file_path": "models/my_model.sql",
        }
    }
    paths_with_missing = set()
    with patch("dbt_checkpoint.utils._discover_sql_files") as mock_discover:
        mock_discover.return_value = [Path("models/my_model.sql")]
        add_related_sqls("models/my_model.yml", nodes, paths_with_missing)
    assert "models/my_model.sql" in paths_with_missing


def test_get_parent_childs_test():
    """Tests get_parent_childs for a test dependency."""
    manifest = {
        "nodes": {
            "model.proj.stg_users": {"name": "stg_users"},
            "test.proj.not_null_stg_users_id.123": {
                "tags": ["schema"],
                "test_metadata": {"name": "not_null"},
            },
        },
        "child_map": {
            "model.proj.stg_users": ["test.proj.not_null_stg_users_id.123"],
        },
    }
    model_obj = ModelSchema(
        model_name="stg_users", filename="schema", schema={}, file=Path("schema.yml")
    )
    children = list(
        get_parent_childs(
            manifest, obj=model_obj, manifest_node="child_map", node_types=["test"]
        )
    )
    assert len(children) == 1
    assert children[0].test_name == "not_null"


def test_get_ephemeral(manifest_obj):
    """Tests the get_ephemeral function."""
    result = get_ephemeral(manifest_obj)
    assert result == ["ephemeral_model"]


def test_get_parent_childs_data_test():
    """Tests get_parent_childs for a data test dependency."""
    manifest = {
        "nodes": {
            "model.proj.stg_users": {"name": "stg_users"},
            "test.proj.unique_stg_users_id.abc": {
                "tags": ["data"],
                "test_metadata": {"name": "unique"},
            },
        },
        "child_map": {
            "model.proj.stg_users": ["test.proj.unique_stg_users_id.abc"],
        },
    }
    model_obj = ModelSchema(
        model_name="stg_users", filename="schema", schema={}, file=Path("schema.yml")
    )
    children = list(
        get_parent_childs(
            manifest, obj=model_obj, manifest_node="child_map", node_types=["test"]
        )
    )
    assert len(children) == 1
    assert children[0].test_type == "data"


def test_get_flags_with_flags():
    """Tests that get_flags correctly processes a list of flags."""
    result = get_flags(["+flag1", "+flag2"])
    assert result == ["-flag1", "-flag2"]


def test_parse_json_action_no_value():
    """Tests the ParseJson argparse action with no value provided."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--vars", action=ParseJson, default={})
    args = parser.parse_args([])
    assert args.vars == {}

def test_get_config_file_suffix_swap_reverse(tmpdir):
    """Tests the yml -> yaml suffix swap."""
    config_file = tmpdir.join("test_config.yml")
    config_file.write("version: 1")
    path_with_wrong_suffix = tmpdir.join("test_config.yaml")
    config = get_config_file(str(path_with_wrong_suffix))
    assert config == {"version": 1}

def test_get_models_non_versioned(manifest_obj):
    """Tests that get_models correctly identifies non-versioned models."""
    manifest_obj["nodes"]["model.proj.my_simple_model"] = {
        "name": "my_simple_model",
        "resource_type": "model",
        "config": {"materialized": "table", "enabled": True},
    }
    result = list(get_models(manifest_obj, filenames={"my_simple_model"}))
    assert len(result) == 1
    assert result[0].model_id == "model.proj.my_simple_model"


def test_get_macro_schemas_all_schemas(tmpdir):
    """Tests get_macro_schemas with the all_schemas flag."""
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2
macros:
    - name: my_macro
      description: A test macro
    """
    )
    # Note: 'my_macro' is not in the filenames set, but should be found anyway
    result = get_macro_schemas([Path(file)], {"other_macro"}, all_schemas=True)
    macros = list(result)
    assert len(macros) == 1
    assert macros[0].macro_name == "my_macro"


def test_get_parent_childs_yields_source():
    """Tests get_parent_childs can yield a Source object."""
    manifest = {
        "nodes": {
            "model.proj.stg_users": {
                "name": "stg_users",
                "path": "stg_users.sql",
            }
        },
        "sources": {
            "source.proj.raw.users": {
                "source_name": "raw",
                "name": "users",
                "path": "/path/to/schema.yml",
            }
        },
        "parent_map": {
             "model.proj.stg_users": ["source.proj.raw.users"],
        },
    }
    model_obj = Model(
        model_id="model.proj.stg_users",
        model_name="stg_users",
        filename="stg_users.sql",
        node=manifest["nodes"]["model.proj.stg_users"]
    )
    parents = list(
        get_parent_childs(
            manifest, obj=model_obj, manifest_node="parent_map", node_types=["source"]
        )
    )
    assert len(parents) == 1
    assert isinstance(parents[0], Source)
    assert parents[0].source_id == "source.proj.raw.users"


def test_get_manifest_node_from_file_path_no_match(manifest_obj):
    """Tests get_manifest_node_from_file_path for a non-existent path."""
    # Isolate the test by clearing other nodes from the fixture
    manifest_obj["nodes"] = {}
    manifest_obj["nodes"]["model.proj.my_model"] = {
        "original_file_path": "models/actual_model.sql",
        "resource_type": "model",
    }

    result = get_manifest_node_from_file_path(manifest_obj, "models/non_existent.sql")
    assert result == {}


def test_discover_prop_files_integration(tmpdir):
    """Tests the _discover_prop_files function directly."""
    p = tmpdir.mkdir("models").join("schema.yml")
    p.write("version: 2")
    with tmpdir.as_cwd():
        discovered_files = list(_discover_prop_files("models/schema.yml"))
        assert len(discovered_files) == 1
        assert discovered_files[0] == Path("models/schema.yml")


def test_add_default_args():
    """Tests that add_default_args adds the expected arguments."""
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    # Test that a few key arguments are present
    parsed = parser.parse_args(["--manifest", "custom/manifest.json", "somefile.sql"])
    assert parsed.manifest == "custom/manifest.json"
    assert parsed.filenames == ["somefile.sql"]


@pytest.mark.parametrize(
    ("str1", "str2", "expected"),
    [
        ("a", "A", True),
        ("abc", "aBc", True),
        ("a", "a", False),
        ("a", "b", False),
        ("A", "B", False),
        ("", "", False),
    ],
)
def test_strings_differ_in_case(str1, str2, expected):
    """Tests the strings_differ_in_case helper function."""
    assert strings_differ_in_case(str1, str2) == expected