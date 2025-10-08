import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dbt_checkpoint.utils import (
    # ADD these missing imports
    check_dbt_schema_version,
    get_config_file,
    # Keep existing imports
    _validate_checkpoint_config_version,
    CalledProcessError,
    CompilationException,
    MacroSchema,
    Model,
    ModelSchema,
    SourceSchema,
    cmd_output,
    extend_dbt_project_dir_flag,
    get_dbt_catalog,
    get_dbt_manifest,
    get_filenames,
    get_macro_schemas,
    get_missing_file_paths,
    get_model_schemas,
    obj_in_deps,
    paths_to_dbt_models,
)


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