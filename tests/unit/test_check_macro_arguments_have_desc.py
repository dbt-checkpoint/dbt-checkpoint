from pathlib import Path

import pytest
import yaml

from dbt_checkpoint.check_macro_arguments_have_desc import check_argument_desc
from dbt_checkpoint.check_macro_arguments_have_desc import main


PROJECT_DIRECTORY_PATH = Path("path/to/dbt/project")


@pytest.mark.parametrize(
    argnames=(
        "input_args",
        "valid_manifest",
        "valid_config",
        "config_has_dbt_project_dir",
        "expected_status_code",
    ),
    argvalues=(
        (
            ["macros/aa/with_argument_description.sql", "--is_test"],
            True,
            True,
            False,
            0,
        ),
        (
            ["macros/aa/with_argument_description.sql", "--is_test"],
            False,
            True,
            False,
            1,
        ),
        (
            ["macros/aa/without_arguments_description.sql", "--is_test"],
            True,
            True,
            False,
            1,
        ),
        (
            ["macros/aa/with_some_argument_description.sql", "--is_test"],
            True,
            True,
            False,
            1,
        ),
        (
            ["macros/aa/with_argument_description.sql", "--is_test"],
            True,
            False,
            False,
            0,
        ),
        (
            [
                (
                    PROJECT_DIRECTORY_PATH
                    / "macros/aa/without_arguments_description.sql"
                ).as_posix(),
                "--is_test",
            ],
            True,
            True,
            True,
            1,
        ),
        (
            [
                (
                    PROJECT_DIRECTORY_PATH / "macros/aa/with_argument_description.sql"
                ).as_posix(),
                "--is_test",
            ],
            True,
            True,
            True,
            0,
        ),
        (
            ["macros/aa/with_arguments_not_listed_in_yml.sql", "--is_test"],
            True,
            True,
            False,
            1,
        ),
        (
            ["macros/aa/with_unexpected_args_in_yml_not_in_sql.sql", "--is_test"],
            True,
            True,
            False,
            1,
        ),
    ),
)
def test_check_macro_arguments_have_desc(
    input_args,
    valid_manifest,
    valid_config,
    config_has_dbt_project_dir,
    expected_status_code,
    config_path_str,
    manifest_path_str,
):
    if config_has_dbt_project_dir:
        with open(config_path_str) as f:
            yaml_config = yaml.safe_load(f)
        yaml_config.update({"dbt-project-dir": PROJECT_DIRECTORY_PATH.as_posix()})
        with open(config_path_str, "w") as f:
            yaml.dump(yaml_config, f)
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_in_schema(
    extension, tmpdir, manifest_path_str
):
    schema_yml = """
version: 2
macros:
-   name: in_schema_argument_description
    arguments:
    -   name: test
        description: aaa
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_argument_description.sql",
            str(yml_file),
            "--is_test",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


@pytest.mark.parametrize(
    argnames=[
        "extension",
        "path",
        "dbt_project_dir",
        "test_paths",
        "return_code",
        "missing",
        "unexpected",
    ],
    argvalues=[
        (
            "yml",
            "macros/aa/with_some_argument_description.sql",
            None,
            None,
            1,
            {"with_some_argument_description": {"test2"}},
            {"with_some_argument_description": set()},
        ),
        (
            "yaml",
            "macros/aa/with_some_argument_description.sql",
            None,
            None,
            1,
            {"with_some_argument_description": {"test2"}},
            {"with_some_argument_description": set()},
        ),
        (
            "yaml",
            "macros/aa/with_argument_description.sql",
            None,
            None,
            0,
            {"with_argument_description": set()},
            {"with_argument_description": set()},
        ),
        (
            "yaml",
            "macros/aa/without_arguments_description.sql",
            None,
            None,
            1,
            {"without_arguments_description": {"test1", "test2"}},
            {"without_arguments_description": set()},
        ),
        (
            "yaml",
            (
                PROJECT_DIRECTORY_PATH / "macros/aa/without_arguments_description.sql"
            ).as_posix(),
            PROJECT_DIRECTORY_PATH,
            None,
            1,
            {"without_arguments_description": {"test1", "test2"}},
            {"without_arguments_description": set()},
        ),
        (
            "yaml",
            (
                PROJECT_DIRECTORY_PATH / "macros/aa/with_argument_description.sql"
            ).as_posix(),
            PROJECT_DIRECTORY_PATH,
            None,
            0,
            {"with_argument_description": set()},
            {"with_argument_description": set()},
        ),
        (
            "yaml",
            "macros/aa/with_arguments_not_listed_in_yml.sql",
            None,
            None,
            1,
            {"with_arguments_not_listed_in_yml": {"test1", "test2"}},
            {"with_arguments_not_listed_in_yml": set()},
        ),
        (
            "yaml",
            "macros/aa/with_unexpected_args_in_yml_not_in_sql.sql",
            None,
            None,
            1,
            {"with_unexpected_args_in_yml_not_in_sql": set()},
            {"with_unexpected_args_in_yml_not_in_sql": {"test3"}},
        ),
        (
            "yml",
            "tests/generic/custom_generic_test_macro_no_description.sql",
            None,
            None,
            1,
            {
                "test_custom_generic_test_macro_no_description": {
                    "test1",
                    "test2",
                }
            },
            {"test_custom_generic_test_macro_no_description": set()},
        ),
        (
            "yml",
            "tests/generic/custom_generic_test_macro.sql",
            None,
            None,
            0,
            {"test_custom_generic_test_macro": set()},
            {"test_custom_generic_test_macro": set()},
        ),
        (
            "yml",
            "data_tests/generic/"
            "custom_generic_test_macro_no_description_in_custom_tests_dir.sql",
            None,
            [Path("data_tests")],
            1,
            {
                "test_custom_generic_test_macro_no_description_in_custom_tests_dir": {
                    "test1",
                    "test2",
                }
            },
            {
                "test_custom_generic_test_macro_"
                "no_description_in_custom_tests_dir": set()
            },
        ),
        (
            "yml",
            "data_tests/generic/custom_generic_test_macro_in_custom_tests_dir.sql",
            None,
            [Path("data_tests")],
            0,
            {"test_custom_generic_test_macro_in_custom_tests_dir": set()},
            {"test_custom_generic_test_macro_in_custom_tests_dir": set()},
        ),
    ],
)
def test_check_argument_desc(
    extension,
    path,
    dbt_project_dir,
    test_paths,
    return_code,
    missing,
    unexpected,
    tmpdir,
    manifest,
):
    schema_yml = """
version: 2
macros:
-   name: with_some_argument_description
    arguments:
    -   name: test1
        description: aaa
    -   name: test2
    """
    if dbt_project_dir:
        dbt_project_yml_path = Path(tmpdir) / dbt_project_dir / "dbt_project.yml"
        dbt_project_yml_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        dbt_project_yml_path = Path(tmpdir) / "dbt_project.yml"
    with open(dbt_project_yml_path, "w") as f:
        test_paths = test_paths if test_paths else [Path("tests")]
        dbt_project_config = {
            "test-paths": test_paths,
        }
        yaml.dump(dbt_project_config, f)
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    hook_properties = check_argument_desc(
        [path],
        manifest,
        "",
        test_paths,
        dbt_project_dir,
    )
    res_stat = hook_properties["status_code"]
    actual_missing = hook_properties["missing"]
    actual_unexpected = hook_properties["unexpected"]
    assert res_stat == return_code
    assert actual_missing == missing
    assert actual_unexpected == unexpected


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_both(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: with_some_argument_description
    arguments:
    -   name: test4
-   name: without_arguments_description
    arguments:
    -   name: test
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "macros/aa/with_some_argument_description.sql",
            "macros/aa/without_arguments_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
            "--is_test",
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_without(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: without_arguments_description
    arguments:
    -   name: test1
        description: aa
    -   name: test2
        description: bb
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "macros/aa/without_arguments_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
            "--is_test",
        ],
    )
    assert result == 0
