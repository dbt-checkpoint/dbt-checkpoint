from pathlib import Path

import pytest
import yaml

from dbt_checkpoint.check_macro_has_description import main


PROJECT_DIRECTORY_PATH = Path("path/to/dbt/project")


@pytest.mark.parametrize(
    argnames=(
        "input_args",
        "valid_manifest",
        "valid_config",
        "config_has_dbt_project_dir",
        "test_paths",
        "expected_status_code",
    ),
    argvalues=(
        (
            ["macros/aa/with_description.sql", "--is_test"],
            True,
            True,
            False,
            None,
            0,
        ),
        (["macros/aa/with_description.sql", "--is_test"], False, True, False, None, 1),
        (
            ["macros/aa/without_description.sql", "--is_test"],
            True,
            True,
            False,
            None,
            1,
        ),
        (["macros/aa/with_description.sql", "--is_test"], True, False, False, None, 0),
        (
            [
                (PROJECT_DIRECTORY_PATH / "macros/aa/with_description.sql").as_posix(),
                "--is_test",
            ],
            True,
            True,
            True,
            None,
            0,
        ),
        (
            [
                (
                    PROJECT_DIRECTORY_PATH / "macros/aa/without_description.sql"
                ).as_posix(),
                "--is_test",
            ],
            True,
            True,
            True,
            None,
            1,
        ),
        (
            ["tests/generic/custom_generic_test_macro.sql", "--is_test"],
            True,
            True,
            False,
            None,
            0,
        ),
        (
            ["tests/generic/custom_generic_test_macro_no_description.sql", "--is_test"],
            True,
            True,
            False,
            None,
            1,
        ),
        (
            ["data_tests/generic/custom_generic_test_macro.sql", "--is_test"],
            True,
            True,
            False,
            ["data_tests"],
            0,
        ),
        (
            ["data_tests/generic/custom_generic_test_macro.sql", "--is_test"],
            True,
            True,
            False,
            ["tests", "data_tests"],
            0,
        ),
        (
            ["tests/generic/custom_generic_test_macro_no_description.sql", "--is_test"],
            True,
            True,
            False,
            ["data_tests"],
            1,
        ),
    ),
)
def test_check_macro_description(
    input_args,
    valid_manifest,
    valid_config,
    config_has_dbt_project_dir,
    test_paths,
    expected_status_code,
    config_path_str,
    manifest_path_str,
    tmpdir,
):
    if config_has_dbt_project_dir:
        dbt_project_yml_path = Path(tmpdir) / PROJECT_DIRECTORY_PATH / "dbt_project.yml"
        dbt_project_yml_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path_str) as f:
            yaml_config = yaml.safe_load(f)
        yaml_config.update({"dbt-project-dir": PROJECT_DIRECTORY_PATH.as_posix()})
        with open(config_path_str, "w") as f:
            yaml.dump(yaml_config, f)
    else:
        dbt_project_yml_path = Path(tmpdir) / "dbt_project.yml"
    with open(dbt_project_yml_path, "w") as f:
        test_paths = test_paths if test_paths else ["tests"]
        dbt_project_config = {
            "test-paths": test_paths,
        }
        yaml.dump(dbt_project_config, f)
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    if valid_config:
        input_args.extend(["--config", config_path_str])
    status_code = main(input_args)
    assert status_code == expected_status_code, input_args


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_description_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: in_schema_desc
    description: blabla
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_desc.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
