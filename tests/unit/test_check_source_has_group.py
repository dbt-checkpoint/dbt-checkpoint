import pytest

from dbt_checkpoint.check_source_has_group import main

# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: with_group
    group: my_team
    tables:
    -   name: test
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: without_group
    tables:
    -   name: test
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: with_group_in_meta
    config:
        meta:
            group: my_team
    tables:
    -   name: test
    """,
        True,
        True,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_has_group(
    input_schema,
    valid_manifest,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = ["--is_test"]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code


GROUPS_TESTS = (
    (
        """
sources:
-   name: with_valid_group
    group: my_team
    tables:
    -   name: test
    """,
        ["my_team", "other_team"],
        0,
    ),
    (
        """
sources:
-   name: with_invalid_group
    group: wrong_team
    tables:
    -   name: test
    """,
        ["my_team", "other_team"],
        1,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "groups", "expected_status_code"), GROUPS_TESTS
)
def test_check_source_has_group_with_allowlist(
    input_schema,
    groups,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    input_args = [
        str(yml_file),
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--config",
        config_path_str,
        "--groups",
    ] + groups
    status_code = main(argv=input_args)
    assert status_code == expected_status_code
