import pytest

from pre_commit_dbt.check_source_has_tests import main


# Input schema, valid_manifest, expected return value
TESTS = (  # type: ignore
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        True,
        True,
        ["--test-cnt", "1", "--is_test"],
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        True,
        True,
        ["--is_test"],
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        False,
        True,
        ["--test-cnt", "1", "--is_test"],
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test3
        description: test description
    """,
        True,
        True,
        ["--test-cnt", "1", "--is_test"],
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test2
        description: test description
    """,
        True,
        True,
        ["--test-cnt", "1", "--is_test"],
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        True,
        False,
        ["--test-cnt", "1", "--is_test"],
        0,
    ),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "valid_manifest",
        "valid_config",
        "input_args",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_source_has_tests(
    input_schema,
    valid_manifest,
    valid_config,
    input_args,
    expected_status_code,
    manifest_path_str,
    config_path_str,
    tmpdir,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
