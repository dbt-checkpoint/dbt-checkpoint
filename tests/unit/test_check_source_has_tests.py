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
        ["--test-cnt", "1"],
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
        [],
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
        ["--test-cnt", "1"],
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
        ["--test-cnt", "1"],
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
        ["--test-cnt", "1"],
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_manifest", "input_args", "expected_status_code"), TESTS
)
def test_check_source_has_tests(
    input_schema,
    valid_manifest,
    input_args,
    expected_status_code,
    manifest_path_str,
    tmpdir,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
