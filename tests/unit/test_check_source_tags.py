import pytest

from pre_commit_dbt.check_source_tags import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: src
    loader: test
    tags:
    -   foo
    -   bar
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
-   name: src
    loader: test
    tags:
    -   foo
    -   bar
    tables:
    -   name: test
    """,
        False,
        True,
        1,
    ),
    (
        """
sources:
-   name: src
    loader: test
    tags:
    -   foo
    tables:
    -   name: test
        tags:
        -    bar
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: src
    loader: test
    tables:
    -   name: test
        tags:
        -   bar
        -   foo
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: src
    loader: test
    tables:
    -   name: test
        tags:
        -   bar
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: src
    loader: test
    tags:
    -    bar
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
-   name: src
    loader: test
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
-   name: src
    loader: test
    tags:
    -   ff
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
-   name: src
    loader: test
    tags:
    -   foo
    -   ff
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
-   name: src
    loader: test
    tables:
    -   name: test
        tags:
        -   foo
        -   ff
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: src
    loader: test
    tags:
    -   foo
    -   bar
    tables:
    -   name: test
    """,
        True,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_has_tags(
    input_schema,
    valid_manifest,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = ["--tags", "foo", "bar", "--is_test"]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
