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
        0,
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
        1,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_has_tags(input_schema, expected_status_code, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), "--tags", "foo", "bar"])
    assert status_code == expected_status_code
