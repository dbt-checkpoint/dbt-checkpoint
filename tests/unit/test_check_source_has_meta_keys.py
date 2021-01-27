import pytest

from pre_commit_dbt.check_source_has_meta_keys import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: src
    loader: test
    meta:
        foo: test
        bar: test
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
    meta:
        foo: test
    tables:
    -   name: test
        meta:
            bar: test
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
        meta:
            bar: test
            foo: test
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
        meta:
            bar: test
    """,
        1,
    ),
    (
        """
sources:
-   name: src
    loader: test
    meta:
        bar: test
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
    """,
        1,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_has_meta_keys(input_schema, expected_status_code, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), "--meta-keys", "foo", "bar"])
    assert status_code == expected_status_code
