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
        True,
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
        meta:
            bar: test
            foo: test
    """,
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
        meta:
            bar: test
    """,
        True,
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
    """,
        True,
        1,
    ),
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
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_has_meta_keys(
    input_schema,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = [
        "--meta-keys",
        "foo",
        "bar",
        "--is_test",
        "--manifest",
        manifest_path_str,
    ]

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
