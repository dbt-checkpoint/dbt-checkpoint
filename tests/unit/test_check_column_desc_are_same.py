import pytest

from pre_commit_dbt.check_column_desc_are_same import main

TESTS = (  # type: ignore
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test2
    """,
        1,
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
    """,
        1,
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
    """,
        0,
        ["--ignore", "test2"],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """,
        0,
        [],
    ),
)


@pytest.mark.parametrize(("schema_yml", "expected_status_code", "ignore"), TESTS)
def test_check_column_desc_is_same(schema_yml, expected_status_code, ignore, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(schema_yml)
    input_args = [str(yml_file)]
    input_args.extend(ignore)
    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_column_desc_is_same_split(extension, tmpdir):
    schema_yml1 = """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """
    schema_yml2 = """
version: 2

models:
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test_bad
    -   name: test2
    """
    yml_file1 = tmpdir.join(f"schema1.{extension}")
    yml_file2 = tmpdir.join(f"schema2.{extension}")
    yml_file1.write(schema_yml1)
    yml_file2.write(schema_yml2)
    input_args = [str(yml_file1), str(yml_file2)]
    status_code = main(input_args)
    assert status_code == 1
