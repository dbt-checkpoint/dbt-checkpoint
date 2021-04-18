import pytest

from pre_commit_dbt.check_source_childs import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        ["--min-child-cnt", "1"],
        True,
        0,
    ),
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        ["--min-child-cnt", "1"],
        False,
        1,
    ),
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        ["--max-child-cnt", "1"],
        True,
        1,
    ),
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        ["--min-child-cnt", "5"],
        True,
        1,
    ),
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        ["--max-child-cnt", "5"],
        True,
        0,
    ),
    (
        """
sources:
-   name: parent_child
    tables:
    -   name: parent_child1
        description: test description
    """,
        [
            "--min-child-cnt",
            "1",
            "--max-child-cnt",
            "3",
        ],
        True,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_parents_and_childs(
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
