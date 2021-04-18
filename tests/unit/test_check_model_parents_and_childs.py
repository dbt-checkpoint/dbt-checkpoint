import pytest

from pre_commit_dbt.check_model_parents_and_childs import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/parent_child.sql"], ["--min-parent-cnt", "1"], True, 0),
    (["aa/bb/parent_child.sql"], ["--min-parent-cnt", "1"], False, 1),
    (["aa/bb/parent_child.sql"], ["--max-parent-cnt", "1"], True, 1),
    (["aa/bb/parent_child.sql"], ["--min-parent-cnt", "5"], True, 1),
    (["aa/bb/parent_child.sql"], ["--max-parent-cnt", "5"], True, 0),
    (["aa/bb/parent_child.sql"], ["--min-child-cnt", "1"], True, 0),
    (["aa/bb/parent_child.sql"], ["--max-child-cnt", "1"], True, 1),
    (["aa/bb/parent_child.sql"], ["--min-child-cnt", "5"], True, 1),
    (["aa/bb/parent_child.sql"], ["--max-child-cnt", "5"], True, 0),
    (
        ["aa/bb/parent_child.sql"],
        [
            "--min-parent-cnt",
            "3",
            "--max-parent-cnt",
            "5",
            "--min-child-cnt",
            "1",
            "--max-child-cnt",
            "3",
        ],
        True,
        0,
    ),
    (
        ["aa/bb/ref1.sql"],
        [
            "--min-child-cnt",
            "3",
            "--max-child-cnt",
            "5",
            "--min-parent-cnt",
            "1",
            "--max-parent-cnt",
            "3",
        ],
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql"],
        ["--min-child-cnt", "1", "--min-parent-cnt", "1"],
        True,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_parents_and_childs(
    input_schema, input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
