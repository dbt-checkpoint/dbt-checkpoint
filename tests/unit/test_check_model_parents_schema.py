import pytest

from pre_commit_dbt.check_model_parents_schema import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/parent_child.sql"], [], True, 1),
    (
        ["aa/bb/parent_child.sql"],
        ["--whitelist", "source1", "source2", "test"],
        True,
        0,
    ),
    (["aa/bb/parent_child.sql"], ["--whitelist", "source1", "source2"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "source1"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "source2"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "dev", "test"], True, 1),
    (
        ["aa/bb/parent_child.sql"],
        ["--whitelist", "source1", "source2", "test"],
        False,
        1,
    ),
    (["aa/bb/parent_child.sql"], ["--blacklist", "dev", "dev1"], True, 0),
    (
        ["aa/bb/parent_child.sql"],
        ["--blacklist", "source1", "source2", "dev1"],
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql"],
        ["--blacklist", "source1", "source2", "test"],
        True,
        1,
    ),
    (["aa/bb/parent_child.sql"], ["--blacklist", "source1"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "source2"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "test"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "dev"], True, 0),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_parents_schema(
    input_schema, input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
