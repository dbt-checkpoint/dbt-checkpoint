import pytest

from pre_commit_dbt.check_model_parents_database import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/parent_child.sql"], [], True, 1),
    (["aa/bb/parent_child.sql"], [], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "prod", "prod2", "core"], True, 0),
    (["aa/bb/parent_child.sql"], ["--whitelist", "prod", "prod2"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "prod"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "dev", "core"], True, 1),
    (["aa/bb/parent_child.sql"], ["--whitelist", "prod", "prod2", "core"], False, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "dev", "dev1"], True, 0),
    (["aa/bb/parent_child.sql"], ["--blacklist", "prod", "prod2", "dev1"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "prod", "prod2", "core"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "core"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "prod2"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "prod"], True, 1),
    (["aa/bb/parent_child.sql"], ["--blacklist", "dev"], True, 0),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_parents_database(
    input_schema, input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
