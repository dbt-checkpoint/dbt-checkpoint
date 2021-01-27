import pytest

from pre_commit_dbt.check_model_has_properties_file import main

TESTS = (
    (["aa/bb/with_schema.sql"], True, 0),
    (["aa/bb/with_schema.sql"], False, 1),
    (["aa/bb/without_schema.sql"], True, 1),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_has_properties_file(
    input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    status_code = main(input_args)
    assert status_code == expected_status_code
