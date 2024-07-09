import pytest

from dbt_checkpoint.check_model_has_properties_file import main

TESTS = (
    (["aa/bb/with_schema.sql", "--is_test"], True, True, 0),
    (["aa/bb/with_schema.sql", "--is_test"], False, True, 1),
    (["aa/bb/with_version_v1.sql", "--is_test"], True, True, 0),
    (["aa/bb/without_schema.sql", "--is_test"], True, False, 1),
    (["aa/bb/some_snapshot.sql", "--is_test"], True, True, 0),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_model_has_properties_file(
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code
