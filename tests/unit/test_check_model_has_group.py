import pytest

from dbt_checkpoint.check_model_has_group import main


TESTS = (  # type: ignore
    (["aa/bb/with_group.sql"], True, 0),
    (["aa/bb/without_group.sql"], True, 1),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "valid_manifest",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_group(
    input_args,
    valid_manifest,
    expected_status_code,
    manifest_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code
