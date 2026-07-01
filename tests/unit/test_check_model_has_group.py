import pytest

from dbt_checkpoint.check_model_has_group import main


TESTS = (
    (["aa/bb/with_group.sql"], None, 0),
    (["aa/bb/with_no_group.sql"], None, 1),
    (["aa/bb/with_group.sql"], ["analytics", "finance"], 0),
    (["aa/bb/with_invalid_group.sql"], ["analytics", "finance"], 1),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "allowed_groups",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_group(
    input_args,
    allowed_groups,
    expected_status_code,
    manifest_path_str,
):
    if allowed_groups:
        input_args.extend(["--groups"] + allowed_groups)
    input_args.extend(["--manifest", manifest_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code
