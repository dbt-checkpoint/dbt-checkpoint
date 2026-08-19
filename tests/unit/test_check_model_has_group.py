import pytest

from dbt_checkpoint.check_model_has_group import main


TESTS = (  # type: ignore
    (["aa/bb/with_group.sql"], True, None, 0),
    (["aa/bb/without_group.sql"], True, None, 1),
    (["aa/bb/with_group.sql"], True, ["my_team", "other_team"], 0),
    (["aa/bb/with_invalid_group.sql"], True, ["my_team", "other_team"], 1),
    (["aa/bb/with_group.sql"], False, None, 1),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "valid_manifest",
        "groups",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_group(
    input_args,
    valid_manifest,
    groups,
    expected_status_code,
    manifest_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if groups:
        input_args.extend(["--groups"] + groups)

    status_code = main(input_args)
    assert status_code == expected_status_code
