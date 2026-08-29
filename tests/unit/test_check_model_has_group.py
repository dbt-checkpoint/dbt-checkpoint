import pytest

from dbt_checkpoint.check_model_has_group import main


TESTS = (
    (["aa/bb/with_group.sql"], True, None, 0),
    (["aa/bb/with_no_group.sql"], True, None, 1),
    (["aa/bb/with_group.sql"], True, ["analytics", "finance"], 0),
    (["aa/bb/with_invalid_group.sql"], True, ["analytics", "finance"], 1),
    (["aa/bb/with_group.sql"], False, None, 1),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "valid_manifest",
        "allowed_groups",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_group(
    input_args,
    valid_manifest,
    allowed_groups,
    expected_status_code,
    manifest_path_str,
):
    if allowed_groups:
        input_args.extend(["--groups"] + allowed_groups)
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    else:
        input_args.extend(["--manifest", "/tmp/nonexistent/manifest.json"])

    status_code = main(input_args)
    assert status_code == expected_status_code
