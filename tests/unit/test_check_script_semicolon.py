import pytest

from pre_commit_dbt.check_script_semicolon import main


# Input, expected return value
TESTS = (
    (b"foo\n", True, 0),
    (b"", True, 0),
    (b"\n\n", True, 0),
    (b"\n\n\n\n", True, 0),
    (b"foo", True, 0),
    (b"foo\n;", True, 1),
    (b";", True, 1),
    (b";\n\n", True, 1),
    (b";\n\n\n\n", True, 1),
    (b"foo;", True, 1),
    (b"\n\n\n\n;", True, 1),
    (b"\r\r\r\r;", True, 1),
    (b";foo\n", True, 0),
    (b"foo\n", False, 0),
)


@pytest.mark.parametrize(("input_s", "valid_config", "expected_status_code"), TESTS)
def test_check_semicolon_integration(
    input_s,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    path = tmpdir.join("file.txt")
    path.write_binary(input_s)
    input_args = ["--manifest", manifest_path_str, "--is_test"]

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main([str(path), *input_args])

    assert status_code == expected_status_code
