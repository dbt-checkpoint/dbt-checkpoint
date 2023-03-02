import io

import pytest

from pre_commit_dbt.check_script_semicolon import check_semicolon
from pre_commit_dbt.remove_script_semicolon import main


# Input, expected return value, expected output
TESTS = (
    (b"foo\n", True, True, 0, b"foo\n"),
    (b"", True, True, 0, b""),
    (b"\n\n", True, True, 0, b"\n\n"),
    (b"\n\n\n\n", True, True, 0, b"\n\n\n\n"),
    (b"foo", True, True, 0, b"foo"),
    (b"foo\n;", True, True, 1, b"foo\n"),
    (b";", True, True, 1, b""),
    (b";\n\n", True, True, 1, b""),
    (b";\n\n\n\n", True, True, 1, b""),
    (b"foo;", True, True, 1, b"foo"),
    (b"\n\n\n\n;", True, True, 1, b"\n\n\n\n"),
    (b"\r\r\r\r;", True, True, 1, b"\r\r\r\r"),
    (b";foo\n", True, True, 0, b";foo\n"),
    (b"foo\n", True, False, 0, b"foo\n"),
    (b"foo\n", False, True, 1, b"foo\n"),
)


@pytest.mark.parametrize(
    ("input_s", "valid_manifest", "valid_config", "expected_status_code", "output"),
    TESTS,
)
def test_fix_semicolon(
    input_s, valid_manifest, valid_config, expected_status_code, output
):
    file_obj = io.BytesIO(input_s)
    status_code = check_semicolon(file_obj, replace=True)
    assert file_obj.getvalue() == output
    assert status_code in [0, 1]


def test_fix_semicolon_default():
    file_obj = io.BytesIO(b";\n\n")
    status_code = check_semicolon(file_obj)
    assert file_obj.getvalue() == b";\n\n"
    assert status_code == 1


@pytest.mark.parametrize(
    ("input_s", "valid_manifest", "valid_config", "expected_status_code", "output"),
    TESTS,
)
def test_fix_semicolon_integration(
    input_s,
    valid_manifest,
    valid_config,
    expected_status_code,
    output,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    path = tmpdir.join("file.txt")
    path.write_binary(input_s)
    input_args = ["--is_test"]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main([str(path), *input_args])
    file_output = path.read_binary()

    assert file_output == output
    assert status_code == expected_status_code
