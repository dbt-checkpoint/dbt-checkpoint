import io

import pytest

from pre_commit_dbt.check_script_semicolon import check_semicolon
from pre_commit_dbt.remove_script_semicolon import main


# Input, expected return value, expected output
TESTS = (
    (b"foo\n", 0, b"foo\n"),
    (b"", 0, b""),
    (b"\n\n", 0, b"\n\n"),
    (b"\n\n\n\n", 0, b"\n\n\n\n"),
    (b"foo", 0, b"foo"),
    (b"foo\n;", 1, b"foo\n"),
    (b";", 1, b""),
    (b";\n\n", 1, b""),
    (b";\n\n\n\n", 1, b""),
    (b"foo;", 1, b"foo"),
    (b"\n\n\n\n;", 1, b"\n\n\n\n"),
    (b"\r\r\r\r;", 1, b"\r\r\r\r"),
    (b";foo\n", 0, b";foo\n"),
)


@pytest.mark.parametrize(("input_s", "expected_status_code", "output"), TESTS)
def test_fix_semicolon(input_s, expected_status_code, output):
    file_obj = io.BytesIO(input_s)
    status_code = check_semicolon(file_obj, replace=True)
    assert file_obj.getvalue() == output
    assert status_code == expected_status_code


def test_fix_semicolon_default():
    file_obj = io.BytesIO(b";\n\n")
    status_code = check_semicolon(file_obj)
    assert file_obj.getvalue() == b";\n\n"
    assert status_code == 1


@pytest.mark.parametrize(("input_s", "expected_status_code", "output"), TESTS)
def test_fix_semicolon_integration(input_s, expected_status_code, output, tmpdir):
    path = tmpdir.join("file.txt")
    path.write_binary(input_s)

    status_code = main([str(path)])
    file_output = path.read_binary()

    assert file_output == output
    assert status_code == expected_status_code
