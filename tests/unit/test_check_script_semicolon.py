import pytest

from pre_commit_dbt.check_script_semicolon import main


# Input, expected return value
TESTS = (
    (b"foo\n", 0),
    (b"", 0),
    (b"\n\n", 0),
    (b"\n\n\n\n", 0),
    (b"foo", 0),
    (b"foo\n;", 1),
    (b";", 1),
    (b";\n\n", 1),
    (b";\n\n\n\n", 1),
    (b"foo;", 1),
    (b"\n\n\n\n;", 1),
    (b"\r\r\r\r;", 1),
    (b";foo\n", 0),
)


@pytest.mark.parametrize(("input_s", "expected_status_code"), TESTS)
def test_check_semicolon_integration(input_s, expected_status_code, tmpdir):
    path = tmpdir.join("file.txt")
    path.write_binary(input_s)

    status_code = main([str(path)])

    assert status_code == expected_status_code
