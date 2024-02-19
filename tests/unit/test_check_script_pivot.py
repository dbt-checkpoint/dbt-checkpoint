import pytest

from dbt_checkpoint.check_script_pivot import main


# Input, expected return value
TESTS = (
    (b"nothing", 0),
    (b"", 0),
    (b"PIVOT(", 1),
    (b"pivot", 1),
    (b"pi vot", 0),
    (b" PIVOT ", 1),
    (b"hello PIVOT hello", 1),
)


@pytest.mark.parametrize(("input_s", "expected_status_code"), TESTS)
def test_check_pivot_integration(input_s, expected_status_code, tmpdir):
    path = tmpdir.join("file.txt")
    path.write_binary(input_s)

    status_code = main([str(path)])

    assert status_code == expected_status_code