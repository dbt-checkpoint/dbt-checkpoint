import pytest
import subprocess
import sys
import os
import json
from unittest.mock import patch

from dbt_checkpoint.check_script_has_no_table_name import has_table_name, main, prev_cur_next_iter, replace_comments, replace_string_literals
from dbt_checkpoint.utils import JsonOpenError

# (input_s, expected_status_code, output)
TESTS = (
    ("SELECT * FROM AA", 1, {"aa"}),
    ("SELECT * FROM AA LEFT JOIN BB ON AA.A = BB.A", 1, {"aa", "bb"}),
    ("WITH AA AS (SELECT * FROM CC) SELECT * FROM AA LEFT JOIN BB ON AA.A = BB.A", 1, {"cc", "bb"}),
    ("WITH AA AS (SELECT * FROM {{ source('aa') }}) SELECT * FROM AA LEFT JOIN BB ON AA.A = BB.A", 1, {"bb"}),
    ("SELECT * FROM {{ source('aa') }}", 0, set()),
    ("SELECT * FROM {{ ref('aa') }}", 0, set()),
    ("SELECT 'this is a from string' FROM aa", 1, {"aa"}),
    ("SELECT EXTRACT(YEAR FROM date_day) AS year FROM {{ ref('model') }}", 0, set()),
    ("SELECT value FROM source, UNNEST([1,2,3,4]) AS value", 1, {"source"}),
    ("SELECT SUBSTRING(column_name FROM 1 FOR 3) AS substring_value FROM {{ ref('model') }}", 0, set()),
    ("SELECT CASE WHEN column_a IS DISTINCT FROM column_b THEN 'Different' ELSE 'Same' END AS comparison FROM {{ ref('model') }}", 0, set()),
    ("AS (SELECT 1 FROM DUAL)", 1, {"dual"}),
    # ADDED FOR COVERAGE: Test for the "FROM DISTINCT FROM" edge case
    ("SELECT * FROM distinct FROM my_cte", 1, {"my_cte"}),
)

@pytest.mark.parametrize(
    ("input_s", "expected_status_code", "output"),
    TESTS,
)
def test_has_table_name(input_s, expected_status_code, output):
    ret, tables = has_table_name(input_s, "test.sql", False)
    assert ret == expected_status_code
    assert tables == output

def test_has_table_name_ignore_dotless():
    sql = "SELECT * FROM aa JOIN bb.cc"
    ret, tables = has_table_name(sql, "test.sql", dotless=True)
    assert ret == 1
    assert tables == {"bb.cc"}

def test_prev_cur_next_iter():
    txt = ["aa", "bb", "cc"]
    gen = prev_cur_next_iter(txt)
    prv, cur, nxt = next(gen)
    assert prv is None
    assert cur == "aa"
    assert nxt == "bb"
    prv, cur, nxt = next(gen)
    assert prv == "aa"
    assert cur == "bb"
    assert nxt == "cc"
    prv, cur, nxt = next(gen)
    assert prv == "bb"
    assert cur == "cc"
    assert nxt is None

def test_replace_comments():
    sql = "SELECT * -- comment\nFROM table"
    # Note: The space before the comment is removed by the regex.
    expected = "SELECT *\nFROM table"
    assert replace_comments(sql).strip() == expected.strip()

def test_replace_string_literals():
    sql = "SELECT 'hello from inside a string' FROM real_table"
    expected = "SELECT '' FROM real_table"
    assert replace_string_literals(sql) == expected

def test_has_table_name_with_comments_and_strings():
    sql = """
    -- This is a comment, it refers to table_in_comment
    SELECT
        'This is a string with table_in_string',
        *
    FROM
        real_table -- another comment
    /*
    multiline comment with table_in_multiline_comment
    */
    """
    ret, tables = has_table_name(sql, "test.sql", False)
    assert ret == 1
    assert tables == {"real_table"}

@patch("dbt_checkpoint.check_script_has_no_table_name.get_dbt_manifest")
def test_main_manifest_error(mock_get_manifest):
    mock_get_manifest.side_effect = JsonOpenError("mocked error")
    result = main(["some_file.sql"])
    assert result == 1

def test_main_with_table_name(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM my_table")
    
    with patch("dbt_checkpoint.check_script_has_no_table_name.get_dbt_manifest", return_value={}):
        result = main([str(sql_file), "--is_test"])
        assert result == 1

def test_main_without_table_name(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM {{ ref('my_model') }}")

    with patch("dbt_checkpoint.check_script_has_no_table_name.get_dbt_manifest", return_value={}):
        result = main([str(sql_file), "--is_test"])
        assert result == 0

def test_has_table_name_ignore_dotless_only():
    """Check that only dotless tables are ignored and status code is 0."""
    sql = "SELECT * FROM aa JOIN bb"
    ret, tables = has_table_name(sql, "test.sql", dotless=True)
    assert ret == 0
    assert tables == set()


def test_main_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM my_table")

    # Create a dummy manifest for the script to find
    target_path = tmp_path / "target"
    target_path.mkdir()
    manifest_file = target_path / "manifest.json"
    manifest_file.write_text(json.dumps({}))

    script_path = os.path.abspath("dbt_checkpoint/check_script_has_no_table_name.py")

    process = subprocess.run(
        [
            sys.executable,
            script_path,
            str(sql_file),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    
    assert process.returncode == 1
    assert "does not use source() or ref()" in process.stdout