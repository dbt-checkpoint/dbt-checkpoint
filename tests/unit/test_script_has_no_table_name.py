import pytest

from pre_commit_dbt.check_script_has_no_table_name import has_table_name
from pre_commit_dbt.check_script_has_no_table_name import main
from pre_commit_dbt.check_script_has_no_table_name import prev_cur_next_iter
from pre_commit_dbt.check_script_has_no_table_name import replace_comments


# Input, expected return value, expected output
TESTS = (  # type: ignore
    (
        """
    SELECT * FROM AA
    """,
        1,
        {"aa"},
    ),
    (
        """
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        1,
        {"aa", "bb"},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM CC
    )
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        1,
        {"cc", "bb"},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    )
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        1,
        {"bb"},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    ),
    bb AS (
        SELECT * FROM xx
    )
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        1,
        {"xx"},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    ),
    bb AS (
        SELECT * FROM xx.xx.xx
    )
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        1,
        {"xx.xx.xx"},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    )
    SELECT * FROM AA
    LEFT JOIN {{ ref('xx') }} ON AA.A = BB.A
    """,
        0,
        {},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    )
    -- SELECT * FROM GG
    SELECT * FROM AA
    LEFT JOIN {{ ref('xx') }} ON AA.A = BB.A
    """,
        0,
        {},
    ),
    (
        """
    WITH AA AS (
        SELECT * FROM {{ source('aa') }}
    )
    /* SELECT *
    FROM GG
    */
    SELECT * FROM AA
    LEFT JOIN {{ ref('xx') }} ON AA.A = BB.A
    """,
        0,
        {},
    ),
)


@pytest.mark.parametrize(("input_s", "expected_status_code", "output"), TESTS)
def test_has_table_name(input_s, expected_status_code, output):
    ret, tables = has_table_name(input_s, "text.sql")
    diff = tables.symmetric_difference(output)
    assert not diff
    assert ret == expected_status_code


@pytest.mark.parametrize(("input_s", "expected_status_code", "output"), TESTS)
def test_has_table_name_integration(input_s, expected_status_code, output, tmpdir):
    path = tmpdir.join("file.txt")
    path.write_text(input_s, "utf-8")

    ret = main([str(path)])

    assert ret == expected_status_code


def test_replace_comments():
    sql = "-- select * from ee"
    assert replace_comments(sql) == ""
    sql = "--- select * from ee"
    assert replace_comments(sql) == ""
    sql = "/* select * from ee*/"
    assert replace_comments(sql) == ""


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
