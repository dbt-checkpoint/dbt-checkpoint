import pytest

from pre_commit_dbt.check_script_has_no_table_name import has_table_name
from pre_commit_dbt.check_script_has_no_table_name import main
from pre_commit_dbt.check_script_has_no_table_name import prev_cur_next_iter
from pre_commit_dbt.check_script_has_no_table_name import replace_comments


# Input, args, expected return value, expected output
TESTS = (  # type: ignore
    (
        """
    SELECT * FROM AA
    """,
        [],
        1,
        {"aa"},
    ),
    (
        """
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        [],
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
        [],
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
        [],
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
        [],
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
        [],
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
        [],
        0,
        {},
    ),
    (
        """
    WITH AA AS (
        SELECT *, BB as CC FROM {{ source('aa') }}
    )
    -- SELECT * FROM GG
    SELECT * FROM AA
    LEFT JOIN {{ ref('xx') }} ON AA.A = BB.A
    """,
        [],
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
        [],
        0,
        {},
    ),
    (
        """
    with source as (
        select * from {{ source('aws_lambda', 'purchase_orders') }}
    ),

    flattened as (
        select * from source, lateral flatten(line_items)
    )
    """,
        [],
        0,
        {},
    ),
    (
        """
    {# This is a test of the check-script-has-no-table-name hook, from pre-commit-dbt

    We would expect the hook to ignore this text because it is in a jinja comment block
    and not actually a join to any other table.

    #}
    with source as (
        select * from {{ source('aa', 'bb') }}
    )
    SELECT * FROM source
    """,
        [],
        0,
        {},
    ),
    (
        """
    /* This is a test of the check-script-has-no-table-name hook, from pre-commit-dbt

    We would expect the hook to ignore this text because it is in a jinja comment block
    and not actually a join to any other table.

    */
    with source as (
        select * from {{ source('aa', 'bb') }}
    )
    SELECT * FROM source
    """,
        [],
        0,
        {},
    ),
    (
        """
    -- join to other
    -- from aaa
    with source as (
        select * from {{ source('aa', 'bb') }}
    )
    SELECT * FROM source
    """,
        [],
        0,
        {},
    ),
    (
        """
with assets as (
    select * from {{ ref('stg_rse__assets') }}
),

asset_category as (
    select * from {{ ref('data_asset_category') }}
),

final as (
    select
        assets.*,
        case
            when assets.category = 'cars' then assets.category
            when assets.category = 'wine-spirits' then assets.category
            when assets.ticker in (select ticker from asset_category)
                then asset_category.asset_category
            else 'unknown'
        end as asset_category

    from assets

    left join asset_category using (ticker)
)

select * from final
    """,
        [],
        0,
        {},
    ),
    (
        """
    {% set positions = ['left', 'right', 'center'] %}

with
{% for p in positions %}

    cte_{{ p }} as (
        select '{{ p }}' as position
    ),

{% endfor %}

unioned as (
    select * from cte_left
    union all
    select * from cte_right
    union all
    select * from cte_center
)

select * from unioned
    """,
        ["--ignore-dotless-table"],
        0,
        {},
    ),
    (
        """
    {% set positions = ['left', 'right', 'center'] %}

with
{% for p in positions %}

    cte_{{ p }} as (
        select '{{ p }}' as position
    ),

{% endfor %}

unioned as (
    select * from cte_left
    union all
    select * from cte_right
    union all
    select * from cte_center
    union all
    select * from aa.bb
)

select * from unioned
    """,
        ["--ignore-dotless-table"],
        1,
        {"aa.bb"},
    ),
)


@pytest.mark.parametrize(("input_s", "args", "expected_status_code", "output"), TESTS)
def test_has_table_name(input_s, args, expected_status_code, output):
    dotless = True if "--ignore-dotless-table" in args else False
    ret, tables = has_table_name(input_s, "text.sql", dotless)
    diff = tables.symmetric_difference(output)
    assert not diff
    assert ret == expected_status_code


@pytest.mark.parametrize(("input_s", "args", "expected_status_code", "output"), TESTS)
def test_has_table_name_integration(
    input_s, args, expected_status_code, output, tmpdir
):
    path = tmpdir.join("file.txt")
    path.write_text(input_s, "utf-8")

    ret = main([str(path), *args])

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
