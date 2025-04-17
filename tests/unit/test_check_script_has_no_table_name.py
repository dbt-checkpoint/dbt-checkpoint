import pytest

from dbt_checkpoint.check_script_has_no_table_name import has_table_name
from dbt_checkpoint.check_script_has_no_table_name import main
from dbt_checkpoint.check_script_has_no_table_name import prev_cur_next_iter
from dbt_checkpoint.check_script_has_no_table_name import replace_comments
from dbt_checkpoint.check_script_has_no_table_name import replace_string_literals

# Input, args, expected return value, expected output
TESTS = (  # type: ignore
    (
        """
    SELECT * FROM AA
    """,
        [],
        True,
        True,
        1,
        {"aa"},
    ),
    (
        """
    SELECT * FROM AA
    LEFT JOIN BB ON AA.A = BB.A
    """,
        [],
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
        0,
        {},
    ),
    (
        """
    {# This is a test of the check-script-has-no-table-name hook, from dbt-checkpoint
    We would expect the hook to ignore this text because it is in a jinja comment block
    and not actually a join to any other table.
    #}
    with source as (
        select * from {{ source('aa', 'bb') }}
    )
    SELECT * FROM source
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    /* This is a test of the check-script-has-no-table-name hook, from dbt-checkpoint
    We would expect the hook to ignore this text because it is in a jinja comment block
    and not actually a join to any other table.
    */
    with source as (
        select * from {{ source('aa', 'bb') }}
    )
    SELECT * FROM source
    """,
        [],
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
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
        True,
        True,
        1,
        {"aa.bb"},
    ),
    (
        """
    with source as (
        select * from {{source('aa', 'bb')}}
    )
    SELECT * FROM source
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    {% macro source_cte(source_name, tuple_list) -%}
    WITH{% for cte_ref in tuple_list %} {{cte_ref[0]}} AS (
        SELECT * FROM {{ source(source_name, cte_ref[1]) }}
    ),
        {%- endfor %} final as (
    {%- endmacro %}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT * FROM AA
    """,
        [],
        False,
        True,
        1,
        {"aa"},
    ),
    (
        """
    SELECT * FROM AA
    """,
        [],
        True,
        False,
        1,
        {"aa"},
    ),
    (
        """
    SELECT
        EXTRACT(YEAR FROM date_day) AS year
    FROM {{ ref('model') }}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    WITH source AS (
        SELECT * FROM {{ source('aa', 'bb') }}
    )
    SELECT
        value
    FROM source,
    UNNEST([1,2,3,4]) AS value
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT
        SUBSTRING(column_name FROM 1 FOR 3) AS substring_value
    FROM {{ ref('model') }}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT
        'This text contains the word from in it' AS string_with_from,
        'Another string FROM with caps' AS another_string
    FROM {{ ref('model') }}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT
        CASE
            WHEN column_a IS DISTINCT FROM column_b THEN 'Different'
            ELSE 'Same'
        END AS comparison
    FROM {{ ref('model') }}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT
        value
    FROM {{ ref('model') }}
    CROSS JOIN UNNEST(array_column) AS value
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    WITH source AS (
        SELECT * FROM {{ source('aa', 'bb') }}
    )
    SELECT
        TRIM(BOTH 'x' FROM column_name) AS trimmed_value
    FROM source
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT
        COUNT(*) FILTER (WHERE is_active) AS active_count
    FROM {{ ref('model') }}
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    SELECT *
    FROM {{ ref('model') }}
    WHERE col1 = 'value'
      AND col2 DISTINCT FROM NULL
    """,
        [],
        True,
        True,
        0,
        {},
    ),
    (
        """
    WITH source AS (
        SELECT * FROM {{ source('aa', 'bb') }}
    )
    SELECT
        EXTRACT(YEAR FROM date_day) AS year,
        *
    FROM source
    JOIN actual_table ON source.id = actual_table.id
    """,
        [],
        True,
        True,
        1,
        {"actual_table"},
    ),
)


@pytest.mark.parametrize(
    (
        "input_s",
        "args",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
        "output",
    ),
    TESTS,
)
def test_has_table_name(
    input_s, args, valid_manifest, valid_config, expected_status_code, output
):
    dotless = True if "--ignore-dotless-table" in args else False
    ret, tables = has_table_name(input_s, "text.sql", dotless)
    diff = tables.symmetric_difference(output)
    assert not diff
    assert ret == expected_status_code


@pytest.mark.parametrize(
    (
        "input_s",
        "args",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
        "output",
    ),
    TESTS,
)
def test_has_table_name_integration(
    input_s,
    args,
    valid_manifest,
    valid_config,
    expected_status_code,
    output,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    path = tmpdir.join("file.txt")
    path.write_text(input_s, "utf-8")
    input_args = ["--is_test", *args]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    ret = main([str(path), *input_args])

    assert ret == expected_status_code


def test_replace_comments():
    sql = "-- select * from ee"
    assert replace_comments(sql) == ""
    sql = "--- select * from ee"
    assert replace_comments(sql) == ""
    sql = "/* select * from ee*/"
    assert replace_comments(sql) == "" or replace_comments(sql) == "/**/"


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


def test_replace_string_literals():
    # Simple string
    sql = "'select * from table'"
    assert replace_string_literals(sql) == "''"

    # String with FROM keyword
    sql = "SELECT * FROM table WHERE col = 'contains FROM keyword'"
    assert replace_string_literals(sql) == "SELECT * FROM table WHERE col = ''"

    # Multiple strings
    sql = "SELECT 'string1', 'string2 FROM multiple'"
    assert replace_string_literals(sql) == "SELECT '', ''"

    # String with escaped quotes
    sql = "SELECT 'don''t use FROM in strings'"
    assert replace_string_literals(sql) == "SELECT ''"

    # Empty string
    sql = "''"
    assert replace_string_literals(sql) == "''"


def test_context_aware_parsing():
    # Test IS DISTINCT FROM tracking
    sql = "SELECT * FROM table WHERE col IS DISTINCT FROM NULL"
    _, tables = has_table_name(sql, "test.sql")
    assert tables == {"table"}  # Only 'table' should be detected, not 'NULL'

    # Test function context tracking with EXTRACT
    sql = "SELECT EXTRACT(YEAR FROM date_field) FROM table"
    _, tables = has_table_name(sql, "test.sql")
    assert tables == {"table"}  # Only 'table' should be detected, not 'date_field'

    # Test SUBSTRING function
    sql = "SELECT SUBSTRING(name FROM 1 FOR 3) FROM employees"
    _, tables = has_table_name(sql, "test.sql")
    assert tables == {"employees"}  # Only 'employees' should be detected

    # Test TRIM function
    sql = "SELECT TRIM(BOTH '0' FROM col) FROM data"
    _, tables = has_table_name(sql, "test.sql")
    assert tables == {"data"}  # Only 'data' should be detected

    # Test multiple context patterns in one query
    sql = """
    SELECT
        EXTRACT(YEAR FROM date_field),
        CASE WHEN col IS DISTINCT FROM NULL THEN 'Valid' ELSE 'Invalid' END,
        'String with FROM in it'
    FROM table
    JOIN valid_table ON table.id = valid_table.id
    """
    _, tables = has_table_name(sql, "test.sql")
    assert tables == {"table", "valid_table"}  # Only actual tables should be detected
