import pytest

from dbt_checkpoint.replace_script_table_names import get_source_from_name
from dbt_checkpoint.replace_script_table_names import main

# Input, expected return value, expected output
TESTS = (  # type: ignore
    (
        """
    SELECT *, bb.replaced_model, replaced_model.aa FROM replaced_model
    """,
        1,
        """
    SELECT *, bb.replaced_model, replaced_model.aa FROM {{ ref('replaced_model') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM ff.replaced_model
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        0,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        False,
        True,
    ),
    (
        """
    SELECT * FROM replaced_model
    JOIN source1.table1
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    JOIN {{ source('source1', 'table1') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM replaced_model
    JOIN source1.table1
    JOIN ff.bb
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    JOIN {{ source('source1', 'table1') }}
    JOIN {{ source('ff', 'bb') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM replaced_model
    JOIN source1.table1
    JOIN ff.bb
    JOIN aa
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    JOIN {{ source('source1', 'table1') }}
    JOIN {{ source('ff', 'bb') }}
    JOIN aa
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM replaced_model
    JOIN source1.table1
    JOIN source1.table2
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    JOIN {{ source('source1', 'table1') }}
    JOIN {{ source('source1', 'table2') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM replaced_model
    JOIN aa.source1.table1
    JOIN source3.table3
    JOIN source1.table2
    """,
        1,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    JOIN {{ source('source1', 'table1') }}
    JOIN {{ source('source3', 'table3') }}
    JOIN {{ source('source1', 'table2') }}
    """,
        True,
        True,
    ),
    (
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        0,
        """
    SELECT * FROM {{ ref('replaced_model') }}
    """,
        True,
        False,
    ),
)


@pytest.mark.parametrize(
    ("input_s", "expected_status_code", "output", "valid_manifest", "valid_config"),
    TESTS,
)
def test_replace_script_table_names(
    input_s,
    expected_status_code,
    output,
    valid_manifest,
    valid_config,
    manifest_path_str,
    config_path_str,
    tmpdir,
):
    path = tmpdir.join("file.txt")
    path.write_text(input_s, "utf-8")
    input_args = [str(path), "--is_test"]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    ret = main(input_args)

    assert ret == expected_status_code
    result = path.read_text(encoding="utf-8")
    assert result == output


def test_get_source_from_name(manifest):
    result = get_source_from_name(
        manifest, {"prod.source1.src3", "dev.source1.src3", "dev2.source1.src3"}
    )
    assert list(result) == [
        ("prod.source1.src3", "{{ source('source1', 'src3') }}"),
        ("dev2.source1.src3", "{{ source('source1', 'src3') }}"),
    ]
