import pytest

from pre_commit_dbt.generate_missing_sources import main

SCHEMA1 = """version: 2
sources:
- name: src
"""


SCHEMA2 = """version: 2
sources:
- name: ff
- name: src
  tables:
  - name: src1
"""

TESTS = (
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref2") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src2") }} sr2 ON bb.id = sr2.id
    """,
        0,
        True,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        """
    SELECT * FROM {{ source("src", "src3") }} bb
    """,
        1,
        True,
        True,
        SCHEMA1,
        """version: 2
sources:
- name: src
  tables:
  - name: src3
""",
    ),
    (
        """
    SELECT * FROM {{ source("src", "src3") }} bb
    """,
        1,
        True,
        True,
        SCHEMA2,
        """version: 2
sources:
- name: ff
- name: src
  tables:
  - name: src1
  - name: src3
""",
    ),
    (
        """
    SELECT * FROM {{ source("aa", "src3") }} bb
    """,
        1,
        True,
        True,
        SCHEMA2,
        SCHEMA2,
    ),
    (
        """
    SELECT * FROM {{ source("src", "src3") }} bb
    JOIN {{ source("src", "src4") }} aa ON aa.id = bb.id
    JOIN {{ source("src", "src1") }} aa ON aa.id = bb.id
    """,
        1,
        True,
        True,
        SCHEMA1,
        """version: 2
sources:
- name: src
  tables:
  - name: src3
  - name: src4
""",
    ),
    (
        """
    SELECT * FROM aa
    """,
        0,
        True,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref3") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src3") }} sr2 ON bb.id = sr2.id
    """,
        1,
        False,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref3") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src3") }} sr2 ON bb.id = sr2.id
    """,
        1,
        True,
        False,
        SCHEMA1,
        SCHEMA1,
    ),
)


@pytest.mark.parametrize(
    (
        "input_s",
        "expected_status_code",
        "valid_manifest",
        "valid_schema",
        "input_schema",
        "schema_result",
    ),
    TESTS,
)
def test_create_missing_sources(
    input_s,
    expected_status_code,
    valid_manifest,
    valid_schema,
    input_schema,
    schema_result,
    manifest_path_str,
    tmpdir,
):
    path = tmpdir.join("file.sql")
    schema = tmpdir.join("schema.yml")
    path.write_text(input_s, "utf-8")
    schema.write_text(input_schema, "utf-8")
    if valid_manifest:
        manifest_args = ["--manifest", manifest_path_str]
    else:
        manifest_args = []
    schema_file = str(schema) if valid_schema else "ff"
    input_args = [str(path), "--schema-file", schema_file]
    input_args.extend(manifest_args)
    ret = main(input_args)

    assert ret == expected_status_code

    result = schema.read_text("utf-8")
    assert result == schema_result
