import pytest

from pre_commit_dbt.check_script_ref_and_source import check_refs_sources
from pre_commit_dbt.check_script_ref_and_source import main
from pre_commit_dbt.utils import get_json


# Input, expected return value, expected output
TESTS = (  # type: ignore
    (
        """
    SELECT * FROM {{ ref('ref1') }} bb
    JOIN {{ source('src', 'src1') }} sr1 ON bb.id = sr1.id
    JOIN {{ ref('ref2') }} r ON bb.id = r.id
    JOIN {{ source('src', 'src2') }} sr2 ON bb.id = sr2.id
    """,
        0,
        set(),
        {},
    ),
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref2") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src2") }} sr2 ON bb.id = sr2.id
    """,
        0,
        set(),
        {},
    ),
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref3") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src3") }} sr2 ON bb.id = sr2.id
    """,
        1,
        {"ref3"},
        {frozenset({"src", "src3"}): {"source_name": "src", "table_name": "src3"}},
    ),
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src3") }} sr2 ON bb.id = sr2.id
    """,
        1,
        set(),
        {frozenset({"src", "src3"}): {"source_name": "src", "table_name": "src3"}},
    ),
    (
        """
    SELECT * FROM {{ ref("ref4") }} bb
    JOIN {{ ref("ref3") }} r ON bb.id = r.id
    JOIN {{ source("src", "src4") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src3") }} sr2 ON bb.id = sr2.id
    """,
        1,
        {"ref3", "ref4"},
        {
            frozenset({"src", "src3"}): {"source_name": "src", "table_name": "src3"},
            frozenset({"src", "src4"}): {"source_name": "src", "table_name": "src4"},
        },
    ),
)

TESTS_INTEGRATION = (
    (
        """
    SELECT * FROM {{ ref("ref1") }} bb
    JOIN {{ ref("ref2") }} r ON bb.id = r.id
    JOIN {{ source("src", "src1") }} sr1 ON bb.id = sr1.id
    JOIN {{ source("src", "src2") }} sr2 ON bb.id = sr2.id
    """,
        0,
        True,
    ),
    (
        """
    SELECT * FROM aa
    """,
        0,
        True,
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
    ),
)


@pytest.mark.parametrize(
    ("input_s", "expected_status_code", "missing_models", "missing_source"), TESTS
)
def test_check_script_ref_and_source(
    input_s,
    expected_status_code,
    missing_models,
    missing_source,
    manifest_path_str,
    tmpdir,
):
    path = tmpdir.join("file.sql")
    path.write_text(input_s, "utf-8")
    manifest = get_json(manifest_path_str)
    ret, models, sources = check_refs_sources(paths=[path], manifest=manifest)

    assert ret == expected_status_code
    assert models == missing_models
    assert sources == missing_source


@pytest.mark.parametrize(
    ("input_s", "expected_status_code", "valid_manifest"), TESTS_INTEGRATION
)
def test_check_script_ref_and_source_integration(
    input_s, expected_status_code, valid_manifest, manifest_path_str, tmpdir
):
    path = tmpdir.join("file.sql")
    path.write_text(input_s, "utf-8")
    if valid_manifest:
        manifest_args = ["--manifest", manifest_path_str]
    else:
        manifest_args = []
    input_args = [str(path)]
    input_args.extend(manifest_args)
    ret = main(input_args)

    assert ret == expected_status_code
