from unittest.mock import patch

import pytest

from dbt_checkpoint.generate_missing_sources import main

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
        0,
        True,
        True,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        0,
        True,
        True,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        1,
        False,
        True,
        True,
        SCHEMA1,
        SCHEMA1,
    ),
    (
        0,
        True,
        True,
        False,
        SCHEMA1,
        SCHEMA1,
    ),
)


@pytest.mark.parametrize(
    (
        "expected_status_code",
        "valid_manifest",
        "valid_schema",
        "valid_config",
        "input_schema",
        "schema_result",
    ),
    TESTS,
)
@patch("dbt_checkpoint.check_script_ref_and_source.check_refs_sources")
def test_create_missing_sources(
    mock_check_refs_sources,
    expected_status_code,
    valid_manifest,
    valid_schema,
    input_schema,
    valid_config,
    schema_result,
    manifest_path_str,
    config_path_str,
    tmpdir,
):
    mock_check_refs_sources.return_value = {
        "status_code": expected_status_code,
        "sources": {
            frozenset(["src", "src1"]): {"source_name": "src", "table_name": "src1"}
        },
    }
    path = tmpdir.join("file.sql")
    schema = tmpdir.join("schema.yml")
    schema.write_text(input_schema, "utf-8")

    if valid_manifest:
        manifest_args = ["--manifest", manifest_path_str]
    else:
        manifest_args = []
    schema_file = str(schema) if valid_schema else "ff"
    input_args = [str(path), "--schema-file", schema_file, "--is_test"]
    input_args.extend(manifest_args)

    if valid_config:
        input_args.extend(["--config", config_path_str])

    ret = main(input_args)

    assert ret == expected_status_code

    result = schema.read_text("utf-8")
    assert result == schema_result
