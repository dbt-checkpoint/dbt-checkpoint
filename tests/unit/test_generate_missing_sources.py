from unittest.mock import patch

import pytest
import yaml

from dbt_checkpoint.generate_missing_sources import main

# A schema where the 'src' source exists but has no tables.
# The hook should be able to add a table to this.
SCHEMA_WITH_SRC = """
version: 2
sources:
- name: src
"""
SCHEMA_WITH_SRC_RESULT = """
version: 2
sources:
- name: src
  tables:
  - name: src1
"""

# A schema without the 'src' source. The hook will attempt to modify but won't add the table.
SCHEMA_WITHOUT_SRC = """
version: 2
sources:
- name: other_source
"""

# A schema where the 'src' source already has a table.
# The hook should append another table.
SCHEMA_WITH_EXISTING_TABLE = """
version: 2
sources:
- name: src
  tables:
  - name: existing_table
"""
SCHEMA_WITH_EXISTING_TABLE_RESULT = """
version: 2
sources:
- name: src
  tables:
  - name: existing_table
  - name: src1
"""


TESTS = (
    # SCENARIO 1: Success. 'src' exists, so 'src1' is added. Expects code 1 (file modified).
    (
        1, True, True, SCHEMA_WITH_SRC, SCHEMA_WITH_SRC_RESULT,
    ),
    # SCENARIO 2: Success. 'src' exists with other tables, 'src1' is appended. Expects code 1.
    (
        1, True, True, SCHEMA_WITH_EXISTING_TABLE, SCHEMA_WITH_EXISTING_TABLE_RESULT,
    ),
    # SCENARIO 3: Failure case. 'src' does NOT exist. Hook still returns 1 but makes no change.
    (
        1, True, True, SCHEMA_WITHOUT_SRC, SCHEMA_WITHOUT_SRC, # <-- CORRECTED THIS LINE
    ),
    # SCENARIO 4: Failure. Schema file path is invalid. Hook returns 1.
    (
        1, True, False, SCHEMA_WITH_SRC, SCHEMA_WITH_SRC,
    ),
    # SCENARIO 5: Failure. Manifest is missing. Hook returns 1.
    (
        1, False, True, SCHEMA_WITH_SRC, SCHEMA_WITH_SRC,
    ),
)


@pytest.mark.parametrize(
    (
        "expected_status_code",
        "valid_manifest",
        "valid_schema",
        "input_schema",
        "expected_schema_result",
    ),
    TESTS,
)
@patch("dbt_checkpoint.generate_missing_sources.check_refs_sources")
def test_create_missing_sources(
    mock_check_refs_sources,
    expected_status_code,
    valid_manifest,
    valid_schema,
    input_schema,
    expected_schema_result,
    manifest_path_str,
    config_path_str,
    tmpdir,
):
    # This simulates `check_refs_sources` finding a missing source.
    mock_check_refs_sources.return_value = {
        "status_code": 1,
        "sources": {
            frozenset(["src", "src1"]): {"source_name": "src", "table_name": "src1"}
        },
    }
    
    sql_file = tmpdir.join("file.sql")
    sql_file.write("select 1")
    schema_file = tmpdir.join("schema.yml")
    schema_file.write(input_schema)

    # Prepare arguments
    schema_file_path = str(schema_file) if valid_schema else "non_existent_schema.yml"
    manifest_args = ["--manifest", manifest_path_str] if valid_manifest else []
    
    argv = [
        str(sql_file),
        "--schema-file",
        schema_file_path,
        *manifest_args,
    ]

    # Run the main function
    actual_status_code = main(argv)

    # Assert the status code is as expected
    assert actual_status_code == expected_status_code

    # Read the file content AFTER the hook has run
    result_text = schema_file.read_text("utf-8")

    # Compare the YAML content, ignoring formatting differences
    expected_yaml = yaml.safe_load(expected_schema_result)
    actual_yaml = yaml.safe_load(result_text)
    
    assert actual_yaml == expected_yaml

@patch("dbt_checkpoint.generate_missing_sources.check_refs_sources")
def test_create_missing_sources_no_missing_sources(
    mock_check_refs_sources,
    manifest_path_str,
    tmpdir,
):
    # This simulates `check_refs_sources` finding NO missing sources.
    mock_check_refs_sources.return_value = {
        "status_code": 0,
        "sources": {},
    }
    
    # Prepare dummy files for the hook to run against
    sql_file = tmpdir.join("file.sql")
    sql_file.write("select 1")
    
    initial_schema_content = "version: 2\nsources:\n- name: src\n"
    schema_file = tmpdir.join("schema.yml")
    schema_file.write(initial_schema_content)

    argv = [
        str(sql_file),
        "--schema-file",
        str(schema_file),
        "--manifest",
        manifest_path_str,
    ]

    # Run the main function
    actual_status_code = main(argv)

    # Assert the status code is 0 (success, no changes made)
    assert actual_status_code == 0

    # Read the file content and assert it is unchanged
    result_text = schema_file.read_text("utf-8")
    assert result_text == initial_schema_content