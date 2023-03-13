import pytest

from pre_commit_dbt.check_source_has_all_columns import get_catalog_nodes
from pre_commit_dbt.check_source_has_all_columns import main


# Input schema, valid_catalog, expected return value
TESTS = (
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
        columns:
           - name: col1
           - name: col2
    """,
        True,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
        columns:
           - name: col1
           - name: col2
    """,
        False,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
        columns:
           - name: col1
           - name: col2
    """,
        True,
        False,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
        columns:
           - name: col1
    """,
        True,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
    """,
        True,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: without_catalog_columns
    """,
        True,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: without_catalog_columns
        columns:
           - name: col1
    """,
        True,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: without_catalog
        columns:
           - name: col1
    """,
        True,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: catalog
    tables:
    -   name: with_catalog_columns
        columns:
           - name: col1
           - name: col2
    """,
        True,
        True,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "valid_manifest",
        "valid_catalog",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_source_columns_have_desc(
    input_schema,
    valid_manifest,
    valid_catalog,
    valid_config,
    expected_status_code,
    catalog_path_str,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    yml_file = tmpdir.join("schema.yml")
    input_args = [str(yml_file), "--is_test"]
    yml_file.write(input_schema)

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(argv=input_args)
    assert status_code == expected_status_code


def test_get_catalog_nodes():
    input_txt = {
        "sources": {
            "source.test.catalog_cols": {
                "metadata": {},
                "columns": {
                    "COL1": {"type": "TEXT", "index": 1, "name": "COL1"},
                    "COL2": {"type": "TEXT", "index": 2, "name": "COL1"},
                },
            }
        }
    }
    result = get_catalog_nodes(input_txt)
    assert list(result.keys()) == [frozenset({"test", "catalog_cols"})]
