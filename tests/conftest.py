import json

import pytest
import yaml

from dbt_checkpoint.utils import cmd_output

MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v6.json",
        "dbt_version": "1.5.0",
        "generated_at": "2022-10-04T16:19:51.780894Z",
        "user_id": "test_user_id",
        "send_anonymous_usage_stats": True,
        "adapter_type": "snowflake",
    },
    "nodes": {
        "model.with_version.v1": {
            "patch_path": "project://bb/with_version.yml",
            "path": "aa/bb/with_version.sql",
            "root_path": "/path/to/aa",
            "config": {
                "materialized": "table",
            },
            "version": 1,
            "latest_version": 1,
        },
        "model.with_schema": {
            "patch_path": "project://bb/with_schema.yml",
            "path": "aa/bb/with_schema.sql",
            "root_path": "/path/to/aa",
            "config": {
                "materialized": "table",
            },
        },
        "model.without_schema": {
            "patch_path": "",
            "path": "aa/bb/without_schema.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_description": {
            "description": "test description",
            "patch_path": "project://bb/with_description.yml",
            "path": "aa/bb/with_description.sql",
            "root_path": "/path/to/aa",
            "original_file_path": "bb/with_description.sql",
        },
        "model.without_description": {
            "description": "",
            "patch_path": "project://bb/without_description.yml",
            "path": "aa/bb/without_description.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_columns": {
            "columns": {"test": {"name": "test"}},
            "patch_path": "project://bb/with_columns.yml",
            "path": "aa/bb/with_columns.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_columns": {
            "patch_path": "project://bb/without_columns.yml",
            "path": "aa/bb/without_columns.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_meta": {
            "meta": {"foo": "test", "bar": "test"},
            "patch_path": "project://bb/with_meta.yml",
            "path": "aa/bb/with_meta.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_meta_foo": {
            "meta": {"foo": "test"},
            "patch_path": "project://bb/with_meta_foo.yml",
            "path": "aa/bb/with_meta_foo.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_meta": {
            "patch_path": "project://bb/without_meta.yml",
            "path": "aa/bb/without_meta.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_labels": {
            "config": {
                "labels": {"foo": "test", "bar": "test"},
            },
            "patch_path": "project://bb/with_labels.yml",
            "path": "aa/bb/with_labels.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_labels_foo": {
            "config": {
                "labels": {"foo": "test"},
            },
            "patch_path": "project://bb/with_labels_foo.yml",
            "path": "aa/bb/with_labels_foo.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_labels": {
            "patch_path": "project://bb/without_labels.yml",
            "path": "aa/bb/without_labels.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_tags": {
            "tags": ["foo", "bar"],
            "patch_path": "project://bb/with_tags.yml",
            "path": "aa/bb/with_tags.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_tags_foo": {
            "tags": ["foo"],
            "patch_path": "project://bb/with_tags_foo.yml",
            "path": "aa/bb/with_tags_foo.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_tags_empty": {
            "tags": [],
            "patch_path": "project://bb/with_tags_empty.yml",
            "path": "aa/bb/with_tags_empty.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_tags": {
            "database": "prod",
            "schema": "test",
            "name": "without_tags",
            "patch_path": "project://bb/without_tags.yml",
            "path": "aa/bb/without_tags.sql",
            "root_path": "/path/to/aa",
        },
        "model.test.catalog_cols": {
            "database": "test",
            "schema": "test",
            "alias": "test",
            "name": "catalog_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
                "col2": {"name": "col2", "description": "test"},
            },
            "patch_path": "project://bb/catalog_cols.yml",
            "path": "aa/bb/catalog_cols.sql",
            "root_path": "/path/to/aa",
        },
        "model.test.partial_catalog_cols": {
            "name": "partial_catalog_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
            },
            "patch_path": "project://bb/partial_catalog_cols.yml",
            "path": "aa/bb/partial_catalog_cols.sql",
            "root_path": "/path/to/aa",
        },
        "model.test.only_model_cols": {
            "name": "only_model_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
                "col2": {"name": "col2", "description": "test"},
            },
            "patch_path": "project://bb/only_model_cols.yml",
            "path": "aa/bb/only_model_cols.sql",
            "root_path": "/path/to/aa",
        },
        "model.test.without_catalog": {
            "name": "without_catalog",
            "database": "test",
            "schema": "test",
            "alias": "test",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
                "col2": {"name": "col2", "description": "test"},
            },
            "patch_path": "project://bb/without_catalog.yml",
            "path": "aa/bb/without_catalog.sql",
            "root_path": "/path/to/aa",
        },
        "model.test.only_catalog_cols": {
            "name": "only_catalog_cols",
            "columns": {},
            "patch_path": "project://bb/only_catalog_cols.yml",
            "path": "aa/bb/only_catalog_cols.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_column_description": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2", "description": "test"},
            },
            "patch_path": "project://bb/with_column_description.yml",
            "path": "aa/bb/with_column_description.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_some_column_description": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2"},
            },
            "patch_path": "project://bb/with_some_column_description.yml",
            "path": "aa/bb/with_some_column_description.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_columns_description": {
            "columns": {
                "test1": {"name": "test1"},
                "test2": {"name": "test2"},
                "patch_path": "project://bb/without_columns_description.yml",
                "path": "aa/bb/without_columns_description.sql",
                "root_path": "/path/to/aa",
            }
        },
        "model.same_col_desc_1": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2", "description": "test"},
            },
            "patch_path": "project://bb/same_col_desc_1.yml",
            "path": "aa/bb/same_col_desc_1.sql",
            "root_path": "/path/to/aa",
        },
        "model.same_col_desc_2": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2"},
            },
            "patch_path": "project://bb/same_col_desc_2.yml",
            "path": "aa/bb/same_col_desc_2.sql",
            "root_path": "/path/to/aa",
        },
        "model.same_col_desc_3": {
            "columns": {
                "test1": {"name": "test1", "description": "test1"},
                "test2": {"name": "test2", "description": "test2"},
            },
            "patch_path": "project://bb/same_col_desc_3.yml",
            "path": "aa/bb/same_col_desc_3.sql",
            "root_path": "/path/to/aa",
        },
        "test.test1": {"tags": ["schema"], "test_metadata": {"name": "unique"}},
        "test.test2": {"tags": ["data"]},
        "test.test3": {"tags": ["schema"], "test_metadata": {"name": "unique_where"}},
        "model.with_test1": {
            "patch_path": "project://bb/with_test1.yml",
            "path": "aa/bb/with_test1.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
        },
        "model.with_test2": {
            "patch_path": "project://bb/with_test2.yml",
            "path": "aa/bb/with_test2.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_test3": {
            "patch_path": "project://bb/with_test3.yml",
            "path": "aa/bb/with_test3.sql",
            "root_path": "/path/to/aa",
        },
        "model.without_test": {
            "patch_path": "project://bb/without_test.yml",
            "path": "aa/bb/without_test.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "incremental"},
        },
        "model.replaced_model": {
            "alias": "replaced_model",
            "patch_path": "project://bb/replaced_model.yml",
            "path": "aa/bb/replaced_model.sql",
            "root_path": "/path/to/aa",
        },
        "model.ref1": {
            "name": "ref1",
            "database": "core",
            "schema": "test",
            "patch_path": "project://bb/ref1.yml",
            "path": "aa/bb/ref1.sql",
            "root_path": "/path/to/aa",
        },
        "model.ref2": {
            "name": "ref2",
            "patch_path": "project://bb/ref2.yml",
            "path": "aa/bb/ref2.sql",
            "root_path": "/path/to/aa",
        },
        "model.parent_child": {
            "name": "parent_child",
            "patch_path": "project://bb/parent_child.yml",
            "path": "aa/bb/parent_child.sql",
            "root_path": "/path/to/aa",
            "config": {
                "materialized": "view",
            },
        },
        "model.with_contract": {
            "patch_path": "project://bb/with_contract.yml",
            "path": "aa/bb/with_contract.sql",
            "root_path": "/path/to/aa",
            "config": {"contract": {"enforced": True}, "materialized": "view"},
        },
        "model.with_no_contract": {
            "patch_path": "project://bb/with_no_contract.yml",
            "path": "aa/bb/with_no_contract.sql",
            "root_path": "/path/to/aa",
        },
        "model.with_no_constraints": {
            "patch_path": "project://bb/with_no_constraints.yml",
            "path": "aa/bb/with_no_constraints.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
        },
        "model.with_empty_constraints": {
            "patch_path": "project://bb/with_empty_constraints.yml",
            "path": "aa/bb/with_empty_constraints.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
            "constraints": [],
        },
        "model.with_constraints": {
            "patch_path": "project://bb/with_constraints.yml",
            "path": "aa/bb/with_constraints.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
            "constraints": [
                {"type": "primary_key", "columns": ["col_a", "col_b"]},
                {"type": "foreign_key", "columns": ["col_a", "col_b"]},
                {"type": "check", "columns": ["col_a", "col_b"]},
                {"type": "not_null", "columns": ["col_a", "col_b"]},
                {"type": "unique", "columns": ["col_a", "col_b"]},
                {"type": "custom"},
            ],
        },
        "model.with_constraints_no_columns": {
            "patch_path": "project://bb/with_constraints_no_columns.yml",
            "path": "aa/bb/with_constraints_no_columns.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
            "constraints": [{"type": "primary_key"}],
        },
        "model.with_constraints_no_match": {
            "patch_path": "project://bb/with_constraints_no_match.yml",
            "path": "aa/bb/with_constraints_no_match.sql",
            "root_path": "/path/to/aa",
            "config": {"materialized": "table"},
            "constraints": [{"type": "foreign_key", "columns": ["col_a", "col_b"]}],
        },
        "snapshot.some_snapshot": {
            "name": "some_snapshot",
            "patch_path": "project://bb/some_snapshot.yml",
            "path": "aa/bb/some_snapshot.sql",
            "root_path": "/path/to/aa",
            "config": {
                "materialized": "snapshot",
            },
        },
    },
    "sources": {
        "source.source1.table1": {
            "database": "prod",
            "schema": "source1",
            "source_name": "source1",
            "name": "table1",
        },
        "source.source1.table2": {
            "database": "prod",
            "schema": "source1",
            "source_name": "source1",
            "name": "table2",
        },
        "source.src.src1": {
            "database": "prod",
            "schema": "source1",
            "source_name": "src",
            "name": "src1",
        },
        "source.src.src2": {
            "database": "prod2",
            "schema": "source2",
            "source_name": "src",
            "name": "src2",
        },
        "source.prod.source1.src3": {
            "database": "prod",
            "schema": "source1",
            "source_name": "source1",
            "name": "src3",
        },
        "source.dev2.source1.src3": {
            "database": "dev2",
            "schema": "source1",
            "source_name": "source1",
            "name": "src3",
        },
        "source.parent_child.parent_child1": {
            "database": "dev2",
            "schema": "source1",
            "source_name": "parent_child",
            "name": "parent_child1",
        },
    },
    "macros": {
        "macro.without_description": {
            "description": "",
            "path": "macros/aa/without_description.sql",
        },
        "macro.with_description": {
            "description": "test description",
            "path": "macros/aa/with_description.sql",
        },
        "macro.with_argument_description": {
            "path": "macros/aa/with_argument_description.sql",
            "arguments": [
                {"name": "test1", "description": "test"},
                {"name": "test2", "description": "test"},
            ],
        },
        "macro.with_some_argument_description": {
            "path": "macros/aa/with_some_argument_description.sql",
            "arguments": [
                {"name": "test1", "description": "test"},
                {"name": "test2"},
            ],
        },
        "macro.without_arguments_description": {
            "path": "macros/aa/without_arguments_description.sql",
            "arguments": [{"name": "test1"}, {"name": "test2"}],
        },
    },
    "child_map": {
        "source.test.test1": ["test.test1", "test.test2", "model.with_schema"],
        "source.test.test2": ["test.test1"],
        "source.test.test3": [],
        "model.with_test1": ["test.test1", "test.test2", "model.with_schema"],
        "model.with_test2": ["test.test1"],
        "model.with_test3": ["test.test1", "test.test3"],
        "model.without_test": [],
        "model.parent_child": [
            "model.ref2",
            "model.replaced_model",
            "ccc.ccc.ddd",
            "ddd.ccc.ddd",
        ],
        "source.parent_child.parent_child1": [
            "model.ref2",
            "model.replaced_model",
            "ccc.ccc.ddd",
            "ddd.ccc.ddd",
        ],
    },
    "parent_map": {
        "model.parent_child": [
            "source.src.src2",
            "source.src.src1",
            "model.ref1",
            "model.without_tags",
            "bbb.ccc.ddd",
        ]
    },
}

CATALOG = {
    "nodes": {
        "model.test.catalog_cols": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "TEXT", "index": 1, "name": "COL1"},
                "COL2": {"type": "TEXT", "index": 2, "name": "COL1"},
            },
        },
        "model.test.partial_catalog_cols": {
            "metadata": {},
            "columns": {"COL2": {"type": "TEXT", "index": 2, "name": "COL1"}},
        },
        "model.test.only_catalog_cols": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "TEXT", "index": 1, "name": "COL1"},
                "COL2": {"type": "TEXT", "index": 2, "name": "COL1"},
            },
        },
        "model.test.only_model_cols": {"metadata": {}, "columns": {}},
        "model.test.with_boolean_column_with_prefix": {
            "metadata": {},
            "columns": {
                "is_boolean": {"type": "boolean", "name": "is_boolean"},
                "COL2": {"type": "TEXT", "name": "COL2"},
                "IS_ALSO_BOOLEAN": {"type": "BOOLEAN", "name": "IS_ALSO_BOOLEAN"},
            },
        },
        "model.test.with_boolean_column_without_prefix": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "boolean", "name": "COL1"},
                "COL2": {"type": "BOOLEAN", "name": "COL2"},
                "COL3": {"type": "TEXT", "name": "COL3"},
            },
        },
        "model.test.without_boolean_column_with_prefix": {
            "metadata": {},
            "columns": {
                "is_not_boolean": {"type": "TEXT", "name": "is_not_boolean"},
                "COL2": {"type": "TEXT", "name": "COL2"},
            },
        },
        "model.test.without_boolean_column_without_prefix": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "TEXT", "name": "COL1"},
                "COL2": {"type": "TEXT", "name": "COL2"},
            },
        },
    },
    "sources": {
        "source.test.ff.with_catalog_columns": {},
        "source.test.aa.catalog.with_catalog_columns": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "TEXT", "index": 1, "name": "COL1"},
                "COL2": {"type": "TEXT", "index": 2, "name": "COL1"},
            },
        },
        "source.test.ff.catalog.without_catalog_columns": {
            "metadata": {},
            "columns": {},
        },
    },
}


CONFIG_FILE = {"version": 1, "disable-tracking": True, "is-test": True}
CONFIG_WITH_TRACKING_FILE = {"version": 1, "disable-tracking": False, "is-test": True}


@pytest.fixture(scope="function")
def config_path_str(tmpdir):
    yaml_config = yaml.dump(CONFIG_FILE)
    file = tmpdir.mkdir("temp").join(".dbt-checkpoint.yaml")
    file.write(yaml_config)
    yield str(file)


@pytest.fixture(scope="function")
def config_with_tracking_path_str(tmpdir):
    yaml_config = yaml.dump(CONFIG_WITH_TRACKING_FILE)
    file = tmpdir.mkdir("temp").join(".dbt-checkpoint.yaml")
    file.write(yaml_config)
    yield str(file)


@pytest.fixture(scope="function")
def manifest_path_str(tmpdir):
    json_manifest = json.dumps(MANIFEST)
    file = tmpdir.mkdir("target").join("manifest.json")
    file.write(json_manifest)
    yield str(file)


@pytest.fixture(scope="function")
def manifest():
    yield MANIFEST


@pytest.fixture(scope="function")
def catalog_path_str(tmpdir):
    json_catalog = json.dumps(CATALOG)
    file = tmpdir.mkdir("target_catalog").join("catalog.json")
    file.write(json_catalog)
    yield str(file)


@pytest.fixture(scope="function")
def temp_git_dir(tmpdir):
    git_dir = tmpdir.join("gits")
    cmd_output("git", "init", "--", str(git_dir))
    yield git_dir


@pytest.fixture(scope="module")
def pre_commit_hooks_yaml_dict():
    with open(".pre-commit-hooks.yaml") as f:
        yield yaml.safe_load(f)
