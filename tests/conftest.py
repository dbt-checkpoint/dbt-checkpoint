import json

import pytest

from pre_commit_dbt.utils import cmd_output

MANIFEST = {
    "nodes": {
        "model.with_schema": {"patch_path": "/path/exists"},
        "model.without_schema": {"patch_path": ""},
        "model.with_description": {"description": "test description"},
        "model.without_description": {"description": ""},
        "model.with_columns": {"columns": {"test": {"name": "test"}}},
        "model.without_columns": {},
        "model.with_meta": {"meta": {"foo": "test", "bar": "test"}},
        "model.with_meta_foo": {"meta": {"foo": "test"}},
        "model.without_meta": {},
        "model.with_tags": {"tags": ["foo", "bar"]},
        "model.with_tags_foo": {"tags": ["foo"]},
        "model.with_tags_empty": {"tags": []},
        "model.without_tags": {"database": "prod", "schema": "test"},
        "model.test.catalog_cols": {
            "database": "test",
            "schema": "test",
            "alias": "test",
            "name": "catalog_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
                "col2": {"name": "col2", "description": "test"},
            },
        },
        "model.test.partial_catalog_cols": {
            "name": "partial_catalog_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
            },
        },
        "model.test.only_model_cols": {
            "name": "only_model_cols",
            "columns": {
                "col1": {"name": "col1", "description": "test"},
                "col2": {"name": "col2", "description": "test"},
            },
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
        },
        "model.test.only_catalog_cols": {"name": "only_catalog_cols", "columns": {}},
        "model.with_column_description": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2", "description": "test"},
            }
        },
        "model.with_some_column_description": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2"},
            }
        },
        "model.without_columns_description": {
            "columns": {"test1": {"name": "test1"}, "test2": {"name": "test2"}}
        },
        "model.same_col_desc_1": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2", "description": "test"},
            }
        },
        "model.same_col_desc_2": {
            "columns": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2"},
            }
        },
        "model.same_col_desc_3": {
            "columns": {
                "test1": {"name": "test1", "description": "test1"},
                "test2": {"name": "test2", "description": "test2"},
            }
        },
        "test.test1": {"tags": ["schema"], "test_metadata": {"name": "unique"}},
        "test.test2": {"tags": ["data"]},
        "test.test3": {"tags": ["schema"], "test_metadata": {"name": "unique_where"}},
        "model.with_test1": {},
        "model.with_test2": {},
        "model.with_test3": {},
        "model.without_test": {},
        "model.replaced_model": {"alias": "replaced_model"},
        "model.ref1": {"name": "ref1", "database": "core", "schema": "test"},
        "model.ref2": {"name": "ref2"},
        "model.parent_child": {"name": "parent_child"},
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
            "arguments": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2", "description": "test"},
            },
        },
        "macro.with_some_argument_description": {
            "path": "macros/aa/with_some_argument_description.sql",
            "arguments": {
                "test1": {"name": "test1", "description": "test"},
                "test2": {"name": "test2"},
            },
        },
        "macro.without_arguments_description": {
            "path": "macros/aa/without_arguments_description.sql",
            "arguments": {"test1": {"name": "test1"}, "test2": {"name": "test2"}},
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
            },
        },
        "model.test.with_boolean_column_without_prefix": {
            "metadata": {},
            "columns": {
                "COL1": {"type": "boolean", "name": "COL1"},
                "COL2": {"type": "TEXT", "name": "COL2"},
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
