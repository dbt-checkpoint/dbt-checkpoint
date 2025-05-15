import pytest

from dbt_checkpoint.check_model_tags import main

TESTS = (
    (["aa/bb/with_tags.sql", "--is_test", "--tags", "foo", "bar"], True, True, 0),
    (["aa/bb/with_tags_foo.sql", "--is_test", "--tags", "foo", "bar"], True, True, 0),
    (["aa/bb/with_tags_foo.sql", "--is_test", "--tags", "bar"], True, True, 1),
    (["aa/bb/with_tags_empty.sql", "--is_test", "--tags", "bar"], True, True, 0),
    (["aa/bb/without_tags.sql", "--is_test", "--tags", "foo", "bar"], True, True, 0),
    (["aa/bb/without_tags.sql", "--is_test", "--tags", "foo", "bar"], False, True, 1),
    (["aa/bb/with_tags.sql", "--is_test", "--tags", "foo", "bar"], True, False, 0),
    (["aa/bb/without_tags_all.sql", "--is_test", "--tags", "foo", "bar", "waldo", "--has-all-tags"], True, True, 1),
    (["aa/bb/without_tags_any.sql", "--is_test", "--tags", "waldo", "--has-any-tag"], True, True, 1),
    (["aa/bb/with_tags_all.sql", "--is_test", "--tags", "foo", "bar", "--has-all-tags"], True, True, 0),
    (["aa/bb/with_tags_any.sql", "--is_test", "--tags", "foo", "bar", "waldo", "--has-any-tags"], True, True, 0),      
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_model_tags(
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code


def test_check_model_tags_in_changed(tmpdir, manifest_path_str):
    schema_yml = """
version: 2
models:
-   name: in_schema_tags
    tags:
        - foo
        - bar
-   name: xxx
    """
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_tags.sql",
            str(yml_file),
            "--is_test",
            "--tags",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
