import pytest

from dbt_checkpoint.check_macro_has_description import main

# Input args, valid manifest, expected return value
TESTS = (
    (["macros/aa/with_description.sql", "--is_test"], True, True, 0),
    (["macros/aa/with_description.sql", "--is_test"], False, True, 1),
    (["macros/aa/without_description.sql", "--is_test"], True, True, 1),
    (["macros/aa/with_description.sql", "--is_test"], True, False, 0),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_macro_description(
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


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_description_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: in_schema_desc
    description: blabla
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_desc.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


def test_check_materialization_with_description(manifest_path_str, config_path_str):
    """A materialization SQL file resolves via path stem and passes when description present."""
    # The manifest contains materialization_snowpipe_snowflake whose path stem is "snowpipe"
    # get_macros should match "snowpipe.sql" via the path fallback introduced in this fix.
    result = main(
        argv=[
            "macros/materializations/snowpipe.sql",
            "--is_test",
            "--manifest",
            manifest_path_str,
            "--config",
            config_path_str,
        ]
    )
    assert result == 0


def test_check_materialization_without_description_fails(manifest_path_str, config_path_str):
    """A materialization SQL file resolves via path stem and fails when description is empty."""
    # materialization_snowpipe_default has an empty description — check should fail.
    # We pass the same file but rely on the adapter-specific entry having no description.
    # To isolate the no-description case we use a fresh manifest with only the empty entry.
    import json, tempfile, os
    manifest = {
        "macros": {
            "macro.project.materialization_snowpipe_default": {
                "name": "materialization_snowpipe_default",
                "description": "",
                "path": "macros/materializations/snowpipe.sql",
                "arguments": [],
            }
        }
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(manifest, f)
        empty_manifest_path = f.name

    try:
        result = main(
            argv=[
                "macros/materializations/snowpipe.sql",
                "--is_test",
                "--manifest",
                empty_manifest_path,
                "--config",
                config_path_str,
            ]
        )
    finally:
        os.unlink(empty_manifest_path)

    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_materialization_description_in_schema_yml(extension, tmpdir, manifest_path_str):
    """A materialization can be described via a schema yml using its file stem as the name."""
    schema_yml = """
version: 2
macros:
-   name: snowpipe
    description: "Custom Snowpipe materialization."
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "macros/materializations/snowpipe.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
