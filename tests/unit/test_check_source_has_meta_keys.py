import pytest

from dbt_checkpoint.check_source_has_meta_keys import main

# Input schema, allow_extra_keys, expected return value
# This test matrix remains the same.
TESTS = (
    # -- Tests where extra keys are allowed --
    (
        """
sources:
-   name: src
    loader: test
    meta:
        foo: test
        bar: test
        baz: test # Extra key
    tables:
    -   name: test
        """,
        True, # allow_extra_keys=True
        0, # Should pass
    ),
    # -- Tests where extra keys are NOT allowed --
    (
        # Should pass: keys are an exact match
        """
sources:
-   name: src
    loader: test
    meta:
        foo: test
        bar: test
    tables:
    -   name: test
        """,
        False, # allow_extra_keys=False
        0, # Should pass
    ),
    (
        # Should FAIL: contains an extra key 'baz'
        """
sources:
-   name: src
    loader: test
    meta:
        foo: test
        bar: test
        baz: test
    tables:
    -   name: test
        """,
        False, # allow_extra_keys=False
        1, # Should fail
    ),
    (
        # Should FAIL: missing key 'foo'
        """
sources:
-   name: src
    loader: test
    tables:
    -   name: test
        meta:
            bar: test
        """,
        False, # allow_extra_keys=False
        1, # Should fail
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "allow_extra_keys", "expected_status_code"), TESTS
)
def test_check_source_has_meta_keys(
    input_schema,
    allow_extra_keys,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = [
        "schema.yml",
        "--meta-keys",
        "foo",
        "bar",
        "--is_test",
        "--manifest",
        manifest_path_str,
        "--config",
        config_path_str,
    ]

    # CORRECTED LOGIC:
    # Only add the flag if we want to allow extra keys.
    # Its absence implies the stricter check.
    if allow_extra_keys:
        input_args.append("--allow-extra-keys")

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)

    # We change the working directory to the tmpdir so the hook can find the file
    with tmpdir.as_cwd():
        status_code = main(argv=input_args)

    assert status_code == expected_status_code


def test_check_source_has_meta_keys_json_error(tmpdir, capsys):
    """Test that the script exits gracefully with a JSON error."""
    # Create a malformed manifest.json file
    manifest_path = tmpdir.join("manifest.json")
    manifest_path.write("{")  # Invalid JSON

    # A dummy schema file is still needed for the script to run
    schema_path = tmpdir.join("schema.yml")
    schema_path.write("version: 2")

    input_args = [
        str(schema_path),
        "--meta-keys",
        "foo",
        "--is_test",
        "--manifest",
        str(manifest_path),
    ]

    # Run the main function and expect it to fail
    status_code = main(argv=input_args)

    # 1. Check that the script returned the correct failure code
    assert status_code == 1

    # 2. Check that the correct error message was printed to the console
    captured = capsys.readouterr()
    assert "Unable to load manifest file" in captured.out