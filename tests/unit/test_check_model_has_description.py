from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from pre_commit_dbt.check_model_has_description import main


# Input args, valid manifest, expected return value
TESTS = (
    (
        ["aa/bb/with_description.sql"],
        {"models": [{"name": "with_description", "description": "test description"}]},
        True,
        0,
    ),
    (
        ["aa/bb/with_description.sql"],
        {"models": [{"name": "with_description", "description": "test description"}]},
        False,
        1,
    ),
    (
        ["aa/bb/without_description.sql"],
        {
            "models": [
                {
                    "name": "without_description",
                }
            ]
        },
        True,
        1,
    ),
)


# with patch("pre_commit_dbt.utils.subprocess.Popen") as mock_popen:
#         mock_popen.return_value.communicate.return_value = (
#             b"stdout",
#             b"stderr",
#         )
#         mock_popen.return_value.returncode = 0
#         result = main(("test",))
#         assert result == 0


@pytest.mark.parametrize(
    ("input_args", "schema", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_description(
    input_args, schema, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    with patch("builtins.open", mock_open(read_data="data")):
        with patch("pre_commit_dbt.utils.yaml.safe_load") as mock_safe_load:
            mock_safe_load.return_value = schema
            status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_description_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
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
