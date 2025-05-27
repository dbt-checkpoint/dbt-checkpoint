import json
import pytest
from dbt_checkpoint.check_semantic_dimensions_have_required_fields import main


@pytest.mark.parametrize(
    "semantic_manifest, required_fields, expected_exit_code",
    [
        # All required fields present -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "dimensions": [{"name": "dim1", "type": "categorical"}],
                    }
                ]
            },
            ["name", "type"],
            0,
        ),
        # Missing type -> failure
        (
            {"semantic_models": [{"name": "model1", "dimensions": [{"name": "dim1"}]}]},
            ["name", "type"],
            1,
        ),
        # Missing name -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "dimensions": [{"type": "categorical"}]}
                ]
            },
            ["name", "type"],
            1,
        ),
        # Time dimension missing time_granularity -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "dimensions": [
                            {"name": "dim1", "type": "time", "type_params": {}}
                        ],
                    }
                ]
            },
            ["name", "type"],
            1,
        ),
        # Time dimension with granularity -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "dimensions": [
                            {
                                "name": "dim1",
                                "type": "time",
                                "type_params": {"time_granularity": "hour"},
                            }
                        ],
                    }
                ]
            },
            ["name", "type"],
            0,
        ),
        # No dimensions -> success
        (
            {"semantic_models": [{"name": "model1", "dimensions": []}]},
            ["name", "type"],
            0,
        ),
    ],
)
def test_main_various_required_field_scenarios(
    semantic_manifest,
    required_fields,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write semantic manifest to a temp file
    sm_path = tmp_path / "semantic_manifest.json"
    sm_path.write_text(json.dumps(semantic_manifest))

    # Build CLI arguments
    args = [
        "--manifest",
        manifest_path_str,
        "--semantic-manifest",
        str(sm_path),
        "--required-fields",
        *required_fields,
    ]

    # Invoke main and assert exit code
    exit_code = main(argv=args)
    assert exit_code == expected_exit_code
