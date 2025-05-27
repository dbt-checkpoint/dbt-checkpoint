import json
import pytest
from dbt_checkpoint.check_semantic_measures_have_required_fields import main


@pytest.mark.parametrize(
    "semantic_manifest, required_fields, expected_exit_code",
    [
        # All required fields present -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "measures": [{"name": "m1", "expr": "SUM(x)", "agg": "sum"}],
                    }
                ]
            },
            ["name", "expr", "agg"],
            0,
        ),
        # Missing expr -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "measures": [{"name": "m1", "agg": "sum"}]}
                ]
            },
            ["name", "expr", "agg"],
            1,
        ),
        # Missing name -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "measures": [{"expr": "SUM(x)", "agg": "sum"}]}
                ]
            },
            ["name", "expr", "agg"],
            1,
        ),
        # Missing agg -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "measures": [{"name": "m1", "expr": "SUM(x)"}]}
                ]
            },
            ["name", "expr", "agg"],
            1,
        ),
        # Missing multiple fields -> failure
        (
            {"semantic_models": [{"name": "model1", "measures": [{"expr": "SUM(x)"}]}]},
            ["name", "expr", "agg"],
            1,
        ),
        # No measures -> success
        (
            {"semantic_models": [{"name": "model1", "measures": []}]},
            ["name", "expr", "agg"],
            0,
        ),
    ],
)
def test_main_various_measure_field_scenarios(
    semantic_manifest,
    required_fields,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write semantic manifest to temp file
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
