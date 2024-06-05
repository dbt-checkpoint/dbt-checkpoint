import pytest

from dbt_checkpoint.check_selectors_duplicate_names import (
    check_selectors_duplicate_names,
)

# Test cases: input data, expected errors
TESTS = [
    (
        {"selectors": [{"name": "selector1"}, {"name": "selector2"}]},
        [],
    ),
    (
        {"selectors": [{"name": "selector1"}, {"name": "selector1"}]},
        ["Duplicate selector name found: 'selector1'"],
    ),
    (
        {
            "selectors": [
                {"name": "selector1"},
                {"name": "selector2"},
                {"name": "selector1"},
            ]
        },
        ["Duplicate selector name found: 'selector1'"],
    ),
    (
        {
            "selectors": [
                {"name": "selector1"},
                {"name": "selector2"},
                {"name": "selector2"},
                {"name": "selector1"},
            ]
        },
        [
            "Duplicate selector name found: 'selector2'",
            "Duplicate selector name found: 'selector1'",
        ],
    ),
    (
        {"selectors": []},
        [],
    ),
]


@pytest.mark.parametrize("input_data, expected_errors", TESTS)
def test_check_selectors_duplicate_names(input_data, expected_errors):
    errors = check_selectors_duplicate_names(input_data)
    assert sorted(errors) == sorted(expected_errors)
