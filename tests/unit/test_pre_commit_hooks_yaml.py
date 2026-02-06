import pytest


def test_hook_name_length_less_le_50_chars(pre_commit_hooks_yaml_dict):
    """
    Pre-commit wants all hook names to be <= 50 characters.
    See here for more info: https://github.com/pre-commit/pre-commit.com/pull/901
    """

    for hook in pre_commit_hooks_yaml_dict:
        if len(hook["name"]) > 50:
            pytest.fail(f"Hook name '{hook['name']}' is longer than 50 characters")
