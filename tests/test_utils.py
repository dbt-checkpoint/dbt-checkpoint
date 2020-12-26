import pytest

from pre_commit_dbt.utils import CalledProcessError
from pre_commit_dbt.utils import cmd_output
from pre_commit_dbt.utils import cmd_realtime
from pre_commit_dbt.utils import paths_to_dbt_models


def test_cmd_output_error():
    with pytest.raises(CalledProcessError):
        cmd_output("sh", "-c", "exit 1")


def test_cmd_output_output():
    ret = cmd_output("echo", "hi")
    assert ret == "hi\n"


def test_cmd_realtime_error():
    with pytest.raises(CalledProcessError):
        cmd_realtime("exit 1")


def test_cmd_realtime_output():
    ret = cmd_realtime("echo", "hi")
    assert ret == 0


@pytest.mark.parametrize(
    "test_input,pre,post,expected",
    [
        (["/aa/bb/cc.txt", "ee"], "", "", ["cc", "ee"]),
        (["/aa/bb/cc.txt"], "+", "", ["+cc"]),
        (["/aa/bb/cc.txt"], "", "+", ["cc+"]),
        (["/aa/bb/cc.txt"], "+", "+", ["+cc+"]),
    ],
)
def test_paths_to_dbt_models(test_input, pre, post, expected):
    result = paths_to_dbt_models(test_input, pre, post)
    assert result == expected
