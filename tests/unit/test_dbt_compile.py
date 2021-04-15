from unittest.mock import patch

import pytest

from pre_commit_dbt.dbt_compile import main
from pre_commit_dbt.dbt_compile import prepare_cmd


def test_dbt_compile():
    with patch("pre_commit_dbt.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(("test",))
        assert result == 0


def test_dbt_compile_error():
    with patch("pre_commit_dbt.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            "stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(("test",))
        assert result == 1


@pytest.mark.parametrize(
    "files,global_flags,cmd_flags,models,expected",
    [
        (["/aa/bb/cc.txt"], None, None, None, ["dbt", "compile", "-m", "cc"]),
        (
            ["/aa/bb/cc.txt"],
            ["++debug", "++no-write-json"],
            None,
            None,
            ["dbt", "--debug", "--no-write-json", "compile", "-m", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            ["+t", "prod"],
            None,
            ["dbt", "compile", "-m", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            "",
            ["+t", "prod"],
            None,
            ["dbt", "compile", "-m", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            [],
            ["dbt", "compile", "-m", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            ["state:modified"],
            ["dbt", "compile", "-m", "state:modified"],
        ),
    ],
)
def test_dbt_compile_cmd(files, global_flags, cmd_flags, models, expected):
    result = prepare_cmd(files, global_flags, cmd_flags, models=models)
    assert result == expected
