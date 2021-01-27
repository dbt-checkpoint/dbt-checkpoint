from unittest.mock import patch

from pre_commit_dbt.dbt_docs_generate import docs_generate_cmd
from pre_commit_dbt.dbt_docs_generate import main


def test_dbt_docs_generate():
    with patch("pre_commit_dbt.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(argv=[])
        assert result == 0


def test_dbt_docs_generate_error():
    with patch("pre_commit_dbt.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(argv=[])
        assert result == 1


def test_dbt_docs_generate_cmd():
    result = docs_generate_cmd()
    assert result == ["dbt", "docs", "generate"]
