from unittest.mock import patch

from dbt_checkpoint.dbt_clean import main, prepare_cmd


def test_dbt_clean():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(argv=[])
        assert result == 0


def test_dbt_clean_error():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(argv=[])
        assert result == 1


def test_dbt_clean_cmd():
    result = prepare_cmd()
    assert result == ["dbt", "clean"]
