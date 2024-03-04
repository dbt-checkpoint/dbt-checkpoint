import pytest
from yaml import safe_dump

from dbt_checkpoint.utils import CompilationException, get_config_file


@pytest.fixture
def temp_yaml_file(tmp_path):
    data = {"disable-tracking": True, "version": 1}  # Sample YAML data with version
    file_path = tmp_path / "dbt-checkpoint.yaml"
    with open(file_path, "w") as f:
        f.write(safe_dump(data))
    return file_path


def test_get_config_file_existing_yaml(temp_yaml_file):
    config = get_config_file(temp_yaml_file)
    assert isinstance(config, dict)
    assert "disable-tracking" in config
    assert config["disable-tracking"] == True


def test_get_config_file_nonexistent_yaml():
    config = get_config_file("nonexistent.yaml")
    assert config == {}


def test_get_config_file_with_dot_dbt_checkpoint_yaml(tmp_path):
    data = {"disable-tracking": True, "version": 1}  # Sample YAML data with version
    file_path = tmp_path / ".dbt-checkpoint.yaml"
    with open(file_path, "w") as f:
        f.write(safe_dump(data))
    config = get_config_file(file_path)
    assert isinstance(config, dict)
    assert "disable-tracking" in config
    assert config["disable-tracking"] == True


def test_get_config_file_with_dot_dbt_checkpoint_yml(tmp_path):
    data = {"disable-tracking": True, "version": 1}  # Sample yml data with version
    file_path = tmp_path / ".dbt-checkpoint.yml"
    with open(file_path, "w") as f:
        f.write(safe_dump(data))
    config = get_config_file(file_path)
    assert isinstance(config, dict)
    assert "disable-tracking" in config
    assert config["disable-tracking"] == True


def test_get_config_file_missing_version(tmp_path):
    file_path = tmp_path / "missing_version.yaml"
    with open(file_path, "w") as f:
        f.write(safe_dump({"disable-tracking": True}))  # YAML data without version
    with pytest.raises(CompilationException):
        get_config_file(file_path)
