import re
from pathlib import Path

from dbt_checkpoint import __version__


def test_version():
    """
    Tests that the imported package version matches the version defined in pyproject.toml.
    This replaces the obsolete method of reading setup.cfg metadata.
    """
    pyproject_path = Path("pyproject.toml")
    
    if not pyproject_path.exists():
        # Safety check, although tox usually runs from the project root.
        raise FileNotFoundError(f"Cannot find required file: {pyproject_path}")

    # Read the text content of pyproject.toml
    content = pyproject_path.read_text(encoding="utf-8")
    
    # Use regex to find the version line: version = "..." 
    # This relies on the version being statically defined in [project]
    match = re.search(r'^version\s*=\s*"(.*)"$', content, re.MULTILINE)
    
    if not match:
        raise ValueError("Could not find 'version = \"...\"' definition in pyproject.toml")
    
    # Extract the version string (e.g., '2.0.7')
    expected_version = match.group(1)

    # Assert that the package's imported version matches the version in the packaging file
    assert __version__ == expected_version