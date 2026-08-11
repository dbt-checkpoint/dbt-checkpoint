# dbt-checkpoint

A Python package providing pre-commit hooks that enforce quality standards in dbt (data build tool) projects. Maintained by Datacoves. Current version: 2.0.10.

## Project Layout

```
dbt_checkpoint/          # All hook implementations (65 Python files, 62 registered hooks)
  utils.py               # Core utilities (858 lines) — start here
  tracking.py            # Mixpanel telemetry
  check_*.py             # Validation hooks
  dbt_*.py               # dbt command wrapper hooks
  generate_*.py          # Generator/modifier hooks
tests/
  conftest.py            # Shared fixtures and mock manifest/catalog data
  unit/                  # One test file per hook (70 files)
.pre-commit-hooks.yaml   # Hook definitions for pre-commit framework
setup.cfg                # Package config, entry points, mypy/flake8/coverage config
.dbt-checkpoint.yaml     # Local dev config (disable-tracking: true)
HOOKS.md                 # Detailed per-hook documentation
```

## Hook Categories

- **Model checks** (25): descriptions, columns, meta keys, tests, tags, materialization, contracts, database casing
- **Script checks** (3): semicolons, table name usage, ref/source validity
- **Source checks** (14): descriptions, columns, freshness, tests, tags
- **Macro checks** (3): descriptions, arguments, meta keys
- **Other entity checks** (5): exposure, seed, snapshot, test meta keys
- **dbt commands** (7): clean, compile, deps, docs-generate, parse, run, test
- **Modifiers** (5): generate sources/properties files, unify descriptions, replace table names, remove semicolons

## Hook Implementation Pattern

Every hook follows this consistent structure:

```python
def core_check(paths, manifest, ...):
    status_code = 0
    # validation logic — set status_code = 1 on failure
    return {"status_code": status_code}

def main(argv=None):
    parser = argparse.ArgumentParser()
    add_default_args(parser)          # adds --manifest, --config, etc.
    # add hook-specific args
    args = parser.parse_args(argv)

    manifest = get_dbt_manifest(args)
    start_time = time.time()
    hook_properties = core_check(...)
    end_time = time.time()

    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(...)

    return hook_properties.get("status_code")

if __name__ == "__main__":
    exit(main())
```

## Key Utilities (utils.py)

- `get_models()`, `get_sources()`, `get_macros()`, `get_tests()` — parse manifest JSON
- `get_filenames()`, `get_filenames_with_paths()` — file discovery
- `checkpoint_safe_load()`, `get_config_file()` — YAML handling
- `add_default_args()`, `add_manifest_args()`, `add_catalog_args()` — argparse helpers
- `get_parent_childs()`, `get_dbt_manifest()`, `get_dbt_catalog()` — manifest utilities
- `get_missing_file_paths()` — finds related SQL/YAML files
- `ParseDict`, `ParseJson` — custom argparse.Action classes
- `red()`, `yellow()` — colored terminal output
- Data classes: `Model`, `Macro`, `Test`, `Source`, `ModelSchema`, `MacroSchema`, `SourceSchema`

## Development Commands

```bash
# Run tests
pytest --cov=dbt_checkpoint

# Run via tox (multiple Python versions)
tox

# Install dev dependencies
pip install -r requirements-dev.txt

# Run pre-commit hooks on all files
pre-commit run --all-files
```

## Code Standards

- **Type hints**: Extensive use of `typing` module; mypy configured with strict-like flags (disallow_untyped_defs, disallow_any_generics, etc. — relaxed for `tests.*`)
- **Line length**: 88 characters (black-compatible)
- **Coverage**: 94% minimum, branch coverage enabled
- **Linting**: flake8 + black + reorder-python-imports
- **Mutation testing**: mutmut (skips help strings, print statements, dataclass decorators)

## Testing Conventions

- Tests live in `tests/unit/`, one file per hook
- `conftest.py` provides a mock manifest with 100+ node definitions and a mock catalog
- Tests are parameterized over different manifest/schema/config combinations
- Return codes: `0` = pass, `1` = fail
- `main(argv=[...])` is the entry point for all tests

## Dependencies

- **Runtime**: `mixpanel`, `pyyaml`
- **Dev**: `pytest`, `pytest-cov`, `mutmut`, `pre-commit`
- **Std lib heavy**: `dataclasses`, `pathlib`, `subprocess`, `json`, `re`, `argparse`

## Telemetry

Every hook tracks execution via Mixpanel (`tracking.py`). Anonymous — uses dbt `user_id` from manifest, not credentials or model details. Disabled locally via `.dbt-checkpoint.yaml` (`disable-tracking: true`). Disabled in tests via a different token / test mode.

## Adding a New Hook

1. Create `dbt_checkpoint/check_<name>.py` following the standard pattern
2. Add an entry point to `setup.cfg` under `[options.entry_points] console_scripts`
3. Add hook definition to `.pre-commit-hooks.yaml`
4. Add tests in `tests/unit/test_check_<name>.py`
5. Document in `HOOKS.md`
