---
description: >-
  This page describes how to install pre-commit-dbt locally on your computer. It
  is preferred way on how to install it, since it prevents you from committing
  errors.
---

# Local installation

### Install

In your command line run:

```bash
pip install pre-commit
```

For detailed installation and usage, instructions see [pre-commit.com](https://pre-commit.com/) site.

### Setup

* Create a file named `.pre-commit-config.yaml` in your `dbt` root folder.
* Add hooks you want to run before every commit. E.g.:

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
  rev: v1.0.0
  hooks:
  - id: check-script-semicolon
  - id: check-script-has-no-table-name
  - id: dbt-test
  - id: dbt-docs-generate
  - id: check-model-has-all-columns
    name: Check columns - core
    files: ^models/core
  - id: check-model-has-all-columns
    name: Check columns - mart
    files: ^models/mart
  - id: check-model-columns-have-desc
    files: ^models/mart
```
{% endcode %}

* Optionally, run `pre-commit install` to set up the git hook scripts. With this, `pre-commit` will run automatically on `git commit`! You can also manually run `pre-commit run` after you `stage` all files you want to run. Or `pre-commit run --all-files` to run the hooks against all of the files \(not only `staged`\).

