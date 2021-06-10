---
description: >-
  Check that column name abides to a contract, as described in this blog post by
  Emily Riederer. A contract consists on a regex pattern and a data type.
---

# check-column-name-contract

**Arguments**

`--pattern`: Regex pattern to match column names. `--dtype`: Data type.

**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-column-name-contract
   args: [--patter, "(is|has|do)_.*", --dtype, boolean]
```
{% endcode %}

**When to use it**

You want to make sure your columns follow a contract, e.g. all your boolean columns start with the prefixes `is_`, `has_` or `do_`.

**Requirements**

| Model exists in `manifest.json` [1](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f1) | Model exists in `catalog.json` [2](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f2) |
| :--- | :--- |
| ❌ Not needed | ✅ Yes |

1 It means that you need to run `dbt run`, `dbt compile` before run this hook.  
2 It means that you need to run `dbt docs generate` before run this hook.

**How it works**

* Hook takes all changed `SQL` files.
* The model name is obtained from the `SQL` file name.
* The catalog is scanned for a model.
* If any column in the found model matches the regex pattern and it's data type does not match the contract's data type, the hook fails.
* If any column in the found model matches the contract's data type and does not match the regex pattern, the hook fails.

