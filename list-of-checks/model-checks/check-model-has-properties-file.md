---
description: Ensures that the model has a properties file (schema file).
---

# check-model-has-properties-file

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  


**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-properties-file
```
{% endcode %}

**When to use it**

You want to make sure that every model has a properties file.

**Requirements**

| Model exists in `manifest.json` [1](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f1) | Model exists in `catalog.json` [2](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f2) |
| :--- | :--- |
| ✅ Yes | ❌ Not needed |

1 It means that you need to run `dbt run`, `dbt compile` before run this hook.  
2 It means that you need to run `dbt docs generate` before run this hook.

**How it works**

* Hook takes all changed `SQL` files.
* The model name is obtained from the `SQL` file name.
* The manifest is scanned for a model.
* If any model does not have `patch_path`, the hook fails.

**Known limitations**

You need to create a schema file and then rerun your model \(`dbt run` or `dbt compile`\), otherwise, this hook will fail.

