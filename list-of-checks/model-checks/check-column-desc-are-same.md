---
description: Check the models have the same descriptions for the same column names.
---

# check-column-desc-are-same

**Arguments**

`--ignore`: columns for which do not check whether have a different description.

**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-column-desc-are-same
```
{% endcode %}

**When to use it**

E.g. in two of your models, you have `customer_id` with the description `This is cutomer_id`, but there is one model where column `customer_id` has a description `Something else`. This hook finds discrepancies between column descriptions.

**Requirements**

| Model exists in `manifest.json` [1](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f1) | Model exists in `catalog.json` [2](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f2) |
| :--- | :--- |
| ❌ Not needed since it also validates properties files | ❌ Not needed |

1 It means that you need to run `dbt run`, `dbt compile` before run this hook.  
2 It means that you need to run `dbt docs generate` before run this hook.

**How it works**

* Hook takes all changed `yml` and `SQL` files.
* The model name is obtained from the `SQL` file name.
* The manifest is scanned for a model.
* Modified `yml` files are scanned for a model.
* If any column in the found model has different descriptions than others, the hook fails.
* The description must be in either the yml file **or** the manifest.

