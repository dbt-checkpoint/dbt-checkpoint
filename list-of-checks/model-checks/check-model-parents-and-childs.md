---
description: Ensures the model has a specific number (max/min) of parents or/and childs.
---

# check-model-parents-and-childs

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  
`--min-parent-cnt`: Minimal number of parent sources and models. `--max-parent-cnt`: Maximal number of parent sources and models.`--min-child-cnt`: Minimal number of child models. `--max-child-cnt`: Maximal number of child models.

**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-parents-and-childs
   args: ["--min-parent-cnt", "2", "--"]
```
{% endcode %}

{% hint style="danger" %}
 Do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.
{% endhint %}

**When to use it**

You want to find orphaned models \(empty file, hard-coded reference, etc.\). Or you want to make sure that every model is used somewhere so you are not e.g. materializing unused tables.

**Requirements**

| Model exists in `manifest.json` [1](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f1) | Model exists in `catalog.json` [2](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f2) |
| :--- | :--- |
| ✅ Yes | ❌ Not needed |

1 It means that you need to run `dbt run`, `dbt compile` before run this hook.  
2 It means that you need to run `dbt docs generate` before run this hook.

**How it works**

* Hook takes all changed `SQL` files.
* The model name is obtained from the `SQL` file name.
* The manifest is scanned for a parent and child models.
* If any model does not have a number of required parents/childs, the hook fails.

