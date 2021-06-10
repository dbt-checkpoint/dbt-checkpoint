# check-model-has-description

Ensures that the model has a description in the properties file \(usually `schema.yml`\).

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

**Example**

```text
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-description
```

**When to use it**

You want to make sure that all models have a description.

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
* If any model \(from a manifest or `yml` files\) does not have a description, the hook fails.
* The model description must be in either the yml file **or** the manifest.

**Known limitations**

If you `run` your model and then you delete the description from a properties file, the hook success since the description is still present in `manifest.json`.

