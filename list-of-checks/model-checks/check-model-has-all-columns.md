# check-model-has-all-columns

Ensures that all columns in the database are also specified in the properties file. \(usually `schema.yml`\).

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  
`--catalog`: location of `catalog.json` file. Usually `target/catalog.json`. dbt uses this file to render information like column types and table statistics into the docs site. In pre-commit-dbt is used for column operations. **Default: `target/catalog.json`**

**Example**

```text
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-all-columns
```

**When to use it**

You want to make sure that you have all the database columns listed in the properties file, or that your properties file no longer contains deleted columns.

**Requirements**

| Model exists in `manifest.json` [1](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f1) | Model exists in `catalog.json` [2](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#f2) |
| :--- | :--- |
| ✅ Yes | ✅ Yes |

1 It means that you need to run `dbt run`, `dbt compile` before run this hook.  
2 It means that you need to run `dbt docs generate` before run this hook.

**How it works**

* Hook takes all changed `SQL` files.
* The model name is obtained from the `SQL` file name.
* The manifest is scanned for a model.
* The catalog is scanned for a model.
* If there is any discrepancy between manifest and catalog models, the hook fails.

**Known limitations**

If you did not update the catalog and manifest results can be wrong.

