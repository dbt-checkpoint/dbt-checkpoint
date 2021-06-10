# check-model-has-tests-by-name

Ensures that the model has a number of tests of a certain name \(e.g. data, unique\).

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  
`--tests`: key-value pairs of test names. Key is the name of test and value is required minimal number of tests eg. --test unique=1 not\_null=2 \(do not put spaces before or after the = sign\).

**Example**

```text
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests-by-name
   args: ["--tests", "unique=1", "data=1", "--"]
```

{% hint style="danger" %}
 Do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.
{% endhint %}

**When to use it**

You want to make sure that every model has certain tests.

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
* If any model does not have the number of required tests, the hook fails.

