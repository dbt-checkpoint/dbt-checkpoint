---
description: Ensures that the model has a number of tests from a group of tests.
---

# check-model-has-tests-by-group

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  
`--tests`: list of test names. `--test_cnt`: number of tests required across test group.

**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests-by-group
   args: ["--tests", "unique", "unique_where", "--test-cnt", "1", "--"] 
```
{% endcode %}

{% hint style="danger" %}
 Do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.
{% endhint %}

**When to use it**

You want to make sure that every model has one \(or more\) of a group of eligible tests \(e.g. a set of unique tests\).

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

