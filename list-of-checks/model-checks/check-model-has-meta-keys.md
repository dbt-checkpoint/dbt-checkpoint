---
description: Ensures that the model has a list of valid meta keys.
---

# check-model-has-meta-keys

**Arguments**

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**  
`--meta-keys`: list of the required keys in the meta part of the model.

**Example**

{% code title=".pre-commit-config.yaml" %}
```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-meta-keys
   args: ['--meta-keys', 'foo', 'bar', "--"]
```
{% endcode %}

{% hint style="danger" %}
Do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.
{% endhint %}

**When to use it**

If every model needs to have certain meta keys.

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
* If any model \(from a manifest or `yml` files\) does not have specified meta keys, the hook fails.
* The meta keys must be in either the yml file **or** the manifest.

**Known limitations**

If you `run` your model and then you delete meta keys from a properties file, the hook success since the meta keys is still present in `manifest.json`.

