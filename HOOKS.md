## List of `pre-commit-dbt` hooks

:bulb: Click on hook name to view the details.

[`check-column-name-contract`]: https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-column-name-contract

**Model checks:**
 * [`check-column-desc-are-same`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-column-desc-are-same): Check column descriptions are the same.
 * [`check-column-name-contract`](): Check column name abides to contract.
 * [`check-model-columns-have-desc`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-columns-have-desc): Check the model columns have description.
 * [`check-model-has-all-columns`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-all-columns): Check the model has all columns in the properties file.
 * [`check-model-has-description`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-description): Check the model has description.
 * [`check-model-has-meta-keys`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-meta-keys): Check the model has keys in the meta part.
 * [`check-model-has-properties-file`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-properties-file): Check the model has properties file.
 * [`check-model-has-tests-by-name`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-tests-by-name): Check the model has a number of tests by test name.
 * [`check-model-has-tests-by-type`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-tests-by-type): Check the model has a number of tests by test type.
 * [`check-model-has-tests-by-group`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-tests-by-group): Check the model has a number of tests from a group of tests.
 * [`check-model-has-tests`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-has-tests): Check the model has a number of tests.
 * [`check-model-name-contract`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-name-contract): Check model name abides to contract.
 * [`check-model-parents-and-childs`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-parents-and-childs): Check the model has a specific number (max/min) of parents or/and childs.
 * [`check-model-parents-database`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-parents-database): Check the parent model has a specific database.
 * [`check-model-parents-schema`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-parents-schema): Check the parent model has a specific schema.
 * [`check-model-tags`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-model-tags): Check the model has valid tags.

**Script checks:**
 * [`check-script-semicolon`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-script-semicolon): Check the script does not contain a semicolon.
 * [`check-script-has-no-table-name`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-script-has-no-table-name): Check the script has not table name (is not using `source()` or `ref()` macro for all tables).
 * [`check-script-ref-and-source`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-script-ref-and-source): Check the script has only existing refs and sources.

**Source checks:**
 * [`check-source-columns-have-desc`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-columns-have-desc): Check for source column descriptions.
 * [`check-source-has-all-columns`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-all-columns): Check the source has all columns in the properties file.
 * [`check-source-table-has-description`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-table-has-description): Check the source table has description.
 * [`check-source-has-freshness`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-freshness): Check the source has the freshness.
 * [`check-source-has-loader`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-loader): Check the source has loader option.
 * [`check-source-has-meta-keys`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-meta-keys): Check the source has keys in the meta part.
 * [`check-source-has-tests-by-name`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-tests-by-name): Check the source has a number of tests by test name.
 * [`check-source-has-tests-by-type`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-tests-by-type): Check the source has a number of tests by test type.
 * [`check-source-has-tests`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-has-tests): Check the source has a number of tests.
 * [`check-source-tags`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-tags): Check the source has valid tags.
 * [`check-source-childs`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-source-childs): Check the source has a specific number (max/min) of childs.

**Macro checks:**
 * [`check-macro-has-description`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-macro-has-description): Check the macro has description.
 * [`check-macro-arguments-have-desc`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#check-macro-arguments-have-desc): Check the macro arguments have description.

**Modifiers:**
 * [`generate-missing-sources`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#generate-missing-sources): If any source is missing this hook tries to create it.
 * [`generate-model-properties-file`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#generate-model-properties-file): Generate model properties file.
 * [`unify-column-description`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#unify-column-description): Unify column descriptions across all models.
 * [`replace-script-table-names`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#replace-script-table-names): Replace table names with `source()` or `ref()` macros in the script.
 * [`remove-script-semicolon`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#remove-script-semicolon): Remove the semicolon at the end of the script.

**dbt commands:**
 * [`dbt-clean`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-clean): Run `dbt clean` command.
 * [`dbt-compile`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-compile): Run `dbt compile` command.
 * [`dbt-deps`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-deps): Run `dbt deps` command.
 * [`dbt-docs-generate`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-docs-generate): Run `dbt docs generate` command.
 * [`dbt-run`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-run): Run `dbt run` command.
 * [`dbt-test`](https://github.com/offbi/pre-commit-dbt/blob/main/HOOKS.md#dbt-test): Run `dbt test` command.

---
:exclamation:**If you have an idea for a new hook or you found a bug, [let us know](https://github.com/offbi/pre-commit-dbt/issues/new)**:exclamation:

## Available Hooks
### `check-column-desc-are-same`

Check the models have the same descriptions for the same column names.

#### Arguments

`--ignore`: columns for which do not check whether have a different description.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-column-desc-are-same
```

#### When to use it

E.g. in two of your models, you have `customer_id` with the description `This is cutomer_id`, but there is one model where column `customer_id` has a description `Something else`. This hook finds discrepancies between column descriptions.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- Modified `yml` files are scanned for a model.
- If any column in the found model has different descriptions than others, the hook fails.
- The description must be in either the yml file **or** the manifest.

-----
### `check-column-name-contract`

Check that column name abides to a contract, as described in [this blog post](https://emilyriederer.netlify.app/post/column-name-contracts/) by Emily Riederer. A contract consists on a regex pattern and a data type.

#### Arguments

`--pattern`: Regex pattern to match column names.
`--dtype`: Data type.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-column-name-contract
   args: [--pattern, "(is|has|do)_.*", --dtype, boolean]
```

#### When to use it

You want to make sure your columns follow a contract, e.g. all your boolean columns start with the prefixes `is_`, `has_` or `do_`.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :white_check_mark: Yes |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The catalog is scanned for a model.
- If any column in the found model matches the regex pattern and it's data type does not match the contract's data type, the hook fails.
- If any column in the found model matches the contract's data type and does not match the regex pattern, the hook fails.


-----
### `check-model-columns-have-desc`

Ensures that the model has columns with descriptions in the properties file (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-columns-have-desc
```

#### When to use it

You want to make sure that all specified columns in the properties files (usually `schema.yml`) have some description. **This hook does not validate if all database columns are also present in a properties file.**
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- Modified `yml` files are scanned for a model.
- If any column in the found model does not contain a description, the hook fails.
- The description must be in either the yml file **or** the manifest.

#### Known limitations

If you `run` your model and then you delete column description from a properties file, the hook success since the description is still present in `manifest.json`.

-----
### `check-model-has-all-columns`

Ensures that all columns in the database are also specified in the properties file. (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--catalog`: location of `catalog.json` file. Usually `target/catalog.json`. dbt uses this file to render information like column types and table statistics into the docs site. In pre-commit-dbt is used for column operations. **Default: `target/catalog.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-all-columns
```

#### When to use it

You want to make sure that you have all the database columns listed in the properties file, or that your properties file no longer contains deleted columns.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes | :white_check_mark: Yes |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- The catalog is scanned for a model.
- If there is any discrepancy between manifest and catalog models, the hook fails.

#### Known limitations

If you did not update the catalog and manifest results can be wrong.

-----

### `check-model-has-description`

Ensures that the model has a description in the properties file (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-description
```

#### When to use it

You want to make sure that all models have a description.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- Modified `yml` files are scanned for a model.
- If any model (from a manifest or `yml` files) does not have a description, the hook fails.
- The model description must be in either the yml file **or** the manifest.

#### Known limitations

If you `run` your model and then you delete the description from a properties file, the hook success since the description is still present in `manifest.json`.

-----

### `check-model-has-meta-keys`

Ensures that the model has a list of valid meta keys. (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--meta-keys`: list of the required keys in the meta part of the model.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-meta-keys
   args: ['--meta-keys', 'foo', 'bar', "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

If every model needs to have certain meta keys.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- Modified `yml` files are scanned for a model.
- If any model (from a manifest or `yml` files) does not have specified meta keys, the hook fails.
- The meta keys must be in either the yml file **or** the manifest.

#### Known limitations

If you `run` your model and then you delete meta keys from a properties file, the hook success since the meta keys is still present in `manifest.json`.

-----

### `check-model-has-properties-file`

Ensures that the model has a properties file (schema file).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-properties-file
```

#### When to use it

You want to make sure that every model has a properties file.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model does not have `patch_path`, the hook fails.

#### Known limitations

You need to create a schema file and then rerun your model (`dbt run` or `dbt compile`), otherwise, this hook will fail.

-----

### `check-model-has-tests-by-name`

Ensures that the model has a number of tests of a certain name (e.g. data, unique).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tests`: key-value pairs of test names. Key is the name of test and value is required minimal number of tests eg. --test unique=1 not_null=2 (do not put spaces before or after the = sign).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests-by-name
   args: ["--tests", "unique=1", "data=1", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every model has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model does not have the number of required tests, the hook fails.

-----

### `check-model-has-tests-by-type`

Ensures that the model has a number of tests of a certain type (data, schema).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tests`: key-value pairs of test types. Key is the type of test (data or schema) and value is required eg. --test data=1 schema=2 (do not put spaces before or after the = sign).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests-by-type
   args: ["--tests", "schema=1", "data=1", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every model has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model does not have the number of required tests, the hook fails.

-----

### `check-model-has-tests-by-group`

Ensures that the model has a number of tests from a group of tests.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tests`: list of test names.
`--test_cnt`: number of tests required across test group.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests-by-group
   args: ["--tests", "unique", "unique_where", "--test-cnt", "1", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every model has one (or more) of a group of eligible tests (e.g. a set of unique tests).
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model does not have the number of required tests, the hook fails.

-----

### `check-model-has-tests`

Ensures that the model has a number of tests.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--test-cnt`: Minimum number of tests required.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests
   args: ["--test-cnt", "2", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every model was tested.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model does not have a number of required tests, the hook fails.

-----

### `check-model-name-contract`

Check that model name abides to a contract (similar to [`check-column-name-contract`]()). A contract consists of a regex pattern.

#### Arguments

`--pattern`: Regex pattern to match model names.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-name-contract
   args: [--pattern, "(base_|stg_).*"]
   files: models/staging/
 - id: check-model-name-contract
   args: [--pattern, "(dim_|fct_).*"]
   files: models/marts/
```

#### When to use it

You want to make sure your model names follow a naming convention (e.g., staging models start with a `stg_` prefix).

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :white_check_mark: Yes |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The catalog is scanned for a model.
- If any model does not match the regex pattern, the hook fails.

-----
### `check-model-parents-and-childs`

Ensures the model has a specific number (max/min) of parents or/and childs.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--min-parent-cnt`: Minimal number of parent sources and models.
`--max-parent-cnt`: Maximal number of parent sources and models.
`--min-child-cnt`: Minimal number of child models.
`--max-child-cnt`: Maximal number of child models.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-parents-and-childs
   args: ["--min-parent-cnt", "2", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to find orphaned models (empty file, hard-coded reference, etc.). Or you want to make sure that every model is used somewhere so you are not e.g. materializing unused tables.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a parent and child models.
- If any model does not have a number of required parents/childs, the hook fails.

-----

### `check-model-parents-database`

Ensures the parent models or sources are from certain database.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--whitelist`: list of allowed databases.
`--blacklist`: list of disabled databases.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-parents-database
   args: ["--blacklist", "SRC", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to be sure that certain models are using only models from specified database(s).
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a parent models/sources.
- If any marent model does not have allowed or has disabled databases, the hook fails.

-----

### `check-model-parents-schema`

Ensures the parent models or sources are from certain schema.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--whitelist`: list of allowed schemas.
`--blacklist`: list of disabled schemas.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-parents-schema
   args: ["--blacklist", "stage", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to be sure that certain models are using only models from specified schema(s).
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a parent models/sources.
- If any marent model does not have allowed or has disabled schemas, the hook fails.

-----

### `check-model-tags`

Ensures that the model has only valid tags from the provided list.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tags`: A list of tags that models can have.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-tags
   args: ["--tags", "foo", "bar", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

Make sure you did not typo in tags.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- If any model has different tags than specified, the hook fails.

-----

### `check-script-ref-and-source`

Ensures that the script contains only existing sources or macros.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-script-ref-and-source
```

#### When to use it

Make sure you have only valid ref and sources in your script and you do not want to wait for `dbt run` to find them. This hook also finds all missing ref and sources, not find first missing only.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

### `check-script-semicolon`

Ensure that the script does not have a semicolon at the end of the file.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-script-semicolon
```

#### When to use it

Make sure you did not provide a semicolon at the end of the file.

#### How it works

- Hook takes all changed `SQL` files.
- It parses `SQL` and finds all sources and refs. If those objects do not exist in `manifest.json`, the hook fails.

-----

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- If the file contains a semicolon at the end of the file, the hook fails.

-----

### `check-script-has-no-table-name`

Ensures that the script is using only source or ref macro to specify the table name.

#### Arguments

`--ignore-dotless-table`: consider all tables without dot in name as CTE

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-script-has-no-table-name
```

#### When to use it

To make sure that you have only refs and sources in your `SQL` files.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- `SQL` is parsed and if it contains direct tables (not ref() or source()), the hook fails.

-----
### `check-source-columns-have-desc`

Ensures that the source has columns with descriptions in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-columns-have-desc
```

#### When to use it

You want to make sure that all specified columns in the properties files (usually `schema.yml`) have some description. **This hook does not validate if all database columns are also present in a properties file.**
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If any column in the source does not contain a description, the hook fails.

-----

### `check-source-has-all-columns`

Ensures that all columns in the database are also specified in the properties file. (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--catalog`: location of `catalog.json` file. Usually `target/catalog.json`. dbt uses this file to render information like column types and table statistics into the docs site. In pre-commit-dbt is used for column operations. **Default: `target/catalog.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-all-columns
```

#### When to use it

You want to make sure that you have all the database columns listed in the properties file, or that your properties file no longer contains deleted columns.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: No | :white_check_mark: Yes |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- The catalog is scanned for a model.
- If there is any discrepancy between found yml sources and catalog sources, the hook fails.

#### Known limitations

If you did not update the catalog and manifest results can be wrong.

-----
### `check-source-table-has-description`

Ensures that the source table has a description in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-table-has-description
```

#### When to use it

You want to make sure that all sources have a description.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source table does not have a description, the hook fails.

-----
### `check-source-has-freshness`

Ensures that the source has freshness options in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-freshness
   args: ["--freshness", "error_after", "warn_after", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that all freshness is correctly set.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have freshness correctly set, the hook fails.

-----
### `check-source-has-loader`

Ensures that the source has a loader option in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-loader
```

#### When to use it

You want to make sure that the source has loader specified.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have a loader set, the hook fails.

-----
### `check-source-has-meta-keys`

Ensures that the source has a list of valid meta keys. (usually `schema.yml`).

#### Arguments

`--meta-keys`: list of the required keys in the meta part of the model.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-meta-keys
   args: ['--meta-keys', 'foo', 'bar', "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

If every source needs to have certain meta keys.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have the required meta keys set, the hook fails.

-----
### `check-source-has-tests-by-name`

Ensures that the source has a number of tests of a certain name (e.g. data, unique).

#### Arguments

`--tests`: key-value pairs of test names. Key is the name of test and value is required minimal number of tests eg. --test unique=1 not_null=2 (do not put spaces before or after the = sign).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-tests-by-name
   args: ["--tests", "unique=1", "data=1", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every source has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Yes | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have the required test names, the hook fails.

-----

### `check-source-has-tests-by-type`

Ensures that the source has a number of tests of a certain type (data, schema).

#### Arguments

`--tests`: key-value pairs of test types. Key is a type of test (data or schema) and value is required eg. --test data=1 schema=2 (do not put spaces before or after the = sign).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-tests-by-type
   args: ["--tests", "schema=1", "data=1", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every source has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Yes | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have the required test types, the hook fails.

-----

### `check-source-has-tests`

Ensures that the source has a number of tests.

#### Arguments

`--tests`: Minimum number of tests required.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-has-tests
   args: ["--tests", "2", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to make sure that every source was tested.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Yes | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source does not have the required test count, the hook fails.

-----

### `check-source-tags`

Ensures that the source has only valid tags from the provided list.

#### Arguments

`--tags`: A list of tags that sources can have.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-tags
   args: ["--tags", "foo", "bar", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

Make sure you did not typo in tags.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- If the source has different tags than specified, the hook fails.

-----

### `check-source-childs`

Ensures the source has a specific number (max/min) of childs.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--min-child-cnt`: Minimal number of child models.
`--max-child-cnt`: Maximal number of child models.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-source-childs
   args: ["--min-child-cnt", "2", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want to find orphaned sources without any dependencies. Or you want to make sure that every source is used somewhere.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes| :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml`.
- All sources from yml file are parsed.
- The manifest is scanned for child models.
- If any source does not have a number of required childs, the hook fails.

-----
### `check-macro-has-description`

Ensures that the macro has a description in the properties file (usually `macro.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-macro-has-description
```

#### When to use it

You want to make sure that all macros have a description.
#### Requirements

| Macro exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Macro exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The macro name is obtained from the `SQL` file name.
- The manifest is scanned for a macro.
- Modified `yml` files are scanned for a macro.
- If any macro (from a manifest or `yml` files) does not have a description, the hook fails.
- The macro description must be in either the yml file **or** the manifest.

#### Known limitations

If you `run` and then you delete the description from a properties file, the hook success since the description is still present in `manifest.json`.

-----
### `check-macro-arguments-have-desc`

Ensures that the macro has arguments with descriptions in the properties file (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.1
 hooks:
 - id: check-macro-arguments-have-desc
```

#### When to use it

You want to make sure that all specified arguments in the properties files (usually `schema.yml`) have some description. **This hook does not validate if all macro arguments are also present in a properties file.**
#### Requirements

| Macro exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Macro exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since it also validates properties files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `yml` and `SQL` files.
- The macro name is obtained from the `SQL` file name.
- The manifest is scanned for a macro.
- Modified `yml` files are scanned for a macro.
- If any argument in the found macro does not contain a description, the hook fails.
- The description must be in either the yml file **or** the manifest.

#### Known limitations

If you `run` and then you delete argument description from a properties file, the hook success since the description is still present in `manifest.json`.

-----

### `generate-missing-sources`

If any source is missing this hook tries to create it.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**.</br>
`--schema-file`: Location of schema.yml file. Where new source tables should be created.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: generate-missing-sources
   args: ["--schema-file", "models/schema.yml", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You are too lazy to define schemas manually :D.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since this hook tries to generate even non-existent source | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- SQL is parsed to find all sources.
- If the source exists in the manifest, nothing is done.
- If not, a new source is created in specified `schema-file` and the hook fails.

#### Known limitations

Source "envelope" has to exist in specified `schema-file`, something like this:

```
version: 2
sources:
- name: <source_name>
```

Otherwise, it is not possible to automatically generate a new source table.

Unfortunately, this hook breaks your formatting.

-----

### `unify-column-description`

Unify column descriptions across all models.

#### Arguments

`--ignore`: Columns for which do not check whether have a different description.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: unify-column-description
   args: ["--ignore", "foo", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You want the descriptions of the same columns to be the same. E.g. in two of your models, you have `customer_id` with the description `This is cutomer_id`, but there is one model where column `customer_id` has a description `Something else`. This hook finds discrepancies between column descriptions and replaces them. So as the results all columns going to have the description `This is customer_id`

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed since this hook is using only yaml files | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `YAML` files.
- From those files columns are parsed and compared.
- If one column name has more than one (not empty) description, the description with the most occurrences is taken and the hook fails.
- If it is not possible to decide which description is dominant, no changes are made.

#### Known limitations

If it is not possible to decide which description is dominant, no changes are made.

-----

### `replace-script-table-names`

Replace table names with `source` or `ref` macros in the script.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: replace-script-table-names
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You are running and debugging your `SQL` in the editor. This editor does not know `source` or `ref` macros. So every time you copy the script from the editor into `dbt` project you need to rewrite all table names to `source` or `ref`. That's boring and error-prone. If you run this hook it will replace all table names with macros instead of you.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- SQL is parsed and table names are found.
- Firstly it tries to find table name in models - `ref`.
- Then it tries to find a table in sources - `source`.
- If nothing is found it creates unknown `source` as `source('<schema_name>', '<table_name>')`
- If the script contains only `ref` and `source` macros, the hook success.

-----

### `generate-model-properties-file`

Generate model properties file if does not exist.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**.<br/>
`--catalog`: location of `catalog.json` file. Usually `target/catalog.json`. dbt uses this file to render information like column types and table statistics into the docs site. In pre-commit-dbt is used for column operations. **Default: `target/catalog.json`**<br/>
`--properties-file`: Location of file where new model properties should be generated. Suffix has to be `yml` or `yaml`. It can also include {database}, {schema}, {name} and {alias} variables. E.g. /models/{schema}/{name}.yml for model `foo.bar` will create properties file in /models/foo/bar.yml. If path already exists, properties are appended.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: generate-model-properties-file
   args: ["--properties-file", "/models/{schema}/{name}.yml", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

#### When to use it

You are running and debugging your `SQL` in the editor. This editor does not know `source` or `ref` macros. So every time you copy the script from the editor into `dbt` project you need to rewrite all table names to `source` or `ref`. That's boring and error-prone. If you run this hook it will replace all table names with macros instead of you.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :white_check_mark: Yes | :x: Yes |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- The model name is obtained from the `SQL` file name.
- The manifest is scanned for a model.
- The catalog is scanned for a model.
- If the model does not have `patch_path` in the manifest, the new schema is written to the specified path. The hook fails.

#### Known limitations

Unfortunately, this hook breaks your formatting in the written file.

-----

### `remove-script-semicolon`

Remove the semicolon at the end of the script.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: remove-script-semicolon
```

#### When to use it

You are too lazy or forgetful to delete one character at the end of the script.

#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

<sup id="f1">1</sup> It means that you need to run `dbt run`, `dbt compile` before run this hook.<br/>
<sup id="f2">2</sup> It means that you need to run `dbt docs generate` before run this hook.

#### How it works

- Hook takes all changed `SQL` files.
- If the file contains a semicolon at the end of the file, it is removed and the hook fails.

-----

### `dbt-clean`

Run the` dbt clean` command. Deletes all folders specified in the clean-targets.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-clean
```

-----

### `dbt-compile`

Run the` dbt compile` command. Generates executable SQL from source model, test, and analysis files.

#### Arguments

`--global-flags`: Global dbt flags applicable to all subcommands. Instead of dash `-` please use `+`.</br>
`--cmd-flags`: Command-specific dbt flags. Instead of dash `-` please use `+`.</br>
`--model-prefix`: Prefix dbt selector, for selecting parents.</br>
`--model-postfix`: Postfix dbt selector, for selecting children.</br>
`--models`: pre-commit-dbt is by default running changed files. If you need to override that, e.g. in case of Slim CI (`state:modified`), you can use this option.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-compile
   args: ["--model-prefix". "+", "--"]
```

or

```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-compile
   args: ["--models", "state:modified", "--cmd-flags", "++defer", "++state", "path/to/artifacts", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

-----

### `dbt-deps`

Run `dbt deps` command. Pulls the most recent version of the dependencies listed in your packages.yml.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-deps
```

-----
### `dbt-docs-generate`

Run `dbt docs generate` command. The command is responsible for generating your project's documentation website.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-docs-generate
```

-----

### `dbt-run`

Run `dbt run` command. Executes compiled SQL model files.

#### Arguments

`--global-flags`: Global dbt flags applicable to all subcommands. Instead of dash `-` please use `+`.</br>
`--cmd-flags`: Command-specific dbt flags. Instead of dash `-` please use `+`.</br>
`--model-prefix`: Prefix dbt selector, for selecting parents.</br>
`--model-postfix`: Postfix dbt selector, for selecting children.</br>
`--models`: pre-commit-dbt is by default running changed files. If you need to override that, e.g. in case of Slim CI (`state:modified`), you can use this option.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-run
   args: ["--model-prefix", "+", "--"]
```

or

```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-run
   args: ["--models", "state:modified", "--cmd-flags", "++defer", "++state", "path/to/artifacts", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

-----
### `dbt-test`

Run `dbt test` command. Runs tests on data in deployed models.

#### Arguments

`--global-flags`: Global dbt flags applicable to all subcommands. Instead of dash `-` please use `+`.</br>
`--cmd-flags`: Command-specific dbt flags. Instead of dash `-` please use `+`.</br>
`--model-prefix`: Prefix dbt selector, for selecting parents.</br>
`--model-postfix`: Postfix dbt selector, for selecting children.
`--models`: pre-commit-dbt is by default running changed files. If you need to override that, e.g. in case of Slim CI (`state:modified`), you can use this option.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-test
   args: ["--model-prefix", "+", "--"]
```

or

```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-test
   args: ["--models", "state:modified", "--cmd-flags", "++defer", "++state", "path/to/artifacts", "--"]
```

:warning: do not forget to include `--` as the last argument. Otherwise `pre-commit` would not be able to separate a list of files with args.

-----
