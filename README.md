<p align="center">
  <img src=".github/pre-commit-dbt.png" alt="dbt-pre-commit" width=600/>
  <h1 align="center">pre-commit-dbt</h1>
</p>
<p align="center">
  <a href="https://github.com/psf/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="black"/>
  </a>
  <a href="https://github.com/pre-commit/pre-commit">
    <img src="https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white" alt="black"/>
  </a>
</p>

Hooks that make your work with `dbt` much easier.

For pre-commit see: https://github.com/pre-commit/pre-commit

## List of pre-commit-dbt hooks

**Model checks:**
 * [`check-column-desc-are-same`](#check-column-desc-are-same): Check column descriptions are same
 * [`check-model-columns-have-desc`](#check-model-columns-have-desc): Check the model columns have description
 * [`check-model-has-all-columns`](#check-model-has-all-columns): Check the model has all columns in properties file
 * [`check-model-has-description`](#check-model-has-description): Check the model has description
 * [`check-model-has-meta-keys`](#check-model-has-meta-keys): Check the model has keys in the meta part
 * [`check-model-has-properties-file`](#check-model-has-properties-file): Check the model has properties file
 * [`check-model-has-tests-by-name`](#check-model-has-tests-by-name): Check the model has a number of tests by test name
 * [`check-model-has-tests-by-type`](#check-model-has-tests-by-type): Check the model has a number of tests by test type
 * [`check-model-has-tests`](#check-model-has-tests): Check the model has a number of tests
 * [`check-model-tags`](#check-model-tags): Check the model has valid tags

**Script checks:**
 * [`check-script-semicolon`](#check-script-semicolon): Check the script does not contain a semicolon
 * [`check-script-has-no-table-name`](#check-script-has-no-table-name)
 * [`check-script-ref-and-source`](#check-script-ref-and-source)

**Source checks:**
 * [`check-source-columns-have-desc`](#check-source-columns-have-desc): Check for source column descriptions
 * [`check-source-has-all-columns`](#check-source-has-all-columns): Check the source has all columns in the properties file
 * [`check-source-has-description`](#check-source-has-description): Check the source has description
 * [`check-source-has-freshness`](#check-source-has-freshness): Check the source has the freshness
 * [`check-source-has-loader`](#check-source-has-loader): Check the source has loader option
 * [`check-source-has-meta-keys`](#check-source-has-meta-keys): Check the source has keys in the meta part
 * [`check-source-has-tests-by-name`](#check-source-has-tests-by-name): Check the source has a number of tests by test name
 * [`check-source-has-tests-by-type`](#check-source-has-tests-by-type): Check the source has a number of tests by test type
 * [`check-source-has-tests`](#check-source-has-tests): Check the source has a number of tests
 * [`check-source-tags`](#check-source-tags): Check the source has valid tags

Modifiers:
 * [`generate-missing-sources`](#generate-missing-sources)
 * [`generate-model-properties-file`](#generate-model-properties-file)
 * [`replace-column-description`](#replace-column-description)
 * [`replace-script-table-names`](#replace-script-table-names)
 * [`replace-semicolon`](#replace-semicolon)

dbt commands:
 * [`dbt-clean`](#dbt-clean)
 * [`dbt-compile`](#dbt-compile)
 * [`dbt-deps`](#dbt-deps)
 * [`dbt-docs-generate`](#dbt-docs-generate)
 * [`dbt-run`](#dbt-run)
 * [`dbt-test`](#dbt-test)




## Available Hooks
### `check-column-desc-are-same`

Check the models have the same descriptions for the same column names.

#### Arguments

`--ignore`: columns for which do not check whether have a different description.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
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
### `check-model-columns-have-desc`

Ensures that the model has columns with descriptions in the properties file (usually `schema.yml`).

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
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
 rev: v0.1.0
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
 rev: v0.1.0
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
 rev: v0.1.0
 hooks:
 - id: check-model-has-meta-keys
 args: ['--meta-keys', 'foo', 'bar']
```

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
 rev: v0.1.0
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
 rev: v0.1.0
 hooks:
 - id: check-model-has-tests-by-type
 args: ["--tests", "unique=1", "data=1"]
```

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
 rev: v0.1.0
 hooks:
 - id: check-model-has-tests-by-name
 args: ["--tests", "schema=1", "data=1"]
```

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

### `check-model-has-tests`

Ensures that the model has a number of tests.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tests`: Minimum number of tests required.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
 hooks:
 - id: check-model-has-tests
 args: ["--tests", 2]
```

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

### `check-model-tags`

Ensures that the model has only valid tags from the provided list.

#### Arguments

`--manifest`: location of `manifest.json` file. Usually `target/manifest.json`. This file contains a full representation of dbt project. **Default: `target/manifest.json`**<br/>
`--tags`: A list of tags that models can have.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
 hooks:
 - id: check-model-tags
 args: ["--tags", "foo", "bar"]
```

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

### `check-script-semicolon`

Ensure that the script does not have a semicolon at the end of the file.

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
 hooks:
 - id: check-script-semicolon
```

#### When to use it

Make sure you did not provide a semicolon at the end of the file.
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
### `check-source-columns-have-desc`

Ensures that the source has columns with descriptions in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
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
 rev: v0.1.0
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
### `check-source-has-description`

Ensures that the source has a description in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
 hooks:
 - id: check-source-has-description
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
- If the source does not have a description, the hook fails.

-----
### `check-source-has-freshness`

Ensures that the source has freshness options in the properties file (usually `schema.yml`).

#### Example
```
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v0.1.0
 hooks:
 - id: check-source-has-freshness
 args: ["--freshness", "error_after", "warn_after"]
```

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
 rev: v0.1.0
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
 rev: v0.1.0
 hooks:
 - id: check-source-has-meta-keys
 args: ['--meta-keys', 'foo', 'bar']
```

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
 rev: v0.1.0
 hooks:
 - id: check-source-has-tests-by-type
 args: ["--tests", "unique=1", "data=1"]
```

#### When to use it

You want to make sure that every source has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

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
 rev: v0.1.0
 hooks:
 - id: check-source-has-tests-by-name
 args: ["--tests", "schema=1", "data=1"]
```

#### When to use it

You want to make sure that every source has certain tests.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

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
 rev: v0.1.0
 hooks:
 - id: check-source-has-tests
 args: ["--tests", 2]
```

#### When to use it

You want to make sure that every source was tested.
#### Requirements

| Model exists in `manifest.json` <sup id="a1">[1](#f1)</sup> | Model exists in `catalog.json` <sup id="a2">[2](#f2)</sup> |
| :----: | :----------: |
| :x: Not needed | :x: Not needed |

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
 rev: v0.1.0
 hooks:
 - id: check-source-tags
 args: ["--tags", "foo", "bar"]
```

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