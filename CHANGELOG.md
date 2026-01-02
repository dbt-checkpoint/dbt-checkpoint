# 2.0.7 - 2025-05-08

**Fixes:**
* fix: Perform a final filter to only file paths in the output by @dbenzion in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/274
* Fix false positives in SQL table name detection by @janposlusny in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/291

## New Contributors
* @dbenzion made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/274
* @janposlusny made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/291

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.6...v2.0.7

# 2.0.6 - 2024-11-13

**Fixes:**
* Fix #259: error discovering property file by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/262
* Fix check_model_has_description missing-filepath sql detection by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/269

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.5...v2.0.6

# 2.0.5 - 2024-10-25

**Breaking Changes:**
* Deprecate Python 3.7 by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/263

**Fixes:**
* Fix #185: --exclude usage docs by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/257
* Fix #260: Add check-model-parents-name-prefix to setup.cfg and fix docs by @lucaslortiz in  https://github.com/dbt-checkpoint/dbt-checkpoint/pull/261

**Enhancements:**
* check-model-name-contract can use manifest.json by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/265
* Feat-2974: ref-and-source support for Seeds and Snapshots by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/266

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.4...v2.0.5

# 2.0.4 - 2024-09-20

**Enhancements:**

- check-script-ref-and-source support for versioned models by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/250


**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.3...v2.0.4

# 2.0.3 - 2024-07-23

**Fixes:**

- yaml safe_loader bug on empty YML files by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/240

**Enhancements:**

- Create hook that checks for database and schema casing consistency by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/235

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.2...v2.0.3

# 2.0.2 - 2024-07-04

**Fixes:**

- Fix some typos in README.md by @Lapkonium in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/217
- Fix Macro arguments schema compliance by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/223

**Enhancements:**

- model has contract and model has constraints by @xasm83 in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/218

**New Contributors**

- @Lapkonium made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/217
- @xasm83 made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/218

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v2.0.1...v2.0.2

# 2.0.1 - 2024-04-19

**Fixes:**

- Fix/dbt parse config by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/213

# 2.0.0 - 2024-04-12

> :warning: **dbt 1.5**: Starting at 2.0.0, certain hooks (like `dbt-parse`) can fail if using dbt-core < 1.5

**Fixes:**

- Bugfix-192: Applied lower() to schema_cols by @ronak-datatonic in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/197
- Documentation Update: Changed Hooks to reflect correct usage of args for source-has-tests by @Thomas-George-T in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/200

**Enhancements:**

- Adding dbt parse hook by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/195
- Fix support model versions by @gbrunois in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/199
- All hook names <= 50 characters by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/209
- Expand testing to python 3.10 - 3.12 by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/207
- Update user visible messages in check_model_parents_and_childs.py by @awal11 in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/204
- Bump action versions by @pgoslatara in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/208
- Describe a faster build configuration in the example by @awal11 in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/211

## New Contributors

- @pgoslatara made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/195
- @ronak-datatonic made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/197
- @Thomas-George-T made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/200
- @gbrunois made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/199
- @awal11 made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/204

# 1.2.1 - 2024-03-07

**Fixes:**

- Fix GitHub action by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/187

**Enhancements:**

- Allow multiple data types, improve error messages by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/180
- Support yml suffix (without A) in dbt-checkpoint config file by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/190
- implement 'check has meta keys' for all meta-supported dbt objects by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/181
- Add --allow-extra-keys to check-source-has-meta-keys by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/176

# 1.2.0 - 2024-01-15

**Fixes:**

- Grammar by @carlthome in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/150
- chore: update README to reflect change to Datatonic by @alfredodimassimo in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/162
- update version by @timwinter06 in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/168
- Fix check-model-has-properties-file for snapshots by @ms32035 in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/137

**Enhancements:**

- Make check_column_name_contract case insensitive for pattern and datatype by @samkessaram in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/167
- feature #136 (create a check-model-has-labels-keys for dbt with BigQuery) by @johnerick-py in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/147
- Feat/check materialization by childs by @LeopoldGabelmann in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/144
- Ignore disabled models/sources by default, unless --include-disabled is passed by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/171

## New Contributors

- @carlthome made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/150
- @alfredodimassimo made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/162
- @samkessaram made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/167
- @johnerick-py made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/147
- @LeopoldGabelmann made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/144
- @timwinter06 made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/168
- @ms32035 made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/137

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v1.1.1...v1.2.0

# 1.1.1 - 2023-07-10

**Fixes:**

- Nitpick: Fix `parent` typo by @followingell in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/111
- fix: typo in track message by @JFrackson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/122
- fix: use proper tracking config file and document in README by @JFrackson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/115
- Fix/support excluding files at hook level by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/119
- Add .dbt-checkpoint.yaml to disable hook execution tracking in mixpanel by @mbhoopathy in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/139

**Enhancements:**

- Use None instead of NoReturn by @ian-r-rose in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/113
- Update sponsorship logos to support dark theme on Github by @noel in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/130
- Dcv 1682 possibility to change the dbt root for all hooks by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/138
- Create per-hook excluding README entry by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/140

## New Contributors

- @followingell made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/111
- @ian-r-rose made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/113
- @noel made their first contribution in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/130

**Full Changelog**: https://github.com/dbt-checkpoint/dbt-checkpoint/compare/v1.1.0...v1.1.1

# 1.1.0 - 2023-03-15

**Fixed bugs:**

- Fix some tiny typos by @slve in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/37
- Fix Github Action docker reference by @tlfbrito in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/49
- Fix devcontainer in codespaces by @tomsej in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/55
- Fixed typo in code example of `dbt-compile` by @karabulute in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/84
- [Fix] Limit check-source-has-tests-by-group hook to yaml files by @katieclaiborne in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/100
- fix: accommodate whitespaces for no table name check by @JFrackson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/93
- fix: accommodate filter in src freshness by @JFrackson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/95

**Enhancements:**

- Feat/add hook check macro ags have descr by @PabloPardoGarcia in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/25
- Feat/add hook check column name contract by @PabloPardoGarcia in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/27
- Add check-source-childs hook to .pre-commit-hooks by @landier in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/33
- Add check-model-name-contract hook by @stumelius in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/32
- Add devcontainer by @tomsej in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/40
- missing check-model-parents-schema by @miki-lwy in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/52
- update regex_comments string in check-script-has-no-table-name by @neddonaldson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/47
- add argument support to dbt commands (dbt clean & dbt deps) by @Aostojic in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/53
- Features and fixes by Datacoves by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/81
- Add check-model-parents-database to hooks by @ktuft-cbh in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/66
- feat/fernandobrito: allow extra meta keys by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/83
- Optionally allow extra keys in check-model-has-meta-keys by @fernandobrito in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/65
- Update .pre-commit-config.yaml by @ssassi in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/89
- Add check source has tests by group by @ashleemtib in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/91
- feat: add telemetry with mixpanel sdk by @JFrackson in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/97
- improve `get missing filepaths` utils logic by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/105
- Change all pre-commit-dbt references to dbt-checkpoint by @BAntonellini in https://github.com/dbt-checkpoint/dbt-checkpoint/pull/107

  # 1.0.0 - 2021-04-16

**Fixed bugs:**

- check-model-has-properties-file fails on macro with a valid properties yml [\#17](https://github.com/offbi/pre-commit-dbt/issues/17)
- check-script-has-no-table-name fails with subqueries referencing CTEs [\#16](https://github.com/offbi/pre-commit-dbt/issues/16)
- check-model-has-description fails with macros with description [\#14](https://github.com/offbi/pre-commit-dbt/issues/14)
- check-script-has-no-table-name doesn't ignore text within Jinja comment blocks [\#9](https://github.com/offbi/pre-commit-dbt/issues/9)
- check-column-desc-are-same fails with a Python error [\#8](https://github.com/offbi/pre-commit-dbt/issues/8)
- check-script-has-no-table-name is failing when using lateral flatten [\#7](https://github.com/offbi/pre-commit-dbt/issues/7)

**Enhancements:**

- Feature/allow slim ci [\#23](https://github.com/offbi/pre-commit-dbt/pull/23) ([tomsej](https://github.com/tomsej))
- New hook check-macro-has-description [\#20](https://github.com/offbi/pre-commit-dbt/pull/20) ([PabloPardoGarcia](https://github.com/PabloPardoGarcia))
- Fix problem when space is missing with parenthesis [\#19](https://github.com/offbi/pre-commit-dbt/pull/19) ([tomsej](https://github.com/tomsej))
- Don't check macros in check-model hooks [\#18](https://github.com/offbi/pre-commit-dbt/pull/18) ([PabloPardoGarcia](https://github.com/PabloPardoGarcia))
- Feature/check model parents and childs [\#13](https://github.com/offbi/pre-commit-dbt/pull/13) ([tomsej](https://github.com/tomsej))
- Replace jinja comments and allow flatten [\#11](https://github.com/offbi/pre-commit-dbt/pull/11) ([tomsej](https://github.com/tomsej))
- Validate model is a dictionary [\#10](https://github.com/offbi/pre-commit-dbt/pull/10) ([tomsej](https://github.com/tomsej))
- Added hook: check_model_has_tests_by_group [\#6](https://github.com/offbi/pre-commit-dbt/pull/6) ([jtalmi](https://github.com/jtalmi))
- Added hook: check-model-parents-and-childs
- Added hook: check-model-parents-database
- Added hook: check-model-parents-schema
- Added hook: check-source-childs

  # 0.1.1 - 2021-02-08

- Fixed problem with dashes in global and cmd flags. Use plus sign instead.
- Fixed problem with greedy replace script table name.
- Documentation fixes.

  # 0.1.0 - 2020-12-23

- First written
