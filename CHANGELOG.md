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
