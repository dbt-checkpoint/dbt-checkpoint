1.0.0 - 2021-04-16
==================

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
- Added hook: check\_model\_has\_tests\_by\_group [\#6](https://github.com/offbi/pre-commit-dbt/pull/6) ([jtalmi](https://github.com/jtalmi))
- Added hook: check-model-parents-and-childs
- Added hook: check-model-parents-database
- Added hook: check-model-parents-schema
- Added hook: check-source-childs

0.1.1 - 2021-02-08
==================
- Fixed problem with dashes in global and cmd flags. Use plus sign instead.
- Fixed problem with greedy replace script table name.
- Documentation fixes.

0.1.0 - 2020-12-23
==================
- First written
