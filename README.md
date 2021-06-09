# What is pre-commit-dbt

![](.gitbook/assets/background.png)

`pre-commit-dbt` is a list of [pre-commit](https://pre-commit.com/) hooks to ensure the quality of your [dbt](https://www.getdbt.com/) projects.

This tool is built on a wonderful [pre-commit](https://pre-commit.com/) project that is useful for identifying simple issues before submission to code review. _By pointing these issues out before code review, allows a code reviewer to focus on the architecture of a change while not wasting time with trivial style nitpicks._

`dbt` is awesome, but when a number of models, sources, and macros grow it starts to be challenging to maintain quality. People often forget to update columns in schema files, add descriptions, or test. Besides, with the growing number of objects, `dbt` slows down, users stop running models/tests \(because they want to deploy the feature quickly\), and the demands on reviews increase.

If this is the case, `pre-commit-dbt` is here to help you!

