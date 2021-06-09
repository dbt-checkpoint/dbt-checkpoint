# Github Action

Unfortunately, you cannot natively use `pre-commit-dbt` if you are using **dbt Cloud**. But you can run checks after you push changes into Github.

`pre-commit-dbt` for the most of the hooks needs `manifest.json` \(see requirements section in hook documentation\), that is in the `target` folder. Since this target folder is usually in `.gitignore`, you need to generate it. For that you need to run `dbt-compile` \(or `dbt-run`\) command. To be able to compile dbt, you also need [profiles.yml](https://docs.getdbt.com/dbt-cli/configure-your-profile) file with your credentials. **To provide passwords and secrets use Github Secrets** \(see example\).

So you want to e.g. run chach on number of tests:

```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: check-model-has-tests
   args: ["--test-cnt", "2", "--"]
```

To be able to run this in Github actions you need to modified it to:

```yaml
repos:
- repo: https://github.com/offbi/pre-commit-dbt
 rev: v1.0.0
 hooks:
 - id: dbt-compile
   args: ["--cmd-flags", "++profiles-dir", "."]
 - id: check-model-has-tests
   args: ["--test-cnt", "2", "--"]
```

#### Create profiles.yml

First step is to create [profiles.yml](https://docs.getdbt.com/dbt-cli/configure-your-profile). E.g.

```yaml
# example profiles.yml file
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: alice
      password: "{{ env_var('DB_PASSWORD') }}"
      port: 5432
      dbname: jaffle_shop
      schema: dbt_alice
      threads: 4
```

and store this file in project root `./profiles.yml`.

#### Create new workflow

* inside your Github repository create folder `.github/workflows` \(unless it already exists\).
* create new file e.g. `main.yml`
* specify your workflow e.g.:

```text
name: pre-commit

on:
  pull_request:
  push:
  branches: [main]

jobs:
  pre-commit:
  runs-on: ubuntu-latest
  steps:
  - uses: actions/checkout@v2
  - uses: actions/setup-python@v2
  - id: file_changes
    uses: trilom/file-changes-action@v1.2.4
    with:
      output: ' '
  - uses: offbi/pre-commit-dbt@v1.0.0
    env:
      DB_PASSWORD: ${{ secrets.SuperSecret }}
    with:
      args: run --files ${{ steps.file_changes.outputs.files}}
```

