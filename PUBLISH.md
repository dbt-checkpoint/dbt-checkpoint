# Publishing

Tutorial of publishing to help me not to forgot to anything :D

## 0. Bump new version

- setup.cfg
- **init**.py

## 1. Run tests

Run pytests:

```
pytest -vvv --cov=dbt_checkpoint --cov-config=setup.cfg --cov-report=term-missing --cov-report=html
```

Run pre-commit:

```
pre-commit run --all-files
```

Try import:

```
pre-commit try-repo .
```

## 2. Docker

Bump version in Dockerfile - .github/.pre-commit-config-action.yaml

Build:

```
docker build . -t dbtcheckpoint/dbt-checkpoint
docker tag dbtcheckpoint/dbt-checkpoint:latest dbtcheckpoint/dbt-checkpoint:<version>
```

Test:

```
docker run dbtcheckpoint/dbt-checkpoint
```

Publish to Docker Hub

```
docker push dbtcheckpoint/dbt-checkpoint
docker push dbtcheckpoint/dbt-checkpoint:<version>
```

## 3. Github Action

Bump docker version in action.yml

## 4. Write CHANGELOG

## 5. Push to Github

## 6. Create new Github deployment
