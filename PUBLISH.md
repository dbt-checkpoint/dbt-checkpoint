# Publishing

Tutorial of publishing to help me not to forgot to anything :D

## 0. Bump new version

- setup.cfg
- __init__.py

## 1. Run tests

Run pytests:

```
pytest -vvv --cov=pre_commit_dbt --cov-config=setup.cfg --cov-report=term-missing --cov-report=html
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
docker build . -t offbi/pre-commit-dbt
docker tag offbi/pre-commit-dbt:latest offbi/pre-commit-dbt:<version>
```

Test:

```
docker run offbi/pre-commit-dbt
```

Publish to Docker Hub

```
docker push offbi/pre-commit-dbt
docker push offbi/pre-commit-dbt:<version>
```

## 3. Github Action

Bump docker version in action.yml

## 4. Write CHANGELOG

## 5. Push to Github

## 6. Create new Github deployment
