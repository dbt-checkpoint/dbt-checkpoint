FROM python:3.8-slim

WORKDIR /work
COPY .github/.pre-commit-config-action.yaml .pre-commit-config.yaml

RUN apt-get update &&\
    apt-get upgrade -y && \
    apt-get install -y git && \
    pip install --no-cache-dir pre-commit==3.1.1 \
    dbt-core==1.3.3 && \
    git init && \
    pre-commit install-hooks && \
    apt-get clean autoclean && \
    apt-get autoremove --yes  && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    rm -rf /work

WORKDIR /github/workspace

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
