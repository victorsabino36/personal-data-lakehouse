FROM python:3.11-slim

LABEL maintainer="Victor Sabino"
LABEL description="DBT Project Container for Cloud Run Execution"

RUN pip install dbt-bigquery

WORKDIR /usr/app

# Copia da subpasta lakehouse_models
COPY lakehouse_models/ /usr/app/

ENTRYPOINT [ "dbt" ]