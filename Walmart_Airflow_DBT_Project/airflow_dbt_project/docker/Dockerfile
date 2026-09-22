# ---------------------------------------------------------------------------
# Custom Airflow image extended with dbt + Databricks SDK tooling.
# ---------------------------------------------------------------------------
FROM apache/airflow:3.0.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# dbt project lives alongside the DAGs so Airflow tasks can shell out to it
COPY --chown=airflow:root dbt_project /opt/airflow/dbt_project

ENV DBT_PROFILES_DIR=/opt/airflow/dbt_project
