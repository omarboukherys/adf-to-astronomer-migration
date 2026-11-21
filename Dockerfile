FROM astrocrpublic.azurecr.io/runtime:3.3-12

# dbt lives in its own virtual environment, so its dependencies never conflict with Airflow's.
# Cosmos calls it through ExecutionConfig(dbt_executable_path=".../dbt_venv/bin/dbt").
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-snowflake==1.12.5
