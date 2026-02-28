FROM astrocrpublic.azurecr.io/runtime:3.1-13
RUN mkdir -p /usr/local/airflow/warehouse_ddb
ENV DBT_PROFILES_DIR=/usr/local/airflow