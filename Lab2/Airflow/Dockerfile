FROM apache/airflow:2.10.1

USER airflow
RUN pip install --no-cache-dir yfinance
RUN pip install --no-cache-dir apache-airflow-providers-snowflake==5.7.0
RUN pip install --no-cache-dir snowflake-connector-python
RUN pip install --no-cache-dir dbt-snowflake==1.8.0