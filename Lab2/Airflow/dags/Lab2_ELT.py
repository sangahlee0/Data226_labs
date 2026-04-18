"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator
Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


conn = BaseHook.get_connection('snowflake_con')
with DAG(
    "LAB_2_ELT",
    start_date=datetime(2026, 3, 2),
    description="Airflow DAG for Lab 2 ELT",
    schedule=None,
    catchup=False,
    
    # default_args={
    #     "env": {
    #         "DBT_USER": conn.login,
    #         "DBT_PASSWORD": conn.password,
    #         "DBT_ACCOUNT": conn.extra_dejson.get("account"),
    #         "DBT_SCHEMA": conn.schema,
    #         "DBT_DATABASE": conn.extra_dejson.get("database"),
    #         "DBT_ROLE": conn.extra_dejson.get("role"),
    #         "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
    #         "DBT_TYPE": "snowflake"
    #     }
    # },

    default_args={
    "env": {
        "DBT_USER": str(conn.login) if conn and conn.login else "",
        "DBT_PASSWORD": str(conn.password) if conn and conn.password else "",
        "DBT_ACCOUNT": str(conn.extra_dejson.get("account", "")) if conn else "",
        "DBT_SCHEMA": "analytics", # Hardcode this to match your profiles.yml
        "DBT_DATABASE": str(conn.extra_dejson.get("database", "")) if conn else "",
        "DBT_ROLE": str(conn.extra_dejson.get("role", "")) if conn else "",
        "DBT_WAREHOUSE": str(conn.extra_dejson.get("warehouse", "")) if conn else "",
        "DBT_TYPE": "snowflake"
    }
}
) as dag:
    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    # )

    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    # )

    # dbt_snapshot = BashOperator(
    #     task_id="dbt_snapshot",
    #     bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    # )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
    task_id="dbt_snapshot",
    bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )
    # print_env_var = BashOperator(
    #    task_id='print_aa_variable',
    #    bash_command='echo "The value of AA is: $DBT_ACCOUNT,$DBT_ROLE,$DBT_DATABASE,$DBT_WAREHOUSE,$DBT_USER,$DBT_TYPE,$DBT_SCHEMA"'
    # )

    dbt_run >> dbt_test >> dbt_snapshot
