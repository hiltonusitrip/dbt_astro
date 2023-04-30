import datetime
import pandas
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)



with DAG(
    dag_id="dbt",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 4, 4, tz="UTC"),
    catchup=False,
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 114474},
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )


    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=284418, # line 39
        check_interval=10,
        timeout=300,
    )

    trigger_dbt_cloud_job_run >> run_this_last
