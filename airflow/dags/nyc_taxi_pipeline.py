from airflow.sdk import dag, task, Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from pendulum import datetime

@dag(
    dag_id="nyc_taxi_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False
)
def nyc_taxi():

    bronze = DatabricksRunNowOperator(
        task_id="bronze",
        databricks_conn_id="databricks_default",
        job_id=751828852057469,
    )

    silver = DatabricksRunNowOperator(
        task_id="silver",
        databricks_conn_id="databricks_default",
        job_id=848138649009218,
    )

    @task.bash
    def gold():
        token = Variable.get("databricks_token")
        project_dir = "/opt/airflow/include/nyc_gold"
        return (
            f"export DBT_DATABRICKS_TOKEN='{token}' && "
            f"cd {project_dir} && "
            f"dbt build --profiles-dir ."
        )

    bronze >> silver >> gold()
    
nyc_taxi()