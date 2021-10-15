from flow_utilities.db import load_df_to_db
import pandas as pd
import prefect
from prefect import task, Flow
from prefect.storage import GitHub
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

FLOW_NAME = "extract_load_dbt_transform_dev"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def extract_and_load(dataset: str) -> None:
    logger = prefect.context.get("logger")
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    load_df_to_db(df, dataset)
    logger.info("Dataset %s with %d rows loaded to DB", dataset, len(df))


with Flow(FLOW_NAME, storage=STORAGE) as flow:
    extract_load_id = create_flow_run(
        flow_name="extract_load_dev",
        project_name="elt-dev",
        task_args={"name": "Staging"},
    )
    extract_load_wait_task = wait_for_flow_run(
        extract_load_id, task_args={"name": "Staging - wait"}
    )

    transform_id = create_flow_run(
        flow_name="dbt_dev", project_name="elt-dev", task_args={"name": "DBT flow"}
    )
    transform_id_wait_task = wait_for_flow_run(
        transform_id, task_args={"name": "DBT flow - wait"}
    )
    extract_load_wait_task.set_downstream(transform_id)

    dashboards = create_flow_run(
        flow_name="refresh_dashboard_dev",
        project_name="elt-dev",
        task_args={"name": "Dashboards"},
    )
    transform_id_wait_task.set_downstream(dashboards)
