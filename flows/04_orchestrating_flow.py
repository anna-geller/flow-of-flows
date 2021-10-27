from prefect import Flow
from prefect.storage import GitHub
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.run_configs import LocalRun

FLOW_NAME = "04_orchestrating_flow"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
PROJECT_NAME = "jaffle_shop"


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"])) as flow:
    extract_load_id = create_flow_run(
        flow_name="01_extract_load",
        project_name=PROJECT_NAME,
        task_args={"name": "Staging"},
    )
    extract_load_wait_task = wait_for_flow_run(
        extract_load_id, task_args={"name": "Staging - wait"}
    )

    transform_id = create_flow_run(
        flow_name="02_dbt", project_name=PROJECT_NAME, task_args={"name": "DBT flow"}
    )
    transform_id_wait_task = wait_for_flow_run(
        transform_id, task_args={"name": "DBT flow - wait"}
    )
    extract_load_wait_task.set_downstream(transform_id)

    dashboards_id = create_flow_run(
        flow_name="03_dashboards",
        project_name=PROJECT_NAME,
        task_args={"name": "Dashboards"},
    )
    dashboards_wait_task = wait_for_flow_run(
        dashboards_id, task_args={"name": "Dashboards - wait"}
    )
    transform_id_wait_task.set_downstream(dashboards_id)
