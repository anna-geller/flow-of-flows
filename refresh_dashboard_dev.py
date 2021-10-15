import prefect
from prefect import task, Flow
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

FLOW_NAME = "refresh_dashboard_dev"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def update_customers_dashboards():
    logger = prefect.context.get("logger")
    # your logic here
    logger.info("Customers dashboard extracts updated!")


@task
def update_sales_dashboards():
    logger = prefect.context.get("logger")
    # your logic here
    logger.info("Sales dashboard extracts updated!")


with Flow(FLOW_NAME, storage=STORAGE, run_config=DockerRun(image="elt:latest")) as flow:
    customers = update_customers_dashboards()
    sales = update_sales_dashboards()
    customers.set_downstream(sales)
