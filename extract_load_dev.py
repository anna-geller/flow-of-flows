from flow_utilities.db import load_df_to_db
import pandas as pd
import prefect
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import DockerRun
from prefect.storage import GitHub


FLOW_NAME = "extract_load_dev"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def extract_and_load(dataset: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    load_df_to_db(df, dataset)
    logger.info("Dataset %s with %d rows loaded to DB", dataset, len(df))


with Flow(
    FLOW_NAME,
    executor=LocalDaskExecutor(),
    storage=STORAGE,
    run_config=DockerRun(image="elt:latest"),
) as flow:
    datasets = ["raw_customers", "raw_orders", "raw_payments"]
    dataframes = extract_and_load.map(datasets)
