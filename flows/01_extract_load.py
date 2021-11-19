from flow_utilities.db_utils import load_df_to_db
import pandas as pd
import prefect
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun
from prefect.storage import GitHub


FLOW_NAME = "01_extract_load"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def extract_and_load(dataset: str) -> None:
    logger = prefect.context.get("logger")
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    load_df_to_db(df, dataset)
    logger.info("Dataset %s with %d rows loaded to DB", dataset, len(df))


with Flow(
    FLOW_NAME,
    executor=LocalDaskExecutor(),
    storage=STORAGE,
    run_config=LocalRun(labels=["dev"]),
) as flow:
    datasets = ["raw_customers", "raw_orders", "raw_payments"]
    dataframes = extract_and_load.map(datasets)

flow.run()