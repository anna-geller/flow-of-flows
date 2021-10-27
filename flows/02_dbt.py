"""
This flow is largely inspired by https://medium.com/slateco-blog/prefect-orchestrating-dbt-10f3ca0baea9
"""
import prefect
from prefect import task, Flow, Parameter
from prefect.client import Secret
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.storage import GitHub
from prefect.triggers import all_finished
from prefect.run_configs import LocalRun
import pygit2
import shutil


DBT_PROJECT = "jaffle_shop"
FLOW_NAME = "02_dbt"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task(name="Clone DBT repo")
def pull_dbt_repo(repo_url: str, branch: str = None):
    pygit2.clone_repository(url=repo_url, path=DBT_PROJECT, checkout_branch=branch)


@task(name="Delete DBT folder if exists", trigger=all_finished)
def delete_dbt_folder_if_exists():
    shutil.rmtree(DBT_PROJECT, ignore_errors=True)


dbt = DbtShellTask(
    return_all=True,
    profile_name=DBT_PROJECT,
    profiles_dir="/Users/anna/.dbt",
    environment="dev",
    overwrite_profiles=True,
    log_stdout=True,
    helper_script=f"cd {DBT_PROJECT}",
    log_stderr=True,
    dbt_kwargs={
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "dbname": "postgres",
        "schema": DBT_PROJECT,
        "user": Secret("POSTGRES_USER").get(),
        "password": Secret("POSTGRES_PASS").get(),
        "threads": 4,
        "client_session_keep_alive": False,
    },
)


@task(trigger=all_finished)
def print_dbt_output(output):
    logger = prefect.context.get("logger")
    for line in output:
        logger.info(line)


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"])) as flow:
    del_task = delete_dbt_folder_if_exists()
    dbt_repo = Parameter(
        "dbt_repo_url", default="https://github.com/anna-geller/jaffle_shop"
    )
    dbt_repo_branch = Parameter("dbt_repo_branch", default=None)
    pull_task = pull_dbt_repo(dbt_repo, dbt_repo_branch)
    del_task.set_downstream(pull_task)

    dbt_run = dbt(command="dbt run", task_args={"name": "DBT Run"})
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT Run Output"})
    pull_task.set_downstream(dbt_run)

    dbt_test = dbt(command="dbt test", task_args={"name": "DBT Test"})
    dbt_test_out = print_dbt_output(dbt_test, task_args={"name": "DBT Test Output"})
    dbt_run.set_downstream(dbt_test)

    del_again = delete_dbt_folder_if_exists()
    dbt_test_out.set_downstream(del_again)

flow.set_reference_tasks([dbt_run])
