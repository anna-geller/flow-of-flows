from prefect import Flow, task
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect.client.secrets import Secret
from flow_utilities.db_utils import get_db_connection_string


FLOW_NAME = "k8s_flow_demo_ecr"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    path=f"{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task(log_stdout=True)
def hello_world():
    text = f"hello from {FLOW_NAME}"
    print(text)
    return text


with Flow(
        FLOW_NAME,
        storage=STORAGE,
        run_config=KubernetesRun(
            image="338306982838.dkr.ecr.eu-central-1.amazonaws.com/community:latest",
            labels=["k8s"],
            image_pull_secrets=["aws-ecr-secret"]
            # env={
            #     "AWS_ACCESS_KEY_ID": Secret("AWS_ACCESS_KEY_ID").get(),
            #     "AWS_SECRET_ACCESS_KEY": Secret("AWS_SECRET_ACCESS_KEY").get(),
            #     "AWS_DEFAULT_REGION": Secret("AWS_DEFAULT_REGION").get(),
            # },
        ),
) as flow:
    hw = hello_world()

if __name__ == "__main__":
    flow.register(project_name="community")
