from prefect import Flow, unmapped
from prefect.tasks.prefect import create_flow_run
from prefect.executors import LocalDaskExecutor


with Flow("extract_load_parallel", executor=LocalDaskExecutor()) as flow:
    mapped_flows = create_flow_run.map(
        flow_name=["el_flow_1", "el_flow_2", "el_flow_3", "el_flow_4"],
        project_name=unmapped("Flow_of_Flows"),
    )
