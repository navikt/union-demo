from flytekit import workflow, Resources
from flytekit import ImageSpec
from flytekitplugins.dbt.schema import (
    DBTRunInput,
    DBTRunOutput,
)
from flytekitplugins.dbt.task import DBTRun

dbt_run_task = DBTRun(
    name="run-task",
    container_image=ImageSpec(
      registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
      packages=[
          "dbt-oracle==1.7.7",
          "flytekitplugins-dbt==1.16.3",
      ],
      copy=["data"],
    ),
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)


@workflow
def run_dbt() -> DBTRunOutput:
    dbt_run_output = dbt_run_task(
        input=DBTRunInput(
            project_dir="data/oracle_dbt",
            profiles_dir="data/oracle_dbt",
            profile="oracle_dbt",
        ),
    )

    return dbt_run_output


if __name__ == "__main__":
    print(f"Running run_dbt() {run_dbt()}")
