from typing import Tuple
from flytekit import workflow, Resources, ImageSpec, Secret
from flytekitplugins.dbt.schema import (
    DBTRunInput,
    DBTRunOutput,
    DBTTestInput,
    DBTTestOutput,
    DBTFreshnessInput,
    DBTFreshnessOutput,
)
from flytekitplugins.dbt.task import DBTRun, DBTTest, DBTFreshness

with open("requirements_dbt.txt") as f:
    packages = f.read().splitlines()

image_spec = ImageSpec(
    registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    packages=packages,
    copy=["data"],
)

SECRET_NAME = "oracle-password"
oracle_secret = Secret(
    group=SECRET_NAME,
    key=SECRET_NAME,
    mount_requirement=Secret.MountType.ENV_VAR,
)

dbt_run_task = DBTRun(
    name="run-tasken",
    container_image=image_spec,
    secret_requests=[oracle_secret],
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)

dbt_test_task = DBTTest(
    name="test-task",
    container_image=image_spec,
    secret_requests=[oracle_secret],
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)

dbt_freshness_task = DBTFreshness(
    name="freshness-task",
    container_image=image_spec,
    secret_requests=[oracle_secret],
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)


@workflow
def wf() -> Tuple[DBTRunOutput, DBTTestOutput, DBTFreshnessOutput]:
    dbt_run_output = dbt_run_task(
        input=DBTRunInput(
            project_dir="data/oracle_dbt",
            profiles_dir="data",
            profile="oracle_dbt",
        )
    )
    dbt_test_output = dbt_test_task(
        input=DBTTestInput(
            project_dir="data/oracle_dbt",
            profiles_dir="data",
            profile="oracle_dbt",
        )
    )
    dbt_freshness_output = dbt_freshness_task(
        input=DBTFreshnessInput(
            project_dir="data/oracle_dbt",
            profiles_dir="data",
            profile="oracle_dbt",
        )
    )

    dbt_run_output >> dbt_test_output
    dbt_test_output >> dbt_freshness_output

    return dbt_run_output, dbt_test_output, dbt_freshness_output


if __name__ == "__main__":
    print(f"Running wf() {wf()}")
