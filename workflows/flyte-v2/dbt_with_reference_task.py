from typing import Tuple
from flytekit import workflow, reference_task, FlyteDirectory, ImageSpec, task


@task(container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        copy=["data/oracle_dbt"],
    ),
)
def prepare_dbt() -> Tuple[FlyteDirectory, str]:
    return FlyteDirectory("data/oracle_dbt"), "oracle_dbt"


@reference_task(
    name="common_dbt.dbt_run",
    project="nada",
    domain="development",
    version="v4",
)
def dbt_run(dbt_dir: FlyteDirectory, profile: str) -> str:
    ...


@workflow
def wf() -> str:
    dbt_dir, profile = prepare_dbt()
    dbt_run_output = dbt_run(
        dbt_dir=dbt_dir,
        profile=profile,
    )

    return dbt_run_output
