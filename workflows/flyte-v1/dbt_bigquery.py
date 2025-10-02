# Laget med utgangspunkt i https://docs.flyte.org/en/latest/flytesnacks/examples/dbt_plugin/dbt_example.html
# Eksempelet under forutsetter at google service accounten som brukes i taskene er gitt BigQuery Job User rolle i prosjektet
# samt BigQuery Admin på datasettet som brukes i dbt-prosjektet.

from typing import Tuple
from flytekit import workflow, Resources
from flytekit import ImageSpec
from flytekitplugins.dbt.schema import (
    DBTRunInput,
    DBTRunOutput,
    DBTTestInput,
    DBTTestOutput,
)
from flytekitplugins.dbt.task import DBTRun, DBTTest


dbt_run_task = DBTRun(
    name="run-dbt",
    container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        packages=[
            "dbt-bigquery==1.7.9",
            "flytekitplugins-dbt==1.16.3",
        ],
        copy=["data/dbt_bigquery"],
    ),
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)

dbt_test_task = DBTTest(
    name="test-dbt",
    container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        packages=[
            "dbt-bigquery==1.7.9",
            "flytekitplugins-dbt==1.16.3",
        ],
        commands=["cd /root/data/dbt_bigquery/nada_dbt_test && dbt deps"],
        copy=["data/dbt_bigquery"],
    ),
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)


@workflow
def dbt_bigquery()-> Tuple[DBTRunOutput, DBTTestOutput]:
    dbt_run_output = dbt_run_task(
        input=DBTRunInput(
            project_dir="data/dbt_bigquery/nada_dbt_test",
            profiles_dir="data/dbt_bigquery/nada_dbt_test",
            profile="nada_dbt_test",
        ),
    )

    dbt_test_output = dbt_test_task(
        input=DBTRunInput(
            project_dir="data/dbt_bigquery/nada_dbt_test",
            profiles_dir="data/dbt_bigquery/nada_dbt_test",
            profile="nada_dbt_test",
        ),
    )

    dbt_run_output >> dbt_test_output

    return dbt_run_output, dbt_test_output


if __name__ == "__main__":
    print(f"Kjører wf", dbt_bigquery())
