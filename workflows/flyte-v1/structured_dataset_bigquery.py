# Project IAM:
# BigQuery Read Session User
# BigQuery Job User
# Dataset IAM:
# BigQuery Admin

import pandas as pd

from flytekit import task, workflow, ImageSpec
from flytekit.types.structured import StructuredDataset


BQ_TABLE_URI = "nada-dev-db2e:unionai.structured_dataset"

image = ImageSpec(
  registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
  packages=[
    "pandas",
    "pandas_gbq",
    "flytekitplugins-bigquery",
    "fastparquet",
    "pyarrow",
  ],
)


@task(container_image=image)
def pandas_to_bq(table_uri: str) -> StructuredDataset:
    df = pd.DataFrame({"Name": ["Erik", "Another"], "Age": [36, 36]})
    return StructuredDataset(dataframe=df, uri=f"bq://{table_uri}")


@task(container_image=image)
def bq_to_pandas(table_uri: str) -> pd.DataFrame:
   sd = StructuredDataset(uri=f"bq://{table_uri}")
   df = sd.open(pd.DataFrame).all()
   print(df)
   return df


@workflow
def structured_dataset_bigquery() -> str:
    sd = pandas_to_bq(BQ_TABLE_URI)
    df = bq_to_pandas(BQ_TABLE_URI)

    sd >> df

    return f"DataFrame number of rows {df.shape[0]}"


if __name__ == "__main__":
    print(f"Running produce_quarto() {structured_dataset_bigquery()}")
