# Project IAM:
# BigQuery Read Session User
# BigQuery Job User
# Dataset IAM:
# BigQuery Admin

import pandas as pd
import oracledb
import sqlalchemy
import os
import sys

from flytekit import task, workflow, ImageSpec, Secret
from flytekit.types.structured import StructuredDataset


ORACLE_TABLE_NAME = "SYSTEM.BADETEMPERATURER"
BQ_TABLE_URI = "nada-dev-db2e:unionai.structured_dataset"


image = ImageSpec(
  registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
  packages=[
    "pandas",
    "pandas_gbq",
    "flytekitplugins-bigquery",
    "fastparquet",
    "pyarrow",
    "oracledb",
    "sqlalchemy",
  ],
)


@task(
      container_image=image, 
      secret_requests=[Secret(group="oracle-password", key="oracle-password", env_var="DBT_ORACLE_PASSWORD")],
)
def oracle_to_bq(oracle_table_uri: str, bq_table_uri: str) -> StructuredDataset:
    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb

    engine = sqlalchemy.create_engine(f"oracle://system:{os.getenv('DBT_ORACLE_PASSWORD')}@incluster-oracledb.incluster-oracledb.svc.cluster.local:1521/?service_name=FREEPDB1")

    with engine.connect() as con:
      df = pd.read_sql_query(f"SELECT * FROM {oracle_table_uri}", con)

    return StructuredDataset(dataframe=df, uri=f"bq://{bq_table_uri}")


@task(container_image=image)
def bq_to_pandas(table_uri: str) -> pd.DataFrame:
   sd = StructuredDataset(uri=f"bq://{table_uri}")
   df = sd.open(pd.DataFrame).all()
   print(df)
   return df


@workflow
def structured_dataset_bigquery() -> str:
    sd = oracle_to_bq(ORACLE_TABLE_NAME, BQ_TABLE_URI)
    df = bq_to_pandas(BQ_TABLE_URI)

    sd >> df

    return f"DataFrame number of rows: {df.shape[0]}"


if __name__ == "__main__":
    print(f"Running structured_dataset_bigquery() {structured_dataset_bigquery()}")
