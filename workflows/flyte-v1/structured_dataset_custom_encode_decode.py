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
from flytekit.types.structured.structured_dataset import StructuredDatasetTransformerEngine
from custom_encoders.structured_dataset import BigQueryTableEncodingHandler, BigQueryTableDecodingHandler

ORACLE_TABLE_NAME = "SYSTEM.BADETEMPERATURER"
BQ_TABLE_URI = "nada-dev-db2e:unionai.structured_dataset2"


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

StructuredDatasetTransformerEngine.register(BigQueryTableEncodingHandler())
StructuredDatasetTransformerEngine.register(BigQueryTableDecodingHandler())


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
def bq_to_pandas(sd: StructuredDataset) -> pd.DataFrame:
   df = sd.open(pd.DataFrame).all()
   print(df.head())
   return df


@workflow
def structured_dataset_bigquery_custom_encode_decode() -> str:
    sd = oracle_to_bq(ORACLE_TABLE_NAME, BQ_TABLE_URI)
    df = bq_to_pandas(sd)

    return f"DataFrame number of rows: {df.shape[0]}"


if __name__ == "__main__":
    print(f"Running structured_dataset_bigquery_custom_encode_decode() {structured_dataset_bigquery_custom_encode_decode()}")
