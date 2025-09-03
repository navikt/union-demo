import sys
import os
import flyte
import oracledb
import sqlalchemy
import pandas as pd

SECRET_NAME = "oracle-password"

env = flyte.TaskEnvironment(
    name="dbt_env",
    secrets=[
        flyte.Secret(key=SECRET_NAME, as_env_var="DBT_ORACLE_PASSWORD"),
    ],
    image=flyte.Image.from_debian_base(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    ).with_requirements(
        file="requirements_read_oracle.txt",
    )
)


@env.task
def read_oracle() -> str:
    print(f"Secret value: {os.getenv('DBT_ORACLE_PASSWORD')}")
    
    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb

    engine = sqlalchemy.create_engine(f"oracle://system:{os.getenv('DBT_ORACLE_PASSWORD')}@incluster-oracledb.incluster-oracledb.svc.cluster.local:1521/?service_name=FREEPDB1")
    oracle_table_name = "badetemperaturer"

    with engine.connect() as con:
      df = pd.read_sql_query(f"SELECT * FROM {oracle_table_name}", con)

    print(df.head())
    return "ok"


@env.task
def main_dbt() -> str:
    secret = read_oracle()
    return secret


if __name__ == "__main__":
    run = flyte.run(main_dbt)
