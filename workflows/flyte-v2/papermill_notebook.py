# For å bruke pandas dataframe som output fra en flyte task må pyarrow og fastparquet være installert i miljøet
from pathlib import Path
import flyte
from flytekit import Resources, ImageSpec, kwtypes
from flytekitplugins.papermill import NotebookTask
import pandas as pd

SECRET_NAME = "oracle-password"

env = flyte.TaskEnvironment(
    name="nb_env",
    secrets=[
        flyte.Secret(key=SECRET_NAME, as_env_var="DBT_ORACLE_PASSWORD"),
    ],
    image=flyte.Image.from_debian_base(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    ).with_pip_packages(
        "papermill",
        "nbformat",
        "numpy",
        "pandas",
        "oracledb",
        "sqlalchemy",
        "google-cloud-secret-manager",
        "flytekitplugins-papermill",
        "fastparquet",
        "pyarrow",
    ).with_source_folder(
      src=Path("data/papermill"),
      dst="./data/papermill",
    )
)


nb = NotebookTask(
    name="simple-nb",
    notebook_path="data/papermill/notebook.ipynb",
    inputs=kwtypes(table_name=str),
    outputs=kwtypes(df=pd.DataFrame),
    requests=Resources(cpu="500m", mem="1Gi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="1", mem="2Gi", ephemeral_storage="100Mi"),
)


@env.task
def print_table_content(df: pd.DataFrame) -> pd.DataFrame:
  print(df.head())
  return df


@env.task
def papermill_notebook() -> pd.DataFrame:
  out = nb(table_name="badetemperaturer")
  print_table_content(df=out.df)
  return out.df


if __name__ == "__main__":
  print(f"Running produce_quarto() {papermill_notebook()}")
