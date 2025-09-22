# For å bruke pandas dataframe som output fra en flyte task må pyarrow og fastparquet være installert i miljøet

from flytekit import workflow, Resources, ImageSpec, kwtypes, task
from flytekitplugins.papermill import NotebookTask
import pandas as pd


nb = NotebookTask(
    name="simple-nb",
    notebook_path="data/papermill/notebook.ipynb",
    container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        packages=[
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
        ],
        copy=["data/papermill/notebook.ipynb"],
    ),
    inputs=kwtypes(table_name=str),
    outputs=kwtypes(df=pd.DataFrame),
    requests=Resources(cpu="500m", mem="1Gi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="1", mem="2Gi", ephemeral_storage="100Mi"),
)


@task(
    container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        packages=[
            "pandas",
            "flytekitplugins-papermill",
            "fastparquet",
            "pyarrow",
        ],
    ),
)
def print_table_content(df: pd.DataFrame) -> pd.DataFrame:
  print(df.head())
  return df


@workflow
def papermill_notebook() -> pd.DataFrame:
  out = nb(table_name="badetemperaturer")
  print_table_content(df=out.df)
  return out.df


if __name__ == "__main__":
  print(f"Running produce_quarto() {papermill_notebook()}")
