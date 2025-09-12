import subprocess

from flytekit import FlyteDirectory, task, Resources, ImageSpec


@task(
    container_image=ImageSpec(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
        packages=[
            "dbt-oracle==1.7.7",
            "flytekitplugins-dbt==1.16.3",
        ],
    ),
    requests=Resources(cpu="200m", mem="500Mi", ephemeral_storage="100Mi"),
    limits=Resources(cpu="200m", mem="1Gi", ephemeral_storage="100Mi"),
)
def dbt_run(dbt_dir: FlyteDirectory, profile: str) -> str:
    dbt_dir.download()

    output = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            f"{dbt_dir.path}",
            "--profiles-dir",
            f"{dbt_dir.path}",
            "--profile",
            profile
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return output.stdout
