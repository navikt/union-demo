from datetime import timedelta
from flytekit import FlyteDirectory, Resources, ImageSpec, reference_task, task, workflow, wait_for_input, conditional


@task(container_image=ImageSpec(
      registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
      copy=["data/quarto"],
      packages=[
        "jupyterlab",
        "pandas",
        "plotly",
        "plotly-express",
        "numpy",
        "datastory",
        "quarto-cli",
      ],
    ),
    requests=Resources(cpu="1", mem="800Mi", ephemeral_storage="1Gi"),
    limits=Resources(cpu="1", mem="1Gi", ephemeral_storage="1Gi"),
)
def quarto_create() -> FlyteDirectory:
    import os
    os.system(f"quarto render data/quarto --execute --output-dir output")
    return FlyteDirectory(f"data/quarto/output")


@reference_task(
    project="nada",
    domain="development",
    name="common_quarto.quarto_preview",
    version="v5",
)
def quarto_preview(quarto_dir: FlyteDirectory
) -> str:
    ...


@reference_task(
    project="nada",
    domain="development",
    name="common_quarto.quarto_publish",
    version="v5",
)
def quarto_publish(directory: FlyteDirectory, 
                   quarto_id: str, 
                   quarto_token_gsm_fqn: str,
                   host: str="data.ekstern.dev.nav.no"
) -> str:
    ...


@workflow
def produce_quarto() -> str:
    quarto_content = quarto_create()
    preview_link = quarto_preview(
        quarto_dir=quarto_content,
    )

    publish_story = wait_for_input(
       "confirm_publish_story",
       timeout=timedelta(hours=2), 
       expected_type=bool
    )

    preview_link >> publish_story

    return (
       conditional("Publish?")
       .if_(publish_story.is_true())
       .then(
         quarto_publish(
             directory=quarto_content,
             quarto_id="bf48d8a4-05ca-47a5-a360-bc24171baf62",
             quarto_token_gsm_fqn="projects/270239535136/secrets/quarto-token-flyte/versions/latest",
             host="data.ekstern.dev.nav.no"
         )
       )
       .else_()
       .fail("Avbrutt av bruker")
    )

if __name__ == "__main__":
    print(f"Running produce_quarto() {produce_quarto()}")
