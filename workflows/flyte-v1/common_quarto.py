from flytekit import FlyteDirectory, ImageSpec, Deck, Resources, task


image_spec = ImageSpec(
    registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    packages=[
        "knatch==1.1.0",
        "google-cloud-secret-manager==2.24.0",
    ]
)


@task(enable_deck=True, deck_fields=[], container_image=ImageSpec(
    registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    packages=[
        "flytekitplugins-deck-standard==1.16.0",
        "google-cloud-storage==3.4.0",
    ],
  ),
  requests=Resources(cpu="1", mem="500Mi", ephemeral_storage="100Mi"),
  limits=Resources(cpu="1", mem="800Mi", ephemeral_storage="100Mi"),
)
def quarto_preview(quarto_dir: FlyteDirectory) -> str:
    import json
    from urllib.parse import urlparse
    from google.cloud.storage import Client as GCSClient

    intended_audience = ["all"]

    parsed_url = urlparse(quarto_dir.remote_source)
    bucket_name = parsed_url.netloc
    blob_name = parsed_url.path[1:]

    gcs_client = GCSClient()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name+"/intended_audience.json")
    with blob.open("w") as f:
        f.write(json.dumps(intended_audience))

    preview_path = f"""https://quarto-previewer.dev.knada.io/{"/".join(quarto_dir.remote_source.split("/")[3:])}"""
    deck = Deck("Quarto Preview", html=f"""
      <p>Trykk <a href="{preview_path}" target="_blank">her</a> for å se forhåndsvisningen av datafortellingen i egen fane</p>
    """)

    deck.append(
       f"""<embed src="{preview_path}" width=100% style="height: 120rem"/>"""
    )
    return f"""Se preview av datafortelling på {preview_path}"""


@task(container_image=image_spec)
def quarto_publish(directory: FlyteDirectory, quarto_id: str, quarto_token_gsm_fqn: str, host: str="data.ekstern.dev.nav.no") -> str:
    from knatch import batch_upload_quarto
    from google.cloud.secretmanager import SecretManagerServiceClient

    if host not in ["data.nav.no", "data.ekstern.dev.nav.no"]:
        raise ValueError("Host must be either 'data.nav.no' or 'data.ekstern.dev.nav.no'")

    client = SecretManagerServiceClient()
    secret = client.access_secret_version(name=quarto_token_gsm_fqn)
    team_token = secret.payload.data.decode('UTF-8')

    directory.download()

    batch_upload_quarto(
        quarto_id=quarto_id,
        folder=directory.path,
        team_token=team_token,
        host=host,
    )

    return f"""Se quarto på https://{"data.ansatt.nav.no" if host == "data.nav.no" else "data.ansatt.dev.nav.no"}/quarto/{quarto_id}"""
