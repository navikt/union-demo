from kubernetes.client import (
    V1Container,
    V1EnvVar,
    V1PodSpec,
)

import flyte

pod_template = flyte.PodTemplate(
    primary_container_name="main",
    labels={"test": "value"},
    annotations={"testannotation": "aValA"},
    pod_spec=V1PodSpec(
        containers=[
            V1Container(
                name="main",
                env=[V1EnvVar(name="TEAM", value="nada")]
            ),
            V1Container(
                name="sidecar",
                image="alpine:latest",
                command=["sh", "-c", "echo \"Hello from the sidecar\"; sleep 10"],
            )
        ],
    ),
)

env = flyte.TaskEnvironment(
    name="pod_template_task",
    pod_template=pod_template,
    image=flyte.Image.from_debian_base(
        registry="europe-north1-docker.pkg.dev/knada-project/flyteimages",
    ).with_pip_packages(
      "kubernetes",
    )
)


@env.task
async def main(data: str) -> str:
    return f"Hello {data}"


if __name__ == "__main__":
    flyte.init_from_config()
    result = flyte.run(main, data="hello world")
    print(result)
