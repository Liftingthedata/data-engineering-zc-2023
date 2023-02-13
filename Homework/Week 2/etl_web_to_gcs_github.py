from etl_web_to_gcs import etl_web_to_gcs
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("git-flow")

deploy = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="etl_web_to_gcs_github",
    storage=github_block,
    entrypoint="./Homeworks/Week 2/etl_web_to_gcs.py:etl_web_to_gcs"
)

if __name__ == "__main__":
    deploy.apply()
