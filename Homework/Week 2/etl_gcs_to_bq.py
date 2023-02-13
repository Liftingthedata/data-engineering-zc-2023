from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame, t_name : str) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    df.to_gbq(
        destination_table=f"rides.{t_name}",
        project_id="de-zc-02393lkb",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def el_gcs_to_bq(color : str, year : int, month: int):
    path = extract_from_gcs(color, year, month)
    df = load_data(path)
    write_bq(df, t_name = color)

@flow()
def parent_el_gcs_to_bq(color : str = 'green', year : int = 2020, months: list[int] = [11]):
    for month in months:
        el_gcs_to_bq(color=color, year=year, month=month)

if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2,3]
    parent_el_gcs_to_bq(color=color, year=year, months=months)
