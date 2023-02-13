from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    date_prefix = {
        "green":"lpep",
        "yellow":"tpep"
    }
    pick_up_datetime = f"{date_prefix[color]}_pickup_datetime"
    drop_off_datetime = f"{date_prefix[color]}_dropoff_datetime"

    df[pick_up_datetime] = pd.to_datetime(df[pick_up_datetime])
    df[drop_off_datetime] = pd.to_datetime(df[drop_off_datetime])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    folder_path = f"data/{color}"
    if not os.path.exists(folder_path):
        print(f"Destination path : {folder_path} does not exist. Creating folder.")
        os.makedirs(folder_path)
        print(f'Folder (with sub-folder) created: {folder_path}')

    path = Path(f"{folder_path}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(color : str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [11], year: int = 2020, color: str = 'green'):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == "__main__":
    color = 'yellow'
    year = 2019
    months = [2,3]
    etl_parent_flow(color=color, months = months, year = year)
