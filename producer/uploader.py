import os
from pathlib import Path

from databricks.sdk import WorkspaceClient


def get_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )


def upload_file(local_path: Path, volume_path: str) -> None:
    # Upload a local file into a Unity Catalog volume path
    w = get_client()
    with open(local_path, "rb") as f:
        w.files.upload(volume_path, f, overwrite=True)