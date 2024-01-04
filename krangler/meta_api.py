from typing import Dict, Optional
from pydantic import AnyHttpUrl, BaseModel, RootModel
import requests


class DepotManifest(BaseModel):
    gid: str
    size: Optional[int] = None
    download: Optional[int] = None


class Build(BaseModel):
    build_id: int
    time_updated: str
    version: Optional[str] = None
    manifests: Dict[str, DepotManifest]


class BuildCollection(RootModel):
    root: Dict[str, Build]


def fetch_build_collection(data_uri: AnyHttpUrl) -> BuildCollection:
    uri = f"{data_uri}builds/public"
    resp = requests.get(uri)
    if resp.status_code != 200:
        raise RuntimeError(f"{resp.status_code}: Could not fetch build collection")
    return BuildCollection.model_validate_json(resp.text)
