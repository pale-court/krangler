import json
import os
from pathlib import Path

import requests
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from krangler.logging import LOG
from krangler.upload import upload_appinfos


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="dev-env.env", extra="ignore")
    api_url: AnyHttpUrl
    appinfo_dir: Path
    molly_guard: str


def main():
    settings = Settings()
    upload_appinfos(
        api_url=settings.api_url,
        appinfo_dir=settings.appinfo_dir,
        molly_guard=settings.molly_guard,
    )


if __name__ == "__main__":
    main()
