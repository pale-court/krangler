import json
import os
from pathlib import Path
import sys

from pydantic import AnyHttpUrl
import requests
from krangler.logging import LOG


def saved_appinfo_to_changelist(saved_appinfo):
    try:
        appinfo = saved_appinfo["appinfo"]
        cl = {"change_id": appinfo["_change_number"], "branches": {}}

        ai_depots = appinfo["depots"]
        ai_branches = ai_depots["branches"]
        for branch_name in ["public"]:
            ai_branch = ai_branches[branch_name]
            branch = {
                "build_id": int(ai_branch["buildid"]),
                "time_updated": ai_branch["timeupdated"],
                "manifests": {},
            }
            manifests = branch["manifests"]
            for depot_id in [238961, 238962, 238963]:
                ai_depot = ai_depots[str(depot_id)]
                mf = ai_depot["manifests"][branch_name]
                if isinstance(
                    mf, str
                ):  # best-effort map from old appinfo format from before 2023-05-30
                    mf = {"gid": mf, "size": ai_depot["maxsize"]}
                manifests[str(depot_id)] = mf
            cl["branches"][branch_name] = branch
        return cl
    except Exception as e:
        LOG.info(f"Could not process appinfo, {e=}")
        return None


def upload_appinfos(*, api_url: AnyHttpUrl, appinfo_dir: Path, molly_guard: str):
    LOG.info("Uploading changelists from appinfos")
    url = f"{api_url}builds/public"
    res = requests.get(url)
    js = res.json()
    for root, dirs, files in os.walk(appinfo_dir / "saved"):
        root = Path(root)
        for file in files:
            path = root / file
            appinfo = json.loads(path.read_bytes())
            if changelist := saved_appinfo_to_changelist(appinfo):
                cid = changelist["change_id"]
                url = f"{api_url}changelists/{cid}"
                bid = changelist["branches"]["public"]["build_id"]
                if str(bid) in js:
                    continue
                resp = None
                try:
                    resp = requests.put(
                        url,
                        json=changelist,
                        params={"molly_guard": molly_guard},
                    )
                    resp.raise_for_status()
                    LOG.info(f"Uploaded changelist {cid} for build {bid}")
                except requests.HTTPError as e:
                    if resp is not None:
                        if resp.status_code == 422:
                            LOG.info(changelist)
                            LOG.info(resp.json())
                        else:
                            LOG.info(resp.text)
                    else:
                        LOG.info("No response")
                    raise
                except requests.RequestException:
                    LOG.exception(f"Could not upload appinfo {file}")
                    pass
