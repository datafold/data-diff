#
# This module contains all the functionality related to the anonymous tracking of data-diff use.
#

import logging
import os
import json
import platform
from time import time
from typing import Any, Dict, Optional
import urllib.request
from uuid import uuid4
import toml
from rich import get_console

from data_diff.version import __version__

TRACK_URL = "https://hosted.rudderlabs.com/v1/track"
START_EVENT = "os_diff_run_start"
END_EVENT = "os_diff_run_end"
TOKEN = "2HgtM4Hcq9BmeiCqNYhz7O9tkjM"
TIMEOUT = 8

DEFAULT_PROFILE = os.path.expanduser("~/.datadiff.toml")


def _load_profile():
    try:
        with open(DEFAULT_PROFILE) as f:
            conf = toml.load(f)
    except FileNotFoundError:
        conf = {}

    if "anonymous_id" not in conf:
        conf["anonymous_id"] = str(uuid4())
        with open(DEFAULT_PROFILE, "w") as f:
            toml.dump(conf, f)
    return conf


def bool_ask_for_email() -> bool:
    """
    Checks the .datadiff.toml profile file for the asked_for_email key

    Returns False immediately if --no-tracking or not in an interactive terminal

    If found, return False (already asked for email)

    If not found, add a key "asked_for_email", and return True (we should ask for email)

    Returns:
        bool: decision on whether to prompt the user for their email
    """
    console = get_console()
    if g_tracking_enabled and console.is_interactive:
        profile = _load_profile()

        if "asked_for_email" not in profile:
            profile["asked_for_email"] = ""
            with open(DEFAULT_PROFILE, "w") as conf:
                toml.dump(profile, conf)
            return True
    return False


def bool_notify_about_extension() -> bool:
    profile = _load_profile()
    console = get_console()
    if "notified_about_extension" not in profile and console.is_interactive:
        profile["notified_about_extension"] = ""
        with open(DEFAULT_PROFILE, "w") as conf:
            toml.dump(profile, conf)
        return True
    return False


g_tracking_enabled = True
g_anonymous_id = None

entrypoint_name = "Python API"


def disable_tracking():
    global g_tracking_enabled
    g_tracking_enabled = False


def is_tracking_enabled():
    return g_tracking_enabled


def set_entrypoint_name(s):
    global entrypoint_name
    entrypoint_name = s


dbt_user_id = None
dbt_version = None
dbt_project_id = None


def set_dbt_user_id(s):
    global dbt_user_id
    dbt_user_id = s


def set_dbt_version(s):
    global dbt_version
    dbt_version = s


def set_dbt_project_id(s):
    global dbt_project_id
    dbt_project_id = s


def get_anonymous_id():
    global g_anonymous_id
    if g_anonymous_id is None:
        profile = _load_profile()
        g_anonymous_id = profile["anonymous_id"]
    return g_anonymous_id


def create_start_event_json(diff_options: Dict[str, Any]):
    return {
        "event": "os_diff_run_start",
        "properties": {
            "distinct_id": get_anonymous_id(),
            "token": TOKEN,
            "time": time(),
            "os_type": os.name,
            "os_version": platform.platform(),
            "python_version": f"{platform.python_version()}/{platform.python_implementation()}",
            "diff_options": diff_options,
            "data_diff_version:": __version__,
            "entrypoint_name": entrypoint_name,
            "dbt_user_id": dbt_user_id,
            "dbt_version": dbt_version,
            "dbt_project_id": dbt_project_id,
        },
    }


def create_end_event_json(
    is_success: bool,
    runtime_seconds: float,
    data_source_1_type: str,
    data_source_2_type: str,
    table1_count: int,
    table2_count: int,
    diff_count: int,
    error: Optional[str],
    diff_id: Optional[int] = None,
    is_cloud: bool = False,
    org_id: Optional[int] = None,
    org_name: Optional[str] = None,
    user_id: Optional[int] = None,
):
    return {
        "event": "os_diff_run_end",
        "properties": {
            "distinct_id": get_anonymous_id(),
            "token": TOKEN,
            "time": time(),
            "is_success": is_success,
            "runtime_seconds": runtime_seconds,
            "data_source_1_type": data_source_1_type,
            "data_source_2_type": data_source_2_type,
            "table_1_rows_cnt": table1_count,
            "table_2_rows_cnt": table2_count,
            "diff_rows_cnt": diff_count,
            "error_message": error,
            "data_diff_version:": __version__,
            "entrypoint_name": entrypoint_name,
            "is_cloud": is_cloud,
            "diff_id": diff_id,
            "dbt_user_id": dbt_user_id,
            "dbt_version": dbt_version,
            "dbt_project_id": dbt_project_id,
            "org_id": org_id,
            "org_name": org_name,
            "user_id": user_id,
        },
    }


def create_email_signup_event_json(email: str) -> Dict[str, Any]:
    return {
        "event": "os_diff_email_opt_in",
        "properties": {
            "distinct_id": get_anonymous_id(),
            "token": TOKEN,
            "time": time(),
            "data_diff_version:": __version__,
            "entrypoint_name": entrypoint_name,
            "email": email,
            "dbt_user_id": dbt_user_id,
            "dbt_project_id": dbt_project_id,
        },
    }


def send_event_json(event_json):
    if not g_tracking_enabled:
        raise RuntimeError("Won't send; tracking is disabled!")

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic MkhndE00SGNxOUJtZWlDcU5ZaHo3Tzl0a2pNOg==",
    }
    data = json.dumps(event_json).encode()
    try:
        req = urllib.request.Request(TRACK_URL, data=data, headers=headers)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as f:
            res = f.read()
            if f.code != 200:
                raise RuntimeError(res)
    except Exception as e:
        logging.debug(f"Failed to post to Rudderstack: {e}")
