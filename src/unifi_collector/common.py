from __future__ import annotations

import os
import sys
import json
import socket
import requests
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from typing import Any

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from dotenv import load_dotenv

# Load .env from repo root / current working dir
load_dotenv()


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def env_bool_from_name(env_name: str, default: str = "false") -> bool:
    return os.getenv(env_name, default).lower() in ("1", "true", "yes", "y")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_host_from_base_url(base_url: str) -> str:
    parsed = urlparse(base_url or "")
    return parsed.hostname or (base_url or "unknown")


def get_storage_env():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        die("Missing AZURE_STORAGE_CONNECTION_STRING.")

    container_name = os.getenv("AZURE_BLOB_CONTAINER", "homelab-telemetry")
    environment = os.getenv("ENVIRONMENT", "homelab")
    collector_host = socket.gethostname()
    return conn, container_name, environment, collector_host


def build_auth_headers(
    auth_type: str,
    *,
    proxmox_token_id_env: str | None = None,
    proxmox_token_secret_env: str | None = None,
) -> dict:
    """
    Header-based auth types only.
    Session-based UniFi OS auth is handled via requests.Session in run.py.
    """
    auth_type = (auth_type or "unifi_api_key").strip().lower()

    if auth_type in ("none", "unifi_os_session"):
        return {}

    if auth_type == "unifi_api_key":
        api_key = os.getenv("DMP_API_KEY")
        if not api_key:
            die("Missing DMP_API_KEY for auth=unifi_api_key")
        return {"X-API-KEY": api_key}

    if auth_type == "proxmox_token":
        tid_env = proxmox_token_id_env or "PROXMOX_API_TOKEN_ID"
        sec_env = proxmox_token_secret_env or "PROXMOX_API_TOKEN_SECRET"

        token_id = os.getenv(tid_env)
        token_secret = os.getenv(sec_env)

        if not token_id:
            die(f"Missing {tid_env} for auth=proxmox_token")
        if not token_secret:
            die(f"Missing {sec_env} for auth=proxmox_token")

        return {"Authorization": f"PVEAPIToken={token_id}={token_secret}"}

    die(f"Unsupported auth type '{auth_type}'.")
    return {}  # unreachable


def request_json(
    base_url: str,
    path: str,
    verify_tls: bool,
    method: str = "GET",
    json_body: dict | None = None,
    extra_headers: dict | None = None,
    timeout_s: int = 20,
    session: requests.Session | None = None,
) -> dict:
    """
    Generic JSON HTTP client (GET/POST), optionally using a requests.Session.
    """
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    headers = {"Accept": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    client = session or requests
    method = (method or "GET").upper().strip()

    if method == "POST":
        r = client.post(url, headers=headers, json=(json_body or {}), timeout=timeout_s, verify=verify_tls)
    elif method == "GET":
        r = client.get(url, headers=headers, timeout=timeout_s, verify=verify_tls)
    else:
        die(f"Unsupported HTTP method '{method}' calling {path}")

    if r.status_code >= 400:
        # For UniFi OS, 401 is expected until login; caller decides when to treat it as fatal.
        die(f"HTTP {r.status_code} calling {path}: {r.text[:500]}")

    try:
        return r.json()
    except ValueError:
        die(f"Non-JSON response calling {path}: {r.text[:500]}")
        return {}  # unreachable


def get_container(conn_str: str, container_name: str):
    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(container_name)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    return container


def ensure_append_blob(container_client, blob_name: str):
    blob = container_client.get_blob_client(blob_name)
    try:
        props = blob.get_blob_properties()
        if props.blob_type != "AppendBlob":
            die(f"Blob exists but is not AppendBlob: {blob_name}")
    except ResourceNotFoundError:
        blob.create_append_blob()
    return blob


def wrap_record(
    source: str,
    entity: str,
    schema_version: int,
    host: str,
    collected_at_utc: str,
    collector_host: str,
    environment: str,
    payload: dict,
    meta_extra: dict | None = None,
) -> dict:
    meta = {
        "source": source,
        "entity": entity,
        "schema_version": schema_version,
        "host": host,
        "collected_at_utc": collected_at_utc,
        "collector_host": collector_host,
        "environment": environment,
    }
    if meta_extra:
        meta.update(meta_extra)
    return {"meta": meta, "payload": payload}


def expand_path_template(template: str, collected_at_utc: str) -> str:
    dt = collected_at_utc[:10]
    yyyy, mm, dd = dt.split("-")
    return (
        template.replace("{dt}", dt)
        .replace("{yyyy}", yyyy)
        .replace("{mm}", mm)
        .replace("{dd}", dd)
    )


def append_jsonl(container_client, blob_name: str, record: dict) -> None:
    line = json.dumps(record, separators=(",", ":")) + "\n"
    blob = ensure_append_blob(container_client, blob_name)
    blob.append_block(line.encode("utf-8"))


def get_list_items(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, list):
        return data
    return []


def format_template(template: str, values: dict[str, Any]) -> str:
    out = template
    for k, v in values.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def unifi_os_login_session(base_url: str, verify_tls: bool) -> requests.Session:
    """
    Logs into UniFi OS (UNAS/Protect/etc) using username+password and returns a session with cookies.
    """
    username = os.getenv("UNIFI_OS_USERNAME")
    password = os.getenv("UNIFI_OS_PASSWORD")
    if not (username and password):
        die("Missing UNIFI_OS_USERNAME and/or UNIFI_OS_PASSWORD for auth=unifi_os_session")

    s = requests.Session()
    url = urljoin(base_url.rstrip("/") + "/", "api/auth/login")

    r = s.post(
        url,
        json={"username": username, "password": password},
        headers={"Accept": "application/json"},
        timeout=20,
        verify=verify_tls,
    )

    if r.status_code >= 400:
        die(f"UniFi OS login failed HTTP {r.status_code}: {r.text[:500]}")

    # Many UniFi OS logins return JSON; but even if not, cookie is what matters.
    return s