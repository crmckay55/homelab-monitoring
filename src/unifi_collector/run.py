from __future__ import annotations

import os
import sys
import yaml

from .common import (
    die,
    utc_now_iso,
    parse_host_from_base_url,
    env_bool_from_name,
    get_storage_env,
    build_auth_headers,
    request_json,
    get_container,
    wrap_record,
    append_jsonl,
    expand_path_template,
    get_list_items,
    format_template,
    unifi_os_login_session,
)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def run_single(cfg: dict, base_url: str, verify_tls: bool, headers: dict, session, config_path: str) -> None:
    source = cfg.get("source")
    entity = cfg.get("entity")
    schema_version = int(cfg.get("schema_version", 1))
    endpoint = cfg.get("endpoint")
    blob_path_tmpl = cfg.get("blob_path")
    method = (cfg.get("method") or "GET").upper()
    body = cfg.get("body")

    if not (source and entity and endpoint and blob_path_tmpl):
        die(f"Config missing required fields: {config_path}")

    conn, container_name, environment, collector_host = get_storage_env()
    host = parse_host_from_base_url(base_url)
    collected_at = utc_now_iso()

    payload = request_json(
        base_url=base_url,
        path=endpoint,
        verify_tls=verify_tls,
        method=method,
        json_body=body,
        extra_headers=headers,
        session=session,
    )

    record = wrap_record(
        source=source,
        entity=entity,
        schema_version=schema_version,
        host=host,
        collected_at_utc=collected_at,
        collector_host=collector_host,
        environment=environment,
        payload=payload,
    )

    blob_name = expand_path_template(blob_path_tmpl, collected_at)
    container = get_container(conn, container_name)
    append_jsonl(container, blob_name, record)

    rows = None
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        rows = len(payload["data"])

    print(f"Appended 1 record -> {container_name}/{blob_name} (rows={rows})")


def run_fanout(cfg: dict, base_url: str, verify_tls: bool, headers: dict, session, config_path: str) -> None:
    source = cfg.get("source")
    entity = cfg.get("entity")
    schema_version = int(cfg.get("schema_version", 1))
    blob_path_tmpl = cfg.get("blob_path")

    fanout = cfg.get("fanout") or {}
    list_endpoint = fanout.get("list_endpoint")
    list_method = (fanout.get("list_method") or "GET").upper()
    list_body = fanout.get("list_body")

    item_endpoint_template = fanout.get("item_endpoint_template")
    item_method = (fanout.get("item_method") or "GET").upper()
    item_body = fanout.get("item_body")

    id_field = fanout.get("id_field")
    extra_meta = fanout.get("extra_meta") or {}

    if not (source and entity and blob_path_tmpl and list_endpoint and item_endpoint_template and id_field):
        die(f"Fanout config missing required fields: {config_path}")

    conn, container_name, environment, collector_host = get_storage_env()
    host = parse_host_from_base_url(base_url)
    collected_at = utc_now_iso()

    blob_name = expand_path_template(blob_path_tmpl, collected_at)
    container = get_container(conn, container_name)

    list_payload = request_json(
        base_url=base_url,
        path=list_endpoint,
        verify_tls=verify_tls,
        method=list_method,
        json_body=list_body,
        extra_headers=headers,
        session=session,
    )

    items = get_list_items(list_payload)
    if not items:
        record = wrap_record(
            source=source,
            entity=entity,
            schema_version=schema_version,
            host=host,
            collected_at_utc=collected_at,
            collector_host=collector_host,
            environment=environment,
            payload={"meta": {"note": "fanout list empty"}, "list_payload": list_payload},
            meta_extra=dict(extra_meta),
        )
        append_jsonl(container, blob_name, record)
        print(f"Appended 1 record -> {container_name}/{blob_name} (rows=0 fanout)")
        return

    appended = 0
    for it in items:
        if not isinstance(it, dict) or id_field not in it:
            continue

        item_id = it[id_field]
        path = format_template(item_endpoint_template, {id_field: item_id})

        item_payload = request_json(
            base_url=base_url,
            path=path,
            verify_tls=verify_tls,
            method=item_method,
            json_body=item_body,
            extra_headers=headers,
            session=session,
        )

        meta_plus = dict(extra_meta)
        meta_plus[id_field] = item_id

        record = wrap_record(
            source=source,
            entity=entity,
            schema_version=schema_version,
            host=host,
            collected_at_utc=collected_at,
            collector_host=collector_host,
            environment=environment,
            payload=item_payload,
            meta_extra=meta_plus,
        )

        append_jsonl(container, blob_name, record)
        appended += 1

    print(f"Appended {appended} record(s) -> {container_name}/{blob_name} (fanout)")


def run_once(config_path: str) -> None:
    cfg = load_config(config_path)

    base_url_env = cfg.get("base_url_env") or "DMP_BASE_URL"
    verify_tls_env = cfg.get("verify_tls_env") or "DMP_VERIFY_TLS"
    auth = (cfg.get("auth") or "unifi_api_key").strip().lower()

    base_url = os.getenv(base_url_env)
    if not base_url:
        die(f"Missing {base_url_env} (set it in .env or environment).")

    verify_tls = env_bool_from_name(verify_tls_env, default="false")

    # Header auth (or none)
    prox_id_env = cfg.get("proxmox_token_id_env") or "PROXMOX_API_TOKEN_ID"
    prox_sec_env = cfg.get("proxmox_token_secret_env") or "PROXMOX_API_TOKEN_SECRET"
    headers = build_auth_headers(auth, proxmox_token_id_env=prox_id_env, proxmox_token_secret_env=prox_sec_env)

    # Session auth (UniFi OS)
    session = None
    if auth == "unifi_os_session":
        session = unifi_os_login_session(base_url, verify_tls)

    if cfg.get("fanout"):
        run_fanout(cfg, base_url, verify_tls, headers, session, config_path)
    else:
        run_single(cfg, base_url, verify_tls, headers, session, config_path)


def main():
    if len(sys.argv) != 2:
        print("Usage: python -m unifi_collector.run <path-to-yaml>", file=sys.stderr)
        sys.exit(2)

    run_once(sys.argv[1])


if __name__ == "__main__":
    main()