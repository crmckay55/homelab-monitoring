from __future__ import annotations

import os
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, List
from string import Formatter

# --- Load .env file explicitly ---
from dotenv import load_dotenv

current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
env_path = project_root / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()
# ---------------------------------------

from common import build_wrapper, utc_now, CollectorError
from storage import write_to_spool, build_bronze_path, flush_spool
from http_client import HttpClient

class ConfigContext:
    """
    Handles variable interpolation in strings and dictionaries.
    """
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        # Seed context with 'collect' block (e.g., node: hl2)
        self.base_context = cfg.get("collect", {}).copy()
    
    def resolve_str(self, text: str, extra_ctx: Dict[str, Any] = None, strict: bool = True) -> str:
        """
        Resolves a string using base context + extra context + environment variables.
        If strict=False, it leaves unresolved variables like {id} alone.
        """
        if not isinstance(text, str): 
            return text
            
        # 1. Start with base context
        full_ctx = self.base_context.copy()
        
        # 2. Add extra runtime context (e.g. item ID)
        if extra_ctx:
            full_ctx.update(extra_ctx)
            
        # 3. Add uppercase versions of keys (e.g. node="hl2" -> NODE="HL2")
        upper_ctx = {k.upper(): str(v).upper() for k, v in full_ctx.items()}
        full_ctx.update(upper_ctx)

        if strict:
            # Strict mode: Fail if a key is missing
            try:
                return text.format(**full_ctx)
            except KeyError as e:
                raise CollectorError(f"Configuration variable {e} missing during resolution of string: '{text}'")
        else:
            # Safe mode: Use safe_substitute-like behavior but for .format style
            return SafeFormatter().format(text, **full_ctx)

    def resolve_dict(self, d: Dict, extra_ctx: Dict = None, strict: bool = True) -> Dict:
        """Recursively resolve values in a dictionary."""
        new_d = {}
        for k, v in d.items():
            if isinstance(v, str):
                new_d[k] = self.resolve_str(v, extra_ctx, strict=strict)
            elif isinstance(v, dict):
                new_d[k] = self.resolve_dict(v, extra_ctx, strict=strict)
            else:
                new_d[k] = v
        return new_d

class SafeFormatter(Formatter):
    """
    A custom formatter that ignores missing keys (leaves {key} intact).
    """
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            try:
                return kwargs[key]
            except KeyError:
                return "{" + key + "}"
        return super().get_value(key, args, kwargs)


def run_collector(cfg: Dict[str, Any]):
    """
    Execute the collection logic for a single YAML configuration.
    """
    # 1. Context Resolution (Safe Mode)
    # We resolve the config partially. We want {node} resolved, but {id} left alone for now.
    ctx = ConfigContext(cfg)
    resolved_cfg = ctx.resolve_dict(cfg, strict=False)

    # 2. Init HTTP Client
    # The base_url and auth params MUST be fully resolved by now.
    client = HttpClient(resolved_cfg)

    # 3. Determine Collection Mode
    collect = resolved_cfg.get("collect", {})
    mode = collect.get("mode", "single")
    records = []

    print(f"Running {resolved_cfg.get('entity')} in mode: {mode}")

    if mode == "single":
        # Check both root level AND 'collect' block for endpoint
        endpoint = resolved_cfg.get("endpoint") or collect.get("endpoint")
        if not endpoint:
            raise CollectorError("Mode 'single' requires 'endpoint' in config")
        
        endpoint = ctx.resolve_str(endpoint, strict=True)
        data = client.get(endpoint)
        records.append(data)

    elif mode == "loop":
        list_endpoint = collect.get("list_endpoint")
        if not list_endpoint:
            raise CollectorError("Mode 'loop' requires 'list_endpoint'")
        
        list_endpoint = ctx.resolve_str(list_endpoint, strict=True)
        
        # print(f"DEBUG: Fetching list from: {list_endpoint}")
        list_data = client.get(list_endpoint)
        
        json_path = collect.get("list_json_path", "")
        items = list_data
        
        if json_path:
            for key in json_path.split("."):
                if isinstance(items, dict):
                    items = items.get(key, [])
                else:
                    break
        
        if not isinstance(items, list):
            print(f"Warning: Expected list at '{json_path}', got {type(items)}. skipping.")
            items = []
        # else:
            # print(f"DEBUG: Found {len(items)} items to process.")

        id_field = collect.get("id_field", "id")
        template = collect.get("item_endpoint_template")
        
        if not template:
            raise CollectorError("Mode 'loop' requires 'item_endpoint_template'")

        for item in items:
            item_id = item.get(id_field)
            if not item_id: 
                continue

            item_ctx = {"id": item_id}
            if isinstance(item, dict):
                safe_item = {k: str(v) for k, v in item.items()}
                item_ctx.update(safe_item)

            try:
                item_url = ctx.resolve_str(template, extra_ctx=item_ctx, strict=True)
                detail_data = client.get(item_url)
                records.append(detail_data)
            except Exception as e:
                print(f"Failed to fetch item {item_id}: {e}")

    # 4. Write Records to Spool
    dt = utc_now().strftime("%Y-%m-%d")
    blob_path = build_bronze_path(resolved_cfg, dt)
    
    count = 0
    for rec in records:
        wrapped = build_wrapper(
            source=resolved_cfg.get("source"),
            entity=resolved_cfg.get("entity"),
            schema_version=resolved_cfg.get("schema_version", 1),
            host=client.base_url,
            payload=rec
        )
        write_to_spool(blob_path, wrapped)
        count += 1

    # 5. Flush to Azure (Scoped to this specific path to avoid race conditions)
    flushed, failed = flush_spool(target_path=blob_path)
    print(f"Finished {resolved_cfg.get('entity')}: collected {count}, flushed {flushed}, failed {failed}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python collector.py <path_to_config.yaml>")
        sys.exit(1)

    user_input_path = Path(sys.argv[1])
    
    if user_input_path.exists():
        target_path = user_input_path
    else:
        # Try to find the config relative to project root/configs
        script_dir = Path(__file__).parent.resolve()
        project_root = script_dir.parent.parent # homelab-monitoring/
        possible_path = project_root / "src" / "configs" / user_input_path.name
        
        # Also check just ../configs in case folder structure varies slightly
        possible_path_2 = script_dir.parent / "configs" / user_input_path.name

        if possible_path.exists():
            target_path = possible_path
        elif possible_path_2.exists():
            target_path = possible_path_2
        else:
            print(f"Error: Config file not found at '{user_input_path}'")
            print(f"Checked: {possible_path}")
            sys.exit(1)

    print(f"Loading config from: {target_path}")

    try:
        with open(target_path, "r") as f:
            cfg = yaml.safe_load(f)
        run_collector(cfg)
    except Exception as e:
        print(f"CRITICAL FAILURE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)