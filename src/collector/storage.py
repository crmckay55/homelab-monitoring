from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Azure imports
try:
    from azure.core.exceptions import AzureError
    from azure.storage.blob import BlobServiceClient
except ImportError:
    AzureError = None
    BlobServiceClient = None

from common import utc_now, CollectorError

# --- Spooling (Local WAL) ---

def get_spool_dir() -> Path:
    p = Path(os.getenv("SPOOL_DIR", "~/.homelab-monitoring/spool")).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_to_spool(blob_path: str, record: Dict[str, Any]) -> Path:
    root = get_spool_dir()
    parts_dir = root / f"{blob_path}.parts"
    parts_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{utc_now().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.jsonl.part"
    file_path = parts_dir / filename
    
    line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
    file_path.write_text(line, encoding="utf-8")
    return file_path

# --- Azure Flushing ---

def build_bronze_path(cfg: Dict[str, Any], dt: str) -> str:
    """
    Constructs the Azure Blob path.
    Fix: Appends 'node' or unique identifier to filename to prevent threading collisions.
    """
    storage = cfg.get("storage", {})
    prefix = storage.get("bronze_prefix", "bronze")
    system = storage.get("system") or cfg.get("source")
    dataset = storage.get("dataset") or cfg.get("entity")
    entity = cfg.get("entity")
    
    # Try to find a unique identifier to split the files by source
    # 1. Check if 'node' is defined in the collect block
    unique_id = cfg.get("collect", {}).get("node")
    
    # 2. If not, check if 'host' is in meta (less likely available here)
    # 3. Fallback to just the entity name if no node found
    
    if unique_id:
        filename = f"{entity}_{unique_id}_{dt}.jsonl"
    else:
        filename = f"{entity}_{dt}.jsonl"
    
    return f"{prefix}/{system}/{dataset}/dt={dt}/{filename}"

def flush_spool(timeout: int = 10, target_path: Optional[str] = None) -> Tuple[int, int]:
    """
    Scans spool directory and uploads .part files to Azure.
    """
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        return 0, 0

    if BlobServiceClient is None:
        return 0, 0

    container_name = os.getenv("AZURE_BLOB_CONTAINER", "homelab-telemetry")
    
    try:
        svc = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client(container_name)
    except Exception as e:
        print(f"Storage Init Error: {e}")
        return 0, 1

    flushed = 0
    failed = 0
    root = get_spool_dir()

    # Determine which directories to scan
    if target_path:
        target_dir = root / f"{target_path}.parts"
        if target_dir.exists():
            parts_dirs = [target_dir]
        else:
            parts_dirs = []
    else:
        parts_dirs = root.rglob("*.jsonl.parts")

    for parts_dir in parts_dirs:
        # Reconstruct blob_path
        if target_path:
            blob_path = target_path
        else:
            rel_path = parts_dir.relative_to(root)
            blob_path = str(rel_path).removesuffix(".parts")
        
        parts = sorted([p for p in parts_dir.glob("*.jsonl.part") if p.is_file()])
        if not parts:
            _cleanup_dir(parts_dir)
            continue

        try:
            blob = container.get_blob_client(blob_path)
            if not blob.exists():
                blob.create_append_blob()

            for part in parts:
                try:
                    data = part.read_bytes()
                    blob.append_block(data, timeout=timeout)
                    part.unlink() 
                    flushed += 1
                except Exception:
                    failed += 1
            
            _cleanup_dir(parts_dir)
            
        except Exception:
            failed += 1

    return flushed, failed

def _cleanup_dir(path: Path):
    try:
        path.rmdir()
    except OSError:
        pass