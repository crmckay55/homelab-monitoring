from __future__ import annotations

import os
import requests
from typing import Dict, Any, Optional
from common import CollectorError

class HttpClient:
    """
    A wrapper around requests.Session that handles:
    1. Base URL management
    2. Authentication (Proxmox Token, Unifi API Key, Unifi OS Session)
    3. TLS verification and timeouts
    """
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.http_cfg = cfg.get("http", {})
        self.auth_cfg = cfg.get("auth", {})
        
        self.session = requests.Session()
        
        self.base_url = str(self.http_cfg.get("base_url", "")).rstrip("/")
        self.verify = bool(self.http_cfg.get("verify_tls", True))
        self.timeout = int(self.http_cfg.get("timeout_seconds", 20))
        
        # Perform authentication immediately upon initialization
        self._authenticate()

    def _authenticate(self):
        """Dispatch to the correct auth handler based on 'mode'."""
        mode = self.auth_cfg.get("mode", "none").strip()
        
        if mode == "none":
            return

        elif mode == "proxmox_token":
            self._auth_proxmox()

        elif mode == "unifi_api_key":
            self._auth_unifi_api_key()
        
        elif mode == "unifi_os_session":
            self._auth_unifi_os_session()
        
        else:
            raise CollectorError(f"Unknown auth.mode: {mode}")

    def _auth_proxmox(self):
        """
        Headers: Authorization: PVEAPIToken=USER@PAM!ID=SECRET
        """
        # The ID is usually static env var
        token_id_env = self.auth_cfg.get("token_id_env", "PROXMOX_API_TOKEN_ID")
        token_id = os.getenv(token_id_env)
        
        # The Secret might be dynamic (e.g. specific to a node), passed via context resolution in collector.py
        # However, for Auth, we look up the ENV VAR name provided in config.
        token_secret_env = self.auth_cfg.get("token_secret_env")
        token_secret = os.getenv(token_secret_env) if token_secret_env else None
        
        if not token_id or not token_secret:
            raise CollectorError(f"Missing Proxmox credentials. Checked {token_id_env} and {token_secret_env}")
            
        self.session.headers.update({
            "Authorization": f"PVEAPIToken={token_id}={token_secret}"
        })

    def _auth_unifi_api_key(self):
        """
        Headers: X-API-KEY: <key>
        """
        key_env = self.auth_cfg.get("api_key_env", "UNIFI_API_KEY")
        key = os.getenv(key_env)
        if not key:
            raise CollectorError(f"Missing Unifi API Key in env: {key_env}")
        
        self.session.headers.update({"X-API-KEY": key})

    def _auth_unifi_os_session(self):
        """
        Login to UniFi OS, get session cookie, and extract X-CSRF-Token.
        """
        user_env = self.auth_cfg.get("username_env", "UNIFI_OS_USERNAME")
        pass_env = self.auth_cfg.get("password_env", "UNIFI_OS_PASSWORD")
        
        username = os.getenv(user_env)
        password = os.getenv(pass_env)
        
        if not username or not password:
            raise CollectorError(f"Missing Unifi OS credentials in {user_env}/{pass_env}")

        # Construct login URL
        login_path = self.auth_cfg.get("login_path", "/api/auth/login")
        # Ensure base_url is set
        if not self.base_url:
            raise CollectorError("http.base_url is required for unifi_os_session auth")
            
        url = f"{self.base_url}{login_path}"
        
        try:
            r = self.session.post(
                url,
                json={"username": username, "password": password},
                verify=self.verify,
                timeout=self.timeout
            )
        except Exception as e:
            raise CollectorError(f"Unifi Login Connection Failed: {e}")

        if r.status_code >= 400:
            raise CollectorError(f"UniFi OS login failed: HTTP {r.status_code} {r.text}")

        # Extract CSRF token from headers (case-insensitive check)
        csrf = r.headers.get("x-csrf-token") or r.headers.get("X-Csrf-Token")
        if csrf:
            self.session.headers.update({"X-CSRF-Token": csrf})
        else:
            # Some older versions or different apps might not return it here, 
            # but usually it's required for subsequent POSTs. 
            # For GETs, the cookie (handled by session) is often enough.
            pass 

    def get(self, endpoint: str) -> Any:
        """
        Execute a GET request using the configured session.
        Handles URL construction and JSON parsing.
        """
        # Determine full URL
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            # Ensure slash consistency
            path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
            url = f"{self.base_url}{path}"

        try:
            r = self.session.get(url, verify=self.verify, timeout=self.timeout)
        except Exception as e:
            raise CollectorError(f"Request failed: {url} - {e}")

        if r.status_code >= 400:
            raise CollectorError(f"HTTP {r.status_code} calling {url}: {r.text[:200]}")

        try:
            return r.json()
        except ValueError:
            raise CollectorError(f"Non-JSON response calling {url}: {r.text[:200]}")