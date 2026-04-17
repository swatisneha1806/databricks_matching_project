from __future__ import annotations

import json
import logging
import os
import threading
import time
import warnings
from argparse import Namespace
from typing import Tuple, Optional

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings("ignore")
RELTIO_SCOPE = os.getenv("RELTIO_SECRET_SCOPE", "reltio")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_reltio_credentials(args: Namespace) -> Tuple[str, str]:
    # First, try to get from environment variables
    client_id = os.getenv("RELTIO_CLIENT_ID")
    client_secret = os.getenv("RELTIO_CLIENT_SECRET")

    if client_id and client_secret:
        logging.info("Using Reltio credentials from environment variables")
        return client_id, client_secret

    # If not in env, get from AWS Secrets Manager
    logging.info(f"Fetching Reltio credentials from AWS Secrets Manager (region: {AWS_REGION}, secret: {args.reltio_secret_name})")
    try:
        import boto3
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        resp = client.get_secret_value(SecretId=args.reltio_secret_name)

        secret_str = resp.get("SecretString")
        if not secret_str:
            raise RuntimeError("SecretString missing from Secrets Manager response")

        # Parse JSON and validate required keys
        secret_dict = json.loads(secret_str)
        required_keys = ["client_id", "client_secret"]
        for key in required_keys:
            if key not in secret_dict:
                raise RuntimeError(f"Missing key '{key}' in secret JSON")

        logging.info("Successfully retrieved Reltio credentials from AWS Secrets Manager")
        return secret_dict["client_id"], secret_dict["client_secret"]

    except Exception as e:
        raise RuntimeError(f"Failed to retrieve Reltio credentials from AWS Secrets Manager: {e}")


def fetch_reltio_access_token(client_id: str, client_secret: str, auth_url) -> Tuple[str, int]:
    try:
        logging.info(f"Requesting access token from {auth_url}")
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        resp = requests.post(auth_url, data=data, timeout=(10, 30))
        resp.raise_for_status()
        token_data = resp.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise RuntimeError("access_token missing from OAuth response")

        expires_in = int(token_data.get("expires_in", 3600))
        logging.info(f"Successfully obtained Reltio access token (expires_in={expires_in}s)")
        return access_token, expires_in

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch access token from Reltio: {e}")


def get_reltio_token(args: Namespace) -> Tuple[str, int]:
    client_id, client_secret = get_reltio_credentials(args)
    return fetch_reltio_access_token(client_id, client_secret, args.reltio_auth_url)


class ReltioTokenManager:
    def __init__(self, args: Namespace, buffer_seconds: int = 300):
        self._args = args
        self._buffer = buffer_seconds
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def get_token(self) -> str:
        # Fast path: return cached token if still valid
        if self._token and time.time() < self._expires_at:
            return self._token
        with self._lock:
            # Re-check after acquiring lock
            if self._token and time.time() < self._expires_at:
                return self._token
            self._do_fetch()
            return self._token

    def force_refresh(self) -> str:
        with self._lock:
            self._do_fetch()
            return self._token

    def _do_fetch(self) -> None:
        token, expires_in = get_reltio_token(self._args)
        self._token = token
        self._expires_at = time.time() + expires_in - self._buffer
