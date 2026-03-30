import os
from datetime import datetime, timezone
from utils.common import require_env

import jwt


def _load_private_key() -> str:
    return require_env("SESSION_JWT_PRIVATE_KEY")


def _load_public_key() -> str:
    return require_env("SESSION_JWT_PUBLIC_KEY")


def _load_public_keys() -> list[str]:
    keys = [_load_public_key()]
    old_key = require_env("SESSION_JWT_PUBLIC_KEY_OLD")
    if old_key:
        keys.append(old_key)
    return keys


def sign_session(session_uuid: str) -> str:
    private_key = _load_private_key()
    now = int(datetime.now(timezone.utc).timestamp())
    payload = {"sub": session_uuid, "iat": now, "aud": "vra_session"}
    token = jwt.encode(payload, private_key, algorithm="RS256")

    return token


def verify_session(token: str) -> str:
    public_key = _load_public_key()
    decoded = jwt.decode(
        token,
        public_key,
        algorithms=["RS256"],
        audience="vra_session",
        options={"require": ["sub", "iat"]},
    )

    sub = decoded.get("sub")
    if not sub:
        raise jwt.InvalidTokenError("Token missing subject")
    return sub
