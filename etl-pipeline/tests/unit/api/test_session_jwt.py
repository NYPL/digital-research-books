import pytest
import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from uuid import uuid4

from api.session_jwt import sign_session, verify_session


def _generate_rsa_pem_pair(key_size: int = 2048):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


@pytest.fixture(scope="module")
def rsa_key_pair():
    return _generate_rsa_pem_pair()


@pytest.fixture(scope="module")
def other_rsa_key_pair():
    return _generate_rsa_pem_pair()


class TestSessionJWT:
    def test_sign_session_creates_valid_jwt(self, monkeypatch, rsa_key_pair):
        private_pem, public_pem = rsa_key_pair
        monkeypatch.setenv("SESSION_JWT_PRIVATE_KEY", private_pem)

        session_id = str(uuid4())
        token = sign_session(session_id)

        assert isinstance(token, str)
        assert token.count(".") == 2

        payload = pyjwt.decode(
            token, public_pem, algorithms=["RS256"], audience="vra_session"
        )

        assert payload["sub"] == session_id
        assert "iat" in payload
        assert payload["aud"] == "vra_session"

    def test_returns_session_uuid(self, monkeypatch, rsa_key_pair):
        private_pem, public_pem = rsa_key_pair
        monkeypatch.setenv("SESSION_JWT_PRIVATE_KEY", private_pem)
        monkeypatch.setenv("SESSION_JWT_PUBLIC_KEY", public_pem)

        session_id = str(uuid4())
        token = sign_session(session_id)

        assert verify_session(token) == session_id

    def test_raises_when_private_key_missing(self, monkeypatch):
        monkeypatch.delenv("SESSION_JWT_PRIVATE_KEY", raising=False)

        with pytest.raises(
            ValueError, match="Environment variable \"SESSION_JWT_PRIVATE_KEY\" must be available."
        ):
            sign_session(str(uuid4()))

    def test_raises_when_public_key_missing(self, monkeypatch):
        monkeypatch.delenv("SESSION_JWT_PUBLIC_KEY", raising=False)

        with pytest.raises(ValueError, match="Environment variable \"SESSION_JWT_PUBLIC_KEY\" must be available."):
            verify_session("any.token.value")

    def test_raises_on_tampered_signature(self, monkeypatch, rsa_key_pair):
        private_pem, public_pem = rsa_key_pair
        monkeypatch.setenv("SESSION_JWT_PRIVATE_KEY", private_pem)
        monkeypatch.setenv("SESSION_JWT_PUBLIC_KEY", public_pem)

        token = sign_session(str(uuid4()))
        tampered = token[:-5] + "XXXXX"

        with pytest.raises(pyjwt.PyJWTError):
            verify_session(tampered)

    def test_raises_on_wrong_key(self, monkeypatch, rsa_key_pair, other_rsa_key_pair):
        private_pem, _ = rsa_key_pair
        _, wrong_public_pem = other_rsa_key_pair
        monkeypatch.setenv("SESSION_JWT_PRIVATE_KEY", private_pem)
        monkeypatch.setenv("SESSION_JWT_PUBLIC_KEY", wrong_public_pem)

        token = sign_session(str(uuid4()))

        with pytest.raises(pyjwt.PyJWTError):
            verify_session(token)

    def test_raises_on_wrong_audience(self, monkeypatch, rsa_key_pair):
        private_pem, public_pem = rsa_key_pair
        monkeypatch.setenv("SESSION_JWT_PRIVATE_KEY", private_pem)
        monkeypatch.setenv("SESSION_JWT_PUBLIC_KEY", public_pem)

        token = pyjwt.encode(
            {"sub": str(uuid4()), "iat": 0, "aud": "wrong_aud"},
            private_pem,
            algorithm="RS256",
        )

        with pytest.raises(pyjwt.PyJWTError):
            verify_session(token)
