import os
import logging
from base64 import urlsafe_b64encode, urlsafe_b64decode

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger("twomoon.encryption")

NONCE_LENGTH = 12


def _derive_key(master_key_b64: str) -> bytes:
    raw = urlsafe_b64decode(master_key_b64 + "==")
    if len(raw) not in (16, 24, 32):
        raise ValueError(
            f"Master key must decode to 16, 24, or 32 bytes, got {len(raw)}"
        )
    return raw


def encrypt_token(plaintext: str, master_key_b64: str) -> str:
    key = _derive_key(master_key_b64)
    aesgcm = AESGCM(key)
    nonce = os.urandom(NONCE_LENGTH)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    combined = nonce + ciphertext
    return urlsafe_b64encode(combined).decode("ascii").rstrip("=")


def decrypt_token(cipher_b64: str, master_key_b64: str) -> str:
    key = _derive_key(master_key_b64)
    padding = 4 - (len(cipher_b64) % 4)
    if padding != 4:
        cipher_b64 += "=" * padding
    combined = urlsafe_b64decode(cipher_b64)
    if len(combined) < NONCE_LENGTH + 16:
        raise ValueError("Ciphertext too short to contain nonce and GCM tag")
    nonce = combined[:NONCE_LENGTH]
    ciphertext = combined[NONCE_LENGTH:]
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")


def generate_master_key() -> str:
    raw = os.urandom(32)
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")
