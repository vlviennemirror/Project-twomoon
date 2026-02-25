import os
import logging
from base64 import urlsafe_b64encode, urlsafe_b64decode

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag

logger = logging.getLogger("twomoon.encryption")

NONCE_LENGTH = 12
MIN_GCM_TAG_LENGTH = 16


def _add_b64_padding(data: str) -> str:
    return data + "=" * ((-len(data)) % 4)


def _derive_key(master_key_b64: str) -> bytes:
    master_key_b64 = _add_b64_padding(master_key_b64)
    raw = urlsafe_b64decode(master_key_b64)

    if len(raw) not in (16, 24, 32):
        raise ValueError(
            f"Master key must decode to 16, 24, or 32 bytes, got {len(raw)}"
        )

    return raw


def encrypt_token(plaintext: str, master_key_b64: str) -> str:
    key = _derive_key(master_key_b64)
    aesgcm = AESGCM(key)

    nonce = os.urandom(NONCE_LENGTH)
    plaintext_bytes = plaintext.encode("utf-8")

    aad = b"twomoon-token-v1"
    ciphertext = aesgcm.encrypt(nonce, plaintext_bytes, aad)

    combined = nonce + ciphertext
    return urlsafe_b64encode(combined).decode("ascii")


def decrypt_token(cipher_b64: str, master_key_b64: str) -> str:
    key = _derive_key(master_key_b64)
    cipher_b64 = _add_b64_padding(cipher_b64)

    combined = urlsafe_b64decode(cipher_b64)

    min_len = NONCE_LENGTH + MIN_GCM_TAG_LENGTH
    if len(combined) < min_len:
        raise ValueError("Ciphertext too short to contain nonce and GCM tag")

    nonce = combined[:NONCE_LENGTH]
    ciphertext = combined[NONCE_LENGTH:]

    aesgcm = AESGCM(key)

    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, b"twomoon-token-v1")
    except InvalidTag:
        raise ValueError("Decryption failed: authentication tag mismatch")

    return plaintext.decode("utf-8")


def generate_master_key() -> str:
    raw = os.urandom(32)
    return urlsafe_b64encode(raw).decode("ascii")