from __future__ import annotations
import os
from typing import Final
from dotenv import load_dotenv
from py_clob_client.client import ClobClient

HOST: str = "https://clob.polymarket.com"
CHAIN_ID: int = 137

load_dotenv()
def dotenv(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def _get_str_attr(obj: object, *names: str) -> str:
    for n in names:
        v = getattr(obj, n, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def main() -> None:
    private_key = dotenv("POLY_KEY")
    signature_type_raw = os.getenv("POLY_SIGNATURE_TYPE", "").strip()
    funder = os.getenv("POLY_FUNDER", "").strip()

    if signature_type_raw:
        signature_type = int(signature_type_raw)
        client = ClobClient(
            host=HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=signature_type,
            funder=funder or None, # type: ignore
        )
    else:
        client = ClobClient(host=HOST, chain_id=CHAIN_ID, key=private_key)

    creds_obj = client.create_or_derive_api_creds()  # :contentReference[oaicite:2]{index=2}

    api_key = _get_str_attr(creds_obj, "apiKey", "api_key", "key")
    api_secret = _get_str_attr(creds_obj, "secret", "apiSecret", "api_secret")
    api_passphrase = _get_str_attr(creds_obj, "passphrase", "apiPassphrase", "api_passphrase")

    if not api_key or not api_secret or not api_passphrase:
        # Helpful debug without leaking values:
        present = {
            "api_key": bool(api_key),
            "api_secret": bool(api_secret),
            "api_passphrase": bool(api_passphrase),
            "creds_type": type(creds_obj).__name__,
        }
        raise RuntimeError(f"Derived creds still look incomplete: {present}")

    print("{")
    print(f'  "apiKey": "{api_key}",')
    print(f'  "secret": "{api_secret}",')
    print(f'  "passphrase": "{api_passphrase}"')
    print("}")
    print("\nPaste into your .env as:")
    print(f"CLOB_API_KEY={api_key}")
    print(f"CLOB_API_SECRET={api_secret}")
    print(f"CLOB_API_PASSPHRASE={api_passphrase}")


if __name__ == "__main__":
    main()
