"""
order_client.py — thin wrapper around py-clob-client for Polymarket order placement.
Run all public functions in a background thread; never call from the GUI/main thread.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
from py_clob_client.constants import POLYGON
from py_clob_client.order_builder.constants import BUY, SELL

# signature_type=1 (POLY_PROXY): EOA signs on behalf of a Polymarket proxy wallet.
# Required when POLY_KEY (EOA private key) and POLY_FUNDER (proxy wallet address)
# are different — which is the standard Polymarket browser-wallet setup.
# signature_type=0 (EOA) is only correct when the signer address == funder address.
_SIG_TYPE = 1  # POLY_PROXY

_client: ClobClient | None = None


def _get_client() -> ClobClient:
    global _client
    if _client is None:
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.environ["POLY_KEY"],
            chain_id=POLYGON,
            signature_type=_SIG_TYPE,
            funder=os.environ["POLY_FUNDER"],
        )
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        _client = client
    return _client


def place_limit_order(
    token_id: str, side: str, price: float, size: float, expiration: int = 0
) -> dict:
    """Limit order. expiration: Unix timestamp (0 = GTC, >0 = GTD)."""
    client = _get_client()
    signed = client.create_order(OrderArgs(
        price=price,
        size=size,
        side=BUY if side == "BUY" else SELL,
        token_id=token_id,
        expiration=expiration,
    ))
    order_type = OrderType.GTD if expiration > 0 else OrderType.GTC
    return client.post_order(signed, order_type)


def place_market_order(token_id: str, side: str, amount: float) -> dict:
    """FOK market order. amount: USDC to spend (BUY) or tokens to sell (SELL)."""
    client = _get_client()
    signed = client.create_market_order(MarketOrderArgs(
        token_id=token_id,
        amount=amount,
        side=BUY if side == "BUY" else SELL,
    ))
    return client.post_order(signed, OrderType.FOK)


def cancel_all() -> dict:
    """Cancel all open orders for this account. Level 2 auth required."""
    return _get_client().cancel_all()
