"""
order_client.py — thin wrapper around py-clob-client-v2 for Polymarket order placement.
Run all public functions in a background thread; never call from the GUI/main thread.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

from py_clob_client_v2 import (
    ClobClient,
    OrderArgs,
    MarketOrderArgs,
    OpenOrderParams,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)

# signature_type=1 (POLY_PROXY): EOA signs on behalf of a Polymarket proxy wallet.
# Required when POLY_KEY (EOA private key) and POLY_FUNDER (proxy wallet address)
# are different — which is the standard Polymarket browser-wallet setup.
# signature_type=0 (EOA) is only correct when the signer address == funder address.
_SIG_TYPE = 1  # POLY_PROXY

_client: ClobClient | None = None


def _get_client() -> ClobClient:
    global _client
    if _client is None:
        # Step 1: derive API creds with L1 auth only
        bootstrap = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=os.environ["POLY_KEY"],
            signature_type=_SIG_TYPE,
            funder=os.environ["POLY_FUNDER"],
        )
        creds = bootstrap.create_or_derive_api_key()
        # Step 2: full client with L1 + L2 creds
        _client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=os.environ["POLY_KEY"],
            creds=creds,
            signature_type=_SIG_TYPE,
            funder=os.environ["POLY_FUNDER"],
        )
    return _client


def place_limit_order(
    token_id: str, side: str, price: float, size: float, expiration: int = 0
) -> dict:
    """Post-only limit order. expiration: Unix timestamp (0 = GTC, >0 = GTD)."""
    client = _get_client()
    order_type = OrderType.GTD if expiration > 0 else OrderType.GTC
    return client.create_and_post_order(
        order_args=OrderArgs(
            price=price,
            size=size,
            side=Side.BUY if side == "BUY" else Side.SELL,
            token_id=token_id,
            expiration=expiration,
        ),
        order_type=order_type,
        post_only=True,
    )


def place_market_order(token_id: str, side: str, amount: float) -> dict:
    """FOK market order. amount: USDC to spend (BUY) or tokens to sell (SELL)."""
    client = _get_client()
    return client.create_and_post_market_order(
        order_args=MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=Side.BUY if side == "BUY" else Side.SELL,
        ),
        order_type=OrderType.FOK,
    )


def cancel_all() -> dict:
    """Cancel all open orders for this account. Level 2 auth required."""
    return _get_client().cancel_all()


def get_open_orders(asset_id: str = "") -> list[dict]:
    """Return currently open orders for this account, optionally filtered by asset_id."""
    params = OpenOrderParams(asset_id=asset_id) if asset_id else None
    return list(_get_client().get_open_orders(params=params))


def get_api_creds() -> dict:
    """Return WS auth dict {apiKey, secret, passphrase}. Initialises client if needed."""
    cr = _get_client().creds
    return {
        "apiKey":     getattr(cr, "api_key",        ""),
        "secret":     getattr(cr, "api_secret",     ""),
        "passphrase": getattr(cr, "api_passphrase", ""),
    }


def get_balance() -> float:
    """Return USDC balance available in the proxy wallet."""
    from py_clob_client_v2 import BalanceAllowanceParams, AssetType
    result = _get_client().get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    return round(float(result.get("balance", 0) or 0) / 1_000_000)


def get_positions() -> dict[str, dict]:
    """Fetch current token positions from the Polymarket data API.

    Returns a dict keyed by asset_id (token ID) with values:
        {"size": float, "avg_price": float, "outcome": str}

    Uses POLY_FUNDER (proxy wallet address) as the query parameter.
    Size < 0.01 shares are omitted (dust).
    """
    import httpx
    funder = os.environ.get("POLY_FUNDER", "").strip()
    if not funder:
        return {}
    url    = f"https://data-api.polymarket.com/positions?user={funder}&limit=500&sizeThreshold=0.01"
    resp   = httpx.get(url, timeout=15)
    resp.raise_for_status()
    rows   = resp.json()
    result: dict[str, dict] = {}
    for row in rows:
        asset_id = str(row.get("asset") or row.get("asset_id") or "").strip()
        size     = float(row.get("size", 0) or 0)
        avg      = float(row.get("avgPrice") or row.get("avg_price") or 0)
        outcome  = str(row.get("outcome", "")).strip()
        if asset_id and size >= 0.01:
            result[asset_id] = {"size": size, "avg_price": avg, "outcome": outcome}
    return result
