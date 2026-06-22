from __future__ import annotations

import base64
import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from .models import PairRules


class ExchangeError(RuntimeError):
    pass


class KrakenReadOnly:
    """Kraken client deliberately exposing no order mutation methods."""

    def __init__(self, api_url: str, timeout: int, key: str = "", secret: str = ""):
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.key = key
        self.secret = secret

    def _public(self, endpoint: str, params: Dict[str, str]) -> Dict[str, Any]:
        response = requests.get(self.api_url + endpoint, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise ExchangeError(";".join(payload["error"]))
        return payload["result"]

    def _private(self, endpoint: str) -> Dict[str, Any]:
        if not self.key or not self.secret:
            raise ExchangeError("read-only reconciliation credentials are not configured")
        nonce = str(time.time_ns())
        data = {"nonce": nonce}
        encoded = urlencode(data)
        digest = hashlib.sha256((nonce + encoded).encode()).digest()
        signature = base64.b64encode(hmac.new(base64.b64decode(self.secret),
                                               endpoint.encode() + digest, hashlib.sha512).digest()).decode()
        response = requests.post(self.api_url + endpoint, data=data,
                                 headers={"API-Key": self.key, "API-Sign": signature}, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise ExchangeError(";".join(payload["error"]))
        return payload["result"]

    def ticker(self, pair: str) -> Dict[str, Decimal]:
        result = self._public("/0/public/Ticker", {"pair": pair})
        item = next(iter(result.values()))
        return {"ask": Decimal(item["a"][0]), "bid": Decimal(item["b"][0]),
                "last": Decimal(item["c"][0])}

    def pair_rules(self, pair: str, minimum_notional: Decimal = Decimal("0")) -> PairRules:
        result = self._public("/0/public/AssetPairs", {"pair": pair})
        item = next(iter(result.values()))
        return PairRules(pair=pair, price_decimals=int(item["pair_decimals"]),
                         quantity_decimals=int(item["lot_decimals"]),
                         minimum_quantity=Decimal(str(item.get("ordermin", "0"))),
                         minimum_notional=max(minimum_notional, Decimal(str(item.get("costmin", "0")))))

    def reconcile(self) -> Dict[str, Any]:
        return {"balances": self._private("/0/private/Balance"),
                "open_orders": self._private("/0/private/OpenOrders").get("open", {})}
