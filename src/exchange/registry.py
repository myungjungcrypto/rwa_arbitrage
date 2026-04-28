"""거래소 어댑터 레지스트리.

이름→`ExchangeBase` 인스턴스 맵핑. main.py가 페어 config를 읽고 필요한 거래소
어댑터들을 주입할 때 사용.

Phase A 스캐폴딩: 빈 레지스트리 인터페이스만. 실제 등록은 main.py 또는 신규
거래소 어댑터 합류 시점(Phase D~G).
"""

from __future__ import annotations

from typing import Iterable

from src.exchange.base import ExchangeBase


class ExchangeRegistry:
    """단순 dict 래퍼 — 거래소 이름으로 어댑터 인스턴스 조회.

    Usage:
        reg = ExchangeRegistry()
        reg.register(hl_adapter)
        reg.register(kis_adapter)
        ...
        ex = reg.get("hyperliquid")
        await ex.subscribe_quotes("xyz:CL", on_quote)
    """

    def __init__(self) -> None:
        self._exchanges: dict[str, ExchangeBase] = {}

    def register(self, exchange: ExchangeBase) -> None:
        """`exchange.name`을 키로 등록. 같은 이름 재등록 시 덮어씀."""
        if not getattr(exchange, "name", None):
            raise ValueError("Exchange must have non-empty .name attribute")
        self._exchanges[exchange.name] = exchange

    def get(self, name: str) -> ExchangeBase:
        """이름으로 어댑터 조회. 없으면 KeyError."""
        if name not in self._exchanges:
            raise KeyError(
                f"Exchange {name!r} not registered. "
                f"Available: {sorted(self._exchanges.keys())}"
            )
        return self._exchanges[name]

    def has(self, name: str) -> bool:
        return name in self._exchanges

    def names(self) -> list[str]:
        return sorted(self._exchanges.keys())

    def all(self) -> Iterable[ExchangeBase]:
        return self._exchanges.values()

    def __contains__(self, name: str) -> bool:
        return name in self._exchanges

    def __len__(self) -> int:
        return len(self._exchanges)
