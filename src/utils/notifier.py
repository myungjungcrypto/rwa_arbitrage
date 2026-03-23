from __future__ import annotations
"""알림 모듈 (Telegram).

비동기 + 동기 양쪽 모두 지원.
Paper Trading Engine (sync 콜백) 및 DataCollector (async) 모두에서 사용 가능.
"""


import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger("arbitrage.notifier")

# Lazy import
_aiohttp = None


def _get_aiohttp():
    global _aiohttp
    if _aiohttp is None:
        try:
            import aiohttp
            _aiohttp = aiohttp
        except ImportError:
            logger.warning("aiohttp not installed — Telegram async disabled")
    return _aiohttp


class TelegramNotifier:
    """텔레그램 봇 알림.

    async send()와 sync send_sync() 양쪽 지원.
    """

    API_BASE = "https://api.telegram.org/bot{token}"

    def __init__(self, bot_token: str = "", chat_id: str = "", enabled: bool = False):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token) and bool(chat_id)
        self._last_send: float = 0
        self._rate_limit: float = 1.0  # seconds

        if self.enabled:
            logger.info("Telegram notifier enabled")
        else:
            logger.debug("Telegram notifier disabled")

    async def send(self, message: str, parse_mode: str = "HTML"):
        """비동기 메시지 전송."""
        if not self.enabled:
            logger.debug(f"Notification (disabled): {message[:80]}")
            return

        aiohttp = _get_aiohttp()
        if aiohttp is None:
            return

        url = f"{self.API_BASE.format(token=self.bot_token)}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": parse_mode,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    self._last_send = time.time()
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"Telegram send failed: {text[:200]}")
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    def send_sync(self, message: str, parse_mode: str = "HTML"):
        """동기 메시지 전송 (sync 콜백에서 사용).

        현재 이벤트 루프가 있으면 task로 스케줄링,
        없으면 무시 (로그만 남김).
        """
        if not self.enabled:
            logger.debug(f"Notification (disabled): {message[:80]}")
            return

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.send(message, parse_mode))
        except RuntimeError:
            # 이벤트 루프가 없는 경우 — 로그만
            logger.info(f"[Telegram] {message[:200]}")

    # ── 편의 메서드 ──

    async def send_trade_alert(
        self,
        product: str,
        action: str,
        perp_side: str,
        futures_side: str,
        basis_bps: float,
        size: float,
    ):
        """거래 알림."""
        emoji = "🟢" if action == "OPEN" else "🔴"
        msg = (
            f"{emoji} <b>{action} {product.upper()}</b>\n"
            f"Perp: {perp_side} | Futures: {futures_side}\n"
            f"Basis: {basis_bps:.1f}bp | Size: {size}\n"
        )
        await self.send(msg)

    def notify_trade_open(self, product: str, direction: str, basis_bps: float,
                          perp_price: float, futures_price: float, contracts: int):
        """트레이드 진입 알림 (sync)."""
        msg = (
            f"🟢 <b>ENTRY {product.upper()}</b>\n"
            f"Direction: {direction}\n"
            f"Basis: {basis_bps:+.1f}bp\n"
            f"Perp: {perp_price:.2f} | Futures: {futures_price:.2f}\n"
            f"Size: {contracts} contracts"
        )
        self.send_sync(msg)

    def notify_trade_close(self, product: str, direction: str, pnl_usd: float,
                           reason: str, hold_hours: float):
        """트레이드 청산 알림 (sync)."""
        emoji = "✅" if pnl_usd >= 0 else "🔴"
        msg = (
            f"{emoji} <b>EXIT {product.upper()}</b>\n"
            f"Direction: {direction}\n"
            f"PnL: <b>${pnl_usd:+.2f}</b>\n"
            f"Hold: {hold_hours:.1f}h\n"
            f"Reason: {reason}"
        )
        self.send_sync(msg)

    async def send_error_alert(self, error: str):
        """에러 알림."""
        msg = f"⚠️ <b>ERROR</b>\n<code>{error[:500]}</code>"
        await self.send(msg)

    async def send_daily_report(
        self,
        date: str,
        trading_pnl: float,
        funding_pnl: float,
        fees: float,
        num_trades: int,
    ):
        """일일 리포트."""
        net = trading_pnl + funding_pnl - fees
        emoji = "📈" if net >= 0 else "📉"
        msg = (
            f"{emoji} <b>Daily Report {date}</b>\n"
            f"Trading PnL: ${trading_pnl:+,.2f}\n"
            f"Funding PnL: ${funding_pnl:+,.2f}\n"
            f"Fees: -${fees:,.2f}\n"
            f"<b>Net: ${net:+,.2f}</b>\n"
            f"Trades: {num_trades}"
        )
        await self.send(msg)
