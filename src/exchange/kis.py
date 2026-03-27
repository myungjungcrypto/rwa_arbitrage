"""KIS (한국투자증권) 해외선물 REST/WebSocket 클라이언트.

실시간 CME 선물 호가(bid/ask)를 수신하여 collector에 공급.
- REST: 토큰 발급, 현재가/호가 조회 (폴백)
- WebSocket: 실시간 호가 (HDFFF010), 실시간 체결 (HDFFF020)

참고: https://github.com/koreainvestment/open-trading-api
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import aiohttp

logger = logging.getLogger("arbitrage.kis")


# ──────────────────────────────────────────────
# 데이터 구조
# ──────────────────────────────────────────────

@dataclass
class FuturesQuote:
    """선물 호가 데이터."""
    symbol: str
    price: float          # 최근 체결가 (또는 mid price)
    bid: float            # 매수1호가
    ask: float            # 매도1호가
    bid_qty: int = 0
    ask_qty: int = 0
    volume: int = 0
    contract_month: str = ""
    timestamp: float = 0.0


# WebSocket 호가 메시지 필드 (HDFFF010) — '^' 구분, 35개
HOKA_COLUMNS = [
    "series_cd", "recv_date", "recv_time", "prev_price",
    "bid_qntt_1", "bid_num_1", "bid_price_1",
    "ask_qntt_1", "ask_num_1", "ask_price_1",
    "bid_qntt_2", "bid_num_2", "bid_price_2",
    "ask_qntt_2", "ask_num_2", "ask_price_2",
    "bid_qntt_3", "bid_num_3", "bid_price_3",
    "ask_qntt_3", "ask_num_3", "ask_price_3",
    "bid_qntt_4", "bid_num_4", "bid_price_4",
    "ask_qntt_4", "ask_num_4", "ask_price_4",
    "bid_qntt_5", "bid_num_5", "bid_price_5",
    "ask_qntt_5", "ask_num_5", "ask_price_5",
    "sttl_price",
]

# WebSocket 체결 메시지 필드 (HDFFF020) — '^' 구분, 25개
CCNL_COLUMNS = [
    "series_cd", "bsns_date", "mrkt_open_date", "mrkt_open_time",
    "mrkt_close_date", "mrkt_close_time", "prev_price",
    "recv_date", "recv_time", "active_flag", "last_price",
    "last_qntt", "prev_diff_price", "prev_diff_rate",
    "open_price", "high_price", "low_price", "vol",
    "prev_sign", "quotsign", "recv_time2", "psttl_price",
    "psttl_sign", "psttl_diff_price", "psttl_diff_rate",
]


# ──────────────────────────────────────────────
# KIS 인증
# ──────────────────────────────────────────────

class KISAuth:
    """KIS OAuth2 토큰 + WebSocket approval_key 관리."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        base_url: str = "https://openapi.koreainvestment.com:9443",
        is_paper: bool = False,
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url
        if is_paper:
            self.base_url = "https://openapivts.koreainvestment.com:29443"
        self._access_token: str = ""
        self._token_expires: float = 0.0
        self._approval_key: str = ""

    async def get_access_token(self) -> str:
        """REST API용 access_token 발급 (24시간 유효)."""
        if self._access_token and time.time() < self._token_expires:
            return self._access_token

        url = f"{self.base_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body) as resp:
                data = await resp.json()
                if "access_token" not in data:
                    raise RuntimeError(f"KIS token error: {data}")
                self._access_token = data["access_token"]
                # 23시간 후 갱신 (실제 유효: 24시간)
                self._token_expires = time.time() + 23 * 3600
                logger.info("KIS access_token issued (expires in 23h)")
                return self._access_token

    async def get_approval_key(self) -> str:
        """WebSocket 접속용 approval_key 발급."""
        url = f"{self.base_url}/oauth2/Approval"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body) as resp:
                data = await resp.json()
                if "approval_key" not in data:
                    raise RuntimeError(f"KIS approval_key error: {data}")
                self._approval_key = data["approval_key"]
                logger.info("KIS approval_key issued")
                return self._approval_key

    def get_rest_headers(self, tr_id: str) -> dict:
        """REST API 호출용 헤더."""
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }


# ──────────────────────────────────────────────
# KIS Futures Client
# ──────────────────────────────────────────────

class KISFuturesClient:
    """KIS 해외선물 실시간 호가 수신 클라이언트.

    Usage:
        client = KISFuturesClient(auth, ws_url="ws://ops.koreainvestment.com:21000")
        await client.connect()
        await client.subscribe("MCLM26", on_quote_callback)
        ...
        await client.disconnect()
    """

    def __init__(
        self,
        auth: KISAuth,
        ws_url: str = "ws://ops.koreainvestment.com:21000",
        is_paper: bool = False,
    ):
        self.auth = auth
        self.ws_url = ws_url
        if is_paper:
            self.ws_url = "ws://ops.koreainvestment.com:31000"

        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._callbacks: dict[str, list[Callable]] = {}  # symbol → [callbacks]
        self._price_divisors: dict[str, float] = {}      # symbol → divisor (KIS는 계약총액으로 호가)
        self._latest_quotes: dict[str, FuturesQuote] = {}
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._reconnect_delay = 5

    async def connect(self) -> bool:
        """인증 + WebSocket 연결."""
        try:
            # 1) access_token 발급 (유료시세 동기화 필수)
            await self.auth.get_access_token()

            # 2) approval_key 발급
            await self.auth.get_approval_key()

            # 3) WebSocket 연결
            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(
                self.ws_url,
                heartbeat=30,
            )
            self._running = True
            self._recv_task = asyncio.create_task(self._recv_loop())
            logger.info(f"KIS WebSocket connected: {self.ws_url}")
            return True

        except Exception as e:
            logger.error(f"KIS connection failed: {e}")
            return False

    async def disconnect(self):
        """연결 종료."""
        self._running = False
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("KIS WebSocket disconnected")

    async def subscribe(self, symbol: str, callback: Callable, price_divisor: float = 1.0):
        """종목 실시간 호가 + 체결 구독.

        Args:
            symbol: KIS 종목코드 (예: "MCLM26", "BZN26")
            callback: fn(FuturesQuote) — 호가 업데이트 시 호출
            price_divisor: 가격 나눗수 (KIS는 계약총액 기준 호가 → 배럴당 가격 변환)
                           MCL: 100 (100배럴/계약), BZ: 1000 (1000배럴/계약)
        """
        if symbol not in self._callbacks:
            self._callbacks[symbol] = []
        self._callbacks[symbol].append(callback)
        self._price_divisors[symbol] = price_divisor

        # 호가 구독 (HDFFF010)
        await self._send_subscribe("HDFFF010", symbol)
        # 체결 구독 (HDFFF020)
        await self._send_subscribe("HDFFF020", symbol)

        logger.info(f"KIS subscribed: {symbol} (hoka + ccnl)")

    async def _send_subscribe(self, tr_id: str, tr_key: str):
        """WebSocket 구독 메시지 전송."""
        if not self._ws:
            return
        msg = json.dumps({
            "header": {
                "approval_key": self.auth._approval_key,
                "custtype": "P",
                "tr_type": "1",  # 1=등록, 0=해제
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                }
            }
        })
        await self._ws.send_str(msg)
        await asyncio.sleep(0.5)  # KIS 요구: 구독 간 0.5초 간격

    async def _recv_loop(self):
        """WebSocket 메시지 수신 루프."""
        while self._running:
            try:
                if not self._ws or self._ws.closed:
                    logger.warning("KIS WebSocket closed, reconnecting...")
                    await asyncio.sleep(self._reconnect_delay)
                    await self._reconnect()
                    continue

                msg = await self._ws.receive(timeout=60)

                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"KIS WS error: {self._ws.exception()}")
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    logger.warning("KIS WS closed by server")
                    break

            except asyncio.TimeoutError:
                # 60초간 데이터 없음 — 정상 (장 외 시간 등)
                continue
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"KIS recv error: {e}")
                await asyncio.sleep(self._reconnect_delay)

        # 루프 종료 → 재연결 시도
        if self._running:
            logger.info("KIS reconnecting...")
            await asyncio.sleep(self._reconnect_delay)
            asyncio.create_task(self._reconnect())

    async def _reconnect(self):
        """WebSocket 재연결."""
        try:
            if self._ws and not self._ws.closed:
                await self._ws.close()
            if self._session:
                await self._session.close()

            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(
                self.ws_url, heartbeat=30,
            )
            logger.info("KIS WebSocket reconnected")

            # 기존 구독 복원
            for symbol in self._callbacks:
                await self._send_subscribe("HDFFF010", symbol)
                await self._send_subscribe("HDFFF020", symbol)
            logger.info(f"KIS subscriptions restored: {list(self._callbacks.keys())}")

        except Exception as e:
            logger.error(f"KIS reconnect failed: {e}")

    def _handle_message(self, raw: str):
        """WebSocket 메시지 파싱.

        KIS 메시지 포맷:
        - 실데이터: "0|HDFFF010|1|<data>" ('^' 구분)
                    "0|HDFFF020|N|<data>" ('^' 구분, N=체결 건수)
        - 응답: JSON (구독 확인, 에러)
        - PINGPONG: JSON
        """
        if not raw:
            return

        first_char = raw[0]

        if first_char == '0':
            # 실시간 데이터 (비암호화)
            parts = raw.split('|')
            if len(parts) < 4:
                return
            tr_id = parts[1]
            data_str = parts[3]

            if tr_id == "HDFFF010":
                self._parse_hoka(data_str)
            elif tr_id == "HDFFF020":
                data_cnt = int(parts[2])
                self._parse_ccnl(data_str, data_cnt)

        elif first_char == '1':
            # 암호화 데이터 (체결통보 등) — 현재 미사용
            pass

        else:
            # JSON 응답 (구독 확인, PINGPONG 등)
            try:
                obj = json.loads(raw)
                tr_id = obj.get("header", {}).get("tr_id", "")

                if tr_id == "PINGPONG":
                    # PONG 응답
                    if self._ws:
                        asyncio.create_task(self._ws.send_str(raw))
                    return

                rt_cd = obj.get("body", {}).get("rt_cd", "")
                msg1 = obj.get("body", {}).get("msg1", "")
                tr_key = obj.get("header", {}).get("tr_key", "")

                if rt_cd == "0":
                    logger.debug(f"KIS subscribe OK: {tr_key} - {msg1}")
                elif rt_cd == "1":
                    if msg1 != "ALREADY IN SUBSCRIBE":
                        logger.error(f"KIS subscribe ERROR: {tr_key} - {msg1}")
            except json.JSONDecodeError:
                logger.warning(f"KIS unknown message: {raw[:100]}")

    def _parse_hoka(self, data_str: str):
        """호가 데이터 파싱 (HDFFF010).

        data_str: '^' 구분, 35개 필드
        """
        values = data_str.split('^')
        if len(values) < 10:
            return

        symbol = values[0].strip()
        try:
            bid_price = float(values[6]) if values[6] else 0.0   # bid_price_1
            ask_price = float(values[9]) if values[9] else 0.0   # ask_price_1
            bid_qty = int(values[4]) if values[4] else 0          # bid_qntt_1
            ask_qty = int(values[7]) if values[7] else 0          # ask_qntt_1
        except (ValueError, IndexError):
            return

        if bid_price <= 0 or ask_price <= 0:
            return

        # KIS는 계약총액 기준 호가 → 배럴당 가격으로 변환
        divisor = self._price_divisors.get(symbol, 1.0)
        if divisor != 1.0:
            bid_price /= divisor
            ask_price /= divisor

        mid_price = (bid_price + ask_price) / 2.0

        quote = FuturesQuote(
            symbol=symbol,
            price=mid_price,
            bid=bid_price,
            ask=ask_price,
            bid_qty=bid_qty,
            ask_qty=ask_qty,
            contract_month=symbol,
            timestamp=time.time(),
        )

        self._latest_quotes[symbol] = quote
        self._notify_callbacks(symbol, quote)

    def _parse_ccnl(self, data_str: str, count: int):
        """체결 데이터 파싱 (HDFFF020).

        체결가를 최신 호가의 price 필드에 반영.
        """
        values = data_str.split('^')
        if len(values) < 12:
            return

        # 마지막 체결 데이터 사용 (복수 건이면 마지막)
        offset = (count - 1) * len(CCNL_COLUMNS)
        if offset + 11 >= len(values):
            offset = 0

        symbol = values[offset + 0].strip()
        try:
            last_price = float(values[offset + 10]) if values[offset + 10] else 0.0
            last_qty = int(values[offset + 11]) if values[offset + 11] else 0
            volume = int(values[offset + 17]) if values[offset + 17] else 0
        except (ValueError, IndexError):
            return

        if symbol in self._latest_quotes and last_price > 0:
            # KIS는 계약총액 기준 → 배럴당 가격으로 변환
            divisor = self._price_divisors.get(symbol, 1.0)
            if divisor != 1.0:
                last_price /= divisor

            q = self._latest_quotes[symbol]
            q.price = last_price
            q.volume = volume
            q.timestamp = time.time()
            self._notify_callbacks(symbol, q)

    def _notify_callbacks(self, symbol: str, quote: FuturesQuote):
        """등록된 콜백 호출."""
        for cb in self._callbacks.get(symbol, []):
            try:
                cb(quote)
            except Exception as e:
                logger.error(f"KIS callback error [{symbol}]: {e}")

    # ──────────────────────────────────────────────
    # REST API (폴백용)
    # ──────────────────────────────────────────────

    async def get_quote_rest(self, symbol: str) -> Optional[FuturesQuote]:
        """REST API로 호가 조회 (WebSocket 불가 시 폴백).

        Args:
            symbol: KIS 종목코드 (예: "MCLM26")

        Returns:
            FuturesQuote 또는 None
        """
        try:
            token = await self.auth.get_access_token()
            url = f"{self.auth.base_url}/uapi/overseas-futureoption/v1/quotations/inquire-asking-price"
            headers = self.auth.get_rest_headers("HHDFC86000000")
            params = {"SRS_CD": symbol}

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    data = await resp.json()

            output = data.get("output1", {})
            if not output:
                return None

            bid = float(output.get("bidp1", 0))
            ask = float(output.get("askp1", 0))
            if bid <= 0 or ask <= 0:
                return None

            return FuturesQuote(
                symbol=symbol,
                price=(bid + ask) / 2,
                bid=bid,
                ask=ask,
                contract_month=symbol,
                timestamp=time.time(),
            )

        except Exception as e:
            logger.error(f"KIS REST quote error [{symbol}]: {e}")
            return None

    def get_latest_quote(self, symbol: str) -> Optional[FuturesQuote]:
        """캐시된 최신 호가 반환."""
        return self._latest_quotes.get(symbol)
