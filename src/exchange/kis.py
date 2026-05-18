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
        account_number: str = "",
        hts_id: str = "",
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url
        if is_paper:
            self.base_url = "https://openapivts.koreainvestment.com:29443"
        self.is_paper = is_paper
        self.account_number = account_number    # "12345678-08" 또는 "1234567808"
        # KIS 일부 endpoint (inquire-deposit/balance 등)에서 HTS ID 헤더 요구.
        # 빈 문자열이면 헤더 미포함 (시세/구독 같이 HTS ID 불필요한 endpoint는 영향 X).
        self.hts_id = hts_id
        self._access_token: str = ""
        self._token_expires: float = 0.0
        self._approval_key: str = ""

    @property
    def account_cano_prdt(self) -> tuple[str, str]:
        """계좌번호를 CANO(8자리) + ACNT_PRDT_CD(2자리)로 분리. 하이픈 제거.

        해외선물 계좌: 일반적으로 ACNT_PRDT_CD='08'.
        """
        s = (self.account_number or "").replace("-", "").strip()
        if len(s) < 10:
            raise ValueError(
                f"KIS account_number must be 10 chars (CANO+ACNT_PRDT_CD). "
                f"Got {len(s)} chars: {self.account_number!r}"
            )
        return s[:8], s[8:10]

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
        """REST API 호출용 헤더.

        hts_id: KIS 일부 endpoint(inquire-deposit/balance, 주문 일부)는 KIS Developer에
        등록한 HTS ID와 호출 ID 일치 요구 (rt_cd=7 '계좌에 등록된 HTS ID와 일치하지 않습니다').
        secrets.yaml의 kis.hts_id 필드에서 로드.
        """
        h = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P",   # 개인 (KIS 기본)
        }
        if self.hts_id:
            h["hts_id"] = self.hts_id
        return h


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
        # 마지막 quote 메시지 수신 시각 — reconnect 후 verify에 사용
        self._last_msg_ts: float = 0.0

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

    async def _send_subscribe(self, tr_id: str, tr_key: str, tr_type: str = "1"):
        """WebSocket 구독/해제 메시지 전송.

        Args:
            tr_id: KIS 서비스 ID (HDFFF010=호가, HDFFF020=체결)
            tr_key: 종목코드
            tr_type: "1"=등록, "2"=해제
        """
        if not self._ws:
            return
        msg = json.dumps({
            "header": {
                "approval_key": self.auth._approval_key,
                "custtype": "P",
                "tr_type": tr_type,
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

    async def unsubscribe(self, symbol: str):
        """종목 구독 해제 (HDFFF010 + HDFFF020).

        내부 콜백/divisor/캐시 맵에서 symbol 제거.
        tr_type="2"로 서버에 unregister 요청.
        """
        if symbol not in self._callbacks:
            logger.warning(f"KIS unsubscribe: {symbol} not subscribed")
            return

        try:
            await self._send_subscribe("HDFFF010", symbol, tr_type="2")
            await self._send_subscribe("HDFFF020", symbol, tr_type="2")
        except Exception as e:
            logger.error(f"KIS unsubscribe send error [{symbol}]: {e}")

        self._callbacks.pop(symbol, None)
        self._price_divisors.pop(symbol, None)
        self._latest_quotes.pop(symbol, None)
        logger.info(f"KIS unsubscribed: {symbol}")

    async def resubscribe(
        self,
        old_symbol: str,
        new_symbol: str,
        callback: Callable,
        price_divisor: float = 1.0,
    ) -> bool:
        """old_symbol 해제 → new_symbol 구독 (rollover 전용).

        WebSocket 프로토콜 레벨 unregister 실패 시에도 내부 dict는 정리하고
        new_symbol 구독을 진행한다. 완전 실패 시 False 반환.
        """
        if old_symbol == new_symbol:
            logger.info(f"KIS resubscribe skipped (same symbol): {old_symbol}")
            return True

        try:
            await self.unsubscribe(old_symbol)
        except Exception as e:
            logger.error(f"KIS resubscribe unsubscribe error: {e}")

        try:
            await self.subscribe(new_symbol, callback, price_divisor=price_divisor)
            logger.warning(f"[ROLLOVER] KIS resubscribe: {old_symbol} → {new_symbol}")
            return True
        except Exception as e:
            logger.error(f"KIS resubscribe subscribe error: {e}")
            return False

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

    async def _reconnect(self, max_attempts: int = 3, verify_seconds: float = 15.0):
        """WebSocket 재연결 + push 검증.

        KIS WS의 알려진 quirk — 잦은 reconnect 후 subscribe ack는 주지만 실제
        quote push 안 시작되는 경우 (5/18 07:02 사건). 그래서 reconnect 후
        N초 wait + `_last_msg_ts` 확인 + push 안 오면 재시도.

        Args:
            max_attempts: 재시도 최대 횟수
            verify_seconds: subscribe 후 push 기다리는 시간
        """
        for attempt in range(1, max_attempts + 1):
            try:
                if self._ws and not self._ws.closed:
                    await self._ws.close()
                if self._session:
                    await self._session.close()

                # 토큰 재발급 — KIS rate limit (분당 1회) 회피 위해 첫 시도엔
                # 캐시된 token 사용. 1차 fail 시 재시도에선 새 token (만료됐을 수 있음).
                if attempt > 1:
                    self.auth._access_token = ""    # cache clear
                    self.auth._approval_key = ""
                await self.auth.get_access_token()
                await self.auth.get_approval_key()

                self._session = aiohttp.ClientSession()
                self._ws = await self._session.ws_connect(
                    self.ws_url, heartbeat=30,
                )
                logger.info(
                    f"KIS WebSocket reconnected (attempt {attempt}/{max_attempts}, "
                    f"tokens refreshed)"
                )

                # 기존 구독 복원
                ts_before_subscribe = time.time()
                for symbol in self._callbacks:
                    await self._send_subscribe("HDFFF010", symbol)
                    await self._send_subscribe("HDFFF020", symbol)
                logger.info(
                    f"KIS subscriptions restored: {list(self._callbacks.keys())} "
                    f"— verifying push for {verify_seconds:.0f}s..."
                )

                # Verify — N초 안 quote 들어오는지 (KIS server-side stale 검출)
                deadline = time.time() + verify_seconds
                while time.time() < deadline:
                    await asyncio.sleep(1)
                    if self._last_msg_ts > ts_before_subscribe:
                        logger.info(
                            f"KIS push verified (first msg at "
                            f"+{self._last_msg_ts - ts_before_subscribe:.1f}s)"
                        )
                        return
                # 검증 실패 — KIS subscribe ack 줬지만 push 안 옴
                logger.error(
                    f"KIS reconnect attempt {attempt}/{max_attempts} — "
                    f"subscribed but NO quote in {verify_seconds:.0f}s, retrying..."
                )

            except Exception as e:
                logger.error(
                    f"KIS reconnect attempt {attempt}/{max_attempts} failed: {e}"
                )
                # KIS rate limit 회피 위해 점진 backoff
                await asyncio.sleep(min(60, 5 * attempt))

        logger.error(
            f"KIS reconnect FAILED after {max_attempts} attempts. "
            "WS_WATCHDOG will catch stale → flatten + alert. "
            "Manual `pm2 restart rwa-arb` 또는 PM2 cron이 회복할 때까지 quote stale."
        )

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

            # 실 quote 수신 시각 기록 — reconnect verify 용
            self._last_msg_ts = time.time()

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


# ──────────────────────────────────────────────
# ExchangeBase Adapter (Phase A 스캐폴딩)
# ──────────────────────────────────────────────
#
# 기존 KISFuturesClient를 감싸 ExchangeBase protocol을 구현. KIS는 dated_futures
# venue이고 주문 API는 아직 미구현 상태(KIS REST 주문은 M10에서 합류 예정).
# Phase A에서는 quote 수신 + 단순 정보 조회만 충실. place_order는 NotImplementedError.

from src.exchange import base as _base   # noqa: E402


class KISExchange:
    """ExchangeBase 어댑터 — KISFuturesClient 래퍼.

    내부적으로 FuturesQuote → Quote 변환만 수행. KIS는 perp이 아니라 dated
    futures이므로 funding 관련 필드는 모두 0, contract_month는 symbol 그대로.

    주문 API는 페이퍼 단계에서 미구현 → NotImplementedError. paper engine은
    별도 KiwoomMock 경유로 시뮬레이션 fill 처리 (M10에서 KIS REST 주문 합류).
    """

    name = "kis"
    venue_type = _base.VenueType.DATED_FUTURES.value
    margin_asset = "USD"   # 해외선물 외화예수금 (USD 환산); 표시용 단위

    def __init__(self, client: KISFuturesClient):
        self._client = client
        # symbol → ExchangeBase 콜백 (KISFuturesClient 콜백과 별도 유지)
        self._symbol_callbacks: dict[str, list[_base.QuoteCallback]] = {}
        self._contract_sizes: dict[str, float] = {}

    async def connect(self) -> bool:
        return await self._client.connect()

    async def disconnect(self) -> None:
        await self._client.disconnect()

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
        self._symbol_callbacks.setdefault(symbol, []).append(callback)
        self._contract_sizes[symbol] = contract_size

        # KIS native callback은 FuturesQuote를 받음 → Quote 변환 후 ExchangeBase 콜백 호출
        async def _bridge(fq: FuturesQuote) -> None:
            quote = self._to_base_quote(fq)
            for cb in self._symbol_callbacks.get(symbol, []):
                try:
                    result = cb(quote)
                    if result is not None and asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"KIS ExchangeBase callback error [{symbol}]: {e}")

        # KIS callback은 sync — bridge를 동기 wrapper로 등록
        def _sync_bridge(fq: FuturesQuote) -> None:
            asyncio.create_task(_bridge(fq))

        await self._client.subscribe(symbol, _sync_bridge, price_divisor=contract_size)

    async def unsubscribe_quotes(self, symbol: str) -> None:
        self._symbol_callbacks.pop(symbol, None)
        self._contract_sizes.pop(symbol, None)
        await self._client.unsubscribe(symbol)

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        # 우선 캐시, 없으면 REST 폴백
        cached = self._client.get_latest_quote(symbol)
        if cached:
            return self._to_base_quote(cached)
        fq = await self._client.get_quote_rest(symbol)
        return self._to_base_quote(fq) if fq else None

    async def place_order(
        self,
        symbol: str,
        side: _base.OrderSideLiteral,
        size: float,
        order_type: _base.OrderTypeLiteral = "market",
        limit_price: Optional[float] = None,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> _base.OrderResult:
        """KIS 해외선물 실주문 (Phase 11a).

        POST /uapi/overseas-futureoption/v1/trading/order
        tr_id: OTFM3001U(실전), VTFM3001U(모의) — auth.is_paper로 분기

        Mapping:
          side='buy'   → SLL_BUY_DVSN_CD='02'
          side='sell'  → SLL_BUY_DVSN_CD='01'
          order_type='market' → PRIC_DVSN_CD='2', FM_LIMIT_ORD_PRIC='0'
          order_type='limit'  → PRIC_DVSN_CD='1', FM_LIMIT_ORD_PRIC=str(limit_price)
        """
        auth = self._client.auth
        try:
            cano, acnt_prdt = auth.account_cano_prdt
        except ValueError as e:
            return _base.OrderResult(
                success=False, exchange=self.name, symbol=symbol,
                error=f"account config error: {e}",
            )

        # reduce_only — KIS REST는 직접 flag 없음. adapter 측에서 사전 잔고
        # 체크로 동등 효과. 5/18 incident: EXIT retry가 reduce_only=False로
        # 발사되어 KIS가 reverse position 누적 (long 0 → sell → short 2).
        # 이제 reduce_only=True면 보유 포지션과 방향/크기 검증 후 reject.
        if reduce_only:
            try:
                positions = await self.get_positions()
                held = next(
                    (p for p in positions if p.symbol == symbol), None,
                )
                if held is None or held.size == 0:
                    return _base.OrderResult(
                        success=False, exchange=self.name, symbol=symbol,
                        error="reduce_only: no position to reduce",
                    )
                # 방향 일치 검증 — long(positive) 보유 시 sell만, short(negative)면 buy만
                want_buy = (side == "buy")
                holds_short = (held.size < 0)
                if want_buy != holds_short:
                    return _base.OrderResult(
                        success=False, exchange=self.name, symbol=symbol,
                        error=(f"reduce_only: side={side} doesn't match "
                               f"position size={held.size}"),
                    )
                # 사이즈 cap — 보유 이상 못 보냄
                if size > abs(held.size):
                    logger.warning(
                        f"[KIS] reduce_only: capping size {size} → {abs(held.size)} "
                        f"(actual position)"
                    )
                    size = abs(held.size)
            except Exception as e:
                # 잔고 조회 실패 — 보수적으로 주문 차단 (silent over-fill 방지)
                logger.error(f"[KIS] reduce_only pre-check failed: {e}")
                return _base.OrderResult(
                    success=False, exchange=self.name, symbol=symbol,
                    error=f"reduce_only pre-check failed: {e}",
                )

        sll_buy = "02" if side == "buy" else "01"
        if order_type == "market":
            pric_dvsn = "2"
            fm_limit = "0"
        else:
            pric_dvsn = "1"
            fm_limit = f"{limit_price}" if limit_price else "0"

        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt,
            "OVRS_FUTR_FX_PDNO": symbol,
            "SLL_BUY_DVSN_CD": sll_buy,
            "PRIC_DVSN_CD": pric_dvsn,
            "FM_LIMIT_ORD_PRIC": fm_limit,
            "FM_STOP_ORD_PRIC": "0",
            "FM_ORD_QTY": str(int(size)),
            "CCLD_CNDT_CD": "6",        # EOD 지정가
            "FM_LQD_USTL_CCLD_DT": "",
            "FM_LQD_USTL_CCNO": "",
            "CPLX_ORD_DVSN_CD": "0",
            "ECIS_RSVN_ORD_YN": "N",
            "FM_HDGE_ORD_SCRN_YN": "N",
        }

        tr_id = "VTFM3001U" if auth.is_paper else "OTFM3001U"
        try:
            await auth.get_access_token()
            headers = auth.get_rest_headers(tr_id)
            url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/order"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            logger.error(f"[KIS] place_order network error: {e}")
            return _base.OrderResult(
                success=False, exchange=self.name, symbol=symbol,
                error=f"network: {e}",
            )

        rt_cd = str(data.get("rt_cd", ""))
        msg = data.get("msg1", "")
        if rt_cd == "0":
            output = data.get("output", {}) or {}
            order_id = (output.get("ODNO") or output.get("KRX_FWDG_ORD_ORGNO") or "")
            logger.warning(
                f"[KIS] ORDER OK {symbol} {side} {int(size)} order_id={order_id} ({msg})"
            )
            return _base.OrderResult(
                success=True, exchange=self.name, symbol=symbol,
                order_id=order_id,
                filled_size=size,
                # 시장가는 응답에 fill price 없음. 후속 inquire-balance 또는 체결 통보
                # WebSocket으로 확인 필요. 여기선 0으로 두고 호출자가 실시간 시세
                # 또는 잔고 조회로 보정.
                filled_price=float(limit_price) if limit_price else 0.0,
                error="",
            )
        else:
            logger.error(f"[KIS] ORDER FAILED {symbol} rt_cd={rt_cd} msg={msg}")
            return _base.OrderResult(
                success=False, exchange=self.name, symbol=symbol,
                error=f"rt_cd={rt_cd} msg={msg}",
            )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """KIS 해외선물 주문 취소.

        POST /uapi/overseas-futureoption/v1/trading/order-rvsecncl
        tr_id: OTFM3003U(실전 취소), VTFM3003U(모의 취소).
        """
        if not order_id:
            return False
        auth = self._client.auth
        try:
            cano, acnt_prdt = auth.account_cano_prdt
        except ValueError:
            return False

        from datetime import datetime as _dt
        # 오늘 날짜로 가정 — 익일 이후 미체결분은 자동 만료/별도 처리 필요
        ord_dt = _dt.now().strftime("%Y%m%d")
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt,
            "ORGN_ORD_DT": ord_dt,
            "ORGN_ODNO": order_id,
            "FM_LIMIT_ORD_PRIC": "0",
            "FM_STOP_ORD_PRIC": "0",
            "FM_LQD_LMT_ORD_PRIC": "0",
            "FM_LQD_STOP_ORD_PRIC": "0",
            "FM_HDGE_ORD_SCRN_YN": "N",
            "FM_MKPR_CVSN_YN": "N",
        }
        tr_id = "VTFM3003U" if auth.is_paper else "OTFM3003U"
        try:
            await auth.get_access_token()
            headers = auth.get_rest_headers(tr_id)
            url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/order-rvsecncl"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            logger.error(f"[KIS] cancel_order network error: {e}")
            return False

        rt_cd = str(data.get("rt_cd", ""))
        if rt_cd == "0":
            logger.warning(f"[KIS] CANCEL OK order_id={order_id} ({data.get('msg1','')})")
            return True
        logger.error(f"[KIS] CANCEL FAILED order_id={order_id} rt_cd={rt_cd} msg={data.get('msg1','')}")
        return False

    async def get_positions(self) -> list[_base.Position]:
        """KIS 해외선물 잔고/포지션 조회.

        POST /uapi/overseas-futureoption/v1/trading/inquire-unpd
        tr_id: OTFM1412R (실전), VTFM1412R (모의)
        FUOP_DVSN: '01' (선물만)
        """
        auth = self._client.auth
        try:
            cano, acnt_prdt = auth.account_cano_prdt
        except ValueError:
            return []

        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt,
            "FUOP_DVSN": "01",          # 선물
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        tr_id = "VTFM1412R" if auth.is_paper else "OTFM1412R"
        try:
            await auth.get_access_token()
            headers = auth.get_rest_headers(tr_id)
            url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/inquire-unpd"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            logger.error(f"[KIS] get_positions error: {e}")
            return []

        if str(data.get("rt_cd", "")) != "0":
            return []

        out: list[_base.Position] = []
        for row in (data.get("output1") or data.get("output") or []) or []:
            if not isinstance(row, dict):
                continue
            try:
                # 미결제 수량 (양수=long, 음수=short — KIS는 양수+SLL_BUY 분리)
                qty = float(row.get("CCLD_QTY", 0) or row.get("FM_OBJ_QTY", 0) or 0)
                if qty == 0:
                    continue
                sll_buy = str(row.get("SLL_BUY_DVSN_CD", "02"))   # 02=매수, 01=매도
                signed_qty = qty if sll_buy == "02" else -qty
                out.append(_base.Position(
                    exchange=self.name,
                    symbol=row.get("OVRS_FUTR_FX_PDNO", ""),
                    size=signed_qty,
                    entry_price=float(row.get("AVG_BUY_UNPR", row.get("FM_AVG_PRIC", 0)) or 0),
                    mark_price=float(row.get("PURCHASE_AVG_PRICE", 0) or 0),
                    unrealized_pnl=float(row.get("FM_TOT_EVLU_PFLS_AMT", 0) or 0),
                    margin_used=float(row.get("FM_OBJ_AMT", 0) or 0),
                    leverage=1.0,
                ))
            except (TypeError, ValueError) as e:
                logger.warning(f"[KIS] position parse error: {e} (row={row})")
        return out

    async def get_account_value(self) -> float:
        """해외선물 예수금 합계 조회.

        GET /uapi/overseas-futureoption/v1/trading/inquire-deposit
        tr_id: OTFM3115R(실전) / VTFM3115R(모의)
        필수 파라미터: CANO, ACNT_PRDT_CD, OVRS_EXCG_CD, CRCY_CD, INQR_DT(YYYYMMDD)
        응답 output1.frcr_dncl_amt_smtl  외화예수금 합계 (USD 환산)
              .tot_dncl_amt              총 예수금

        실패 시 RuntimeError raise — balance_poll_loop가 잡아서 note에 사유 기록.
        """
        from datetime import datetime, timezone
        auth = self._client.auth
        cano, acnt_prdt = auth.account_cano_prdt
        if not cano:
            raise RuntimeError("KIS account_number not set")
        # OTFM1411R: 해외선물옵션 예수금현황 (output2에 통화별 잔고 list)
        tr_id = "VTFM1411R" if auth.is_paper else "OTFM1411R"
        await auth.get_access_token()   # 토큰 ensure
        headers = auth.get_rest_headers(tr_id)
        # KIS 일부 endpoint는 INQR_DT(YYYYMMDD) 필수 — 누락 시 rt_cd=7
        # KST 기준 오늘 (KIS 서버 시각)
        inqr_dt = datetime.now(timezone.utc).strftime("%Y%m%d")
        params = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt,
            "OVRS_EXCG_CD": "CME",
            "CRCY_CD": "USD",
            "INQR_DT": inqr_dt,
        }
        url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/inquire-deposit"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as r:
                data = await r.json()
        if data.get("rt_cd") != "0":
            raise RuntimeError(
                f"rt_cd={data.get('rt_cd')} msg={data.get('msg1','').strip()[:80]}"
            )
        # OTFM1411R 응답: data['output'] (단수 dict, 25 keys).
        # 우선순위: 총자산평가금액 → 주문가능금액 → 익일예수금.
        # fm_tot_asst_evlu_amt 가 가장 의미있는 잔고 (예수금 + 평가손익).
        out = data.get("output") or {}
        if isinstance(out, dict):
            for key in ("fm_tot_asst_evlu_amt", "fm_ord_psbl_amt",
                        "fm_nxdy_dncl_amt", "fm_drwg_psbl_amt",
                        "fm_dnca_rmnd"):
                v = out.get(key)
                if v:
                    try: return float(v)
                    except (TypeError, ValueError): continue
        return 0.0

    async def get_funding_info(self, symbol: str) -> Optional[_base.FundingInfo]:
        """KIS는 dated_futures venue → funding 개념 없음. 항상 None."""
        return None

    # ── 내부 변환 ──

    def _to_base_quote(self, fq: FuturesQuote) -> _base.Quote:
        return _base.Quote(
            exchange=self.name,
            symbol=fq.symbol,
            mid_price=fq.price,
            bid=fq.bid,
            ask=fq.ask,
            bid_qty=float(fq.bid_qty),
            ask_qty=float(fq.ask_qty),
            index_price=0.0,
            funding_rate=0.0,
            funding_interval_hours=0.0,
            contract_month=fq.contract_month or fq.symbol,
            volume_24h=float(fq.volume),
            timestamp=fq.timestamp,
        )
