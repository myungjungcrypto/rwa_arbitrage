# RWA Arbitrage Bot — Oil Perpetual vs Monthly Futures

## 프로젝트 개요

trade.xyz(Hyperliquid 기반)의 원유 퍼페추얼 선물과, 해당 퍼프가 인덱스로 추적하는 CME 월물 선물 간의 베이시스 차익거래(basis arbitrage) 봇.

### 대상 상품

| 구분 | 퍼페추얼 (trade.xyz) | 월물 (키움증권) | 거래소 |
|------|----------------------|----------------|--------|
| WTI | WTIOIL (ticker: CL perp) | CL 근월물 (예: CLK6→CLM6) | NYMEX/CME |
| Brent | BRENTOIL (ticker: BZ perp) | BZ 근월물 (예: BZM6→BZN6) | ICE/CME |

### 핵심 메커니즘

- trade.xyz 퍼프는 CME 근월물 가격을 오라클 인덱스로 사용
- 매월 5~10 영업일에 롤오버 (가중 전환 방식)
- 펀딩레이트: 매시간 정산, 연간 ~5% (전통자산 0.5x 스케일링)
- 베이시스(perp price - index price) 확대 시 진입, 수렴 시 청산

---

## Phase 1: 인프라 구축 및 데이터 수집

### 1.1 Hyperliquid API 연동
- **SDK**: `pip install hyperliquid-python-sdk`
- **GitHub**: https://github.com/hyperliquid-dex/hyperliquid-python-sdk
- **테스트넷 사용**: `constants.TESTNET_API_URL` 로 페이퍼 트레이딩
- **필요 기능**:
  - 시세 조회 (mark price, index price, funding rate)
  - 오더북 조회
  - 주문 생성/취소/조회
  - 포지션 조회
  - WebSocket 실시간 데이터

### 1.2 키움증권 해외선물 OpenAPI-W 연동
- **공식 가이드**: https://download.kiwoom.com/web/openapi/kiwoom_openapi_w_devguide_ver_1.0.pdf
- **Python 래퍼**: `koapy` (PyPI), `pykiwoom`
- **주의**: OCX 기반 → Windows 환경 필요 (또는 Wine/VM)
- **필요 기능**:
  - CL, BZ 월물 시세/호가 조회
  - 주문 생성/취소/조회
  - 잔고/증거금 조회
  - 모의투자 → 실투자 전환

### 1.3 데이터 파이프라인
- Hyperliquid: WebSocket으로 실시간 mark price, index price, funding rate 수신
- 키움: 실시간 시세 FID 구독 (해외선물 호가/체결)
- 통합 데이터 저장: SQLite 또는 TimescaleDB
- 베이시스 계산: `basis = perp_mark_price - futures_price`
- 펀딩레이트 누적 추적

---

## Phase 2: 페이퍼 트레이딩 봇

### 2.1 아키텍처

```
┌─────────────────┐     ┌──────────────────┐
│  Hyperliquid WS │────▶│                  │
│  (perp data)    │     │   Arbitrage      │
└─────────────────┘     │   Engine         │
                        │                  │
┌─────────────────┐     │  - Basis Calc    │──▶ Paper Trade Logger
│  Kiwoom API     │────▶│  - Signal Gen    │──▶ PnL Tracker
│  (futures data) │     │  - Risk Mgmt     │──▶ Dashboard
└─────────────────┘     │                  │
                        └──────────────────┘
```

### 2.2 전략 로직

1. **베이시스 모니터링**: `basis = perp_price - futures_index_price`
2. **진입 조건**:
   - Long basis (perp > futures): Perp SHORT + Futures LONG
   - Short basis (perp < futures): Perp LONG + Futures SHORT
   - 진입 임계값: 과거 N시간 베이시스 평균 ± K*σ
3. **청산 조건**:
   - 베이시스가 평균으로 회귀 시
   - 또는 목표 수익률 도달 시
4. **펀딩레이트 수익**: 포지션 방향에 따라 펀딩 수취/지급 고려
5. **롤오버 관리**: 매월 5~10 영업일 롤 기간에는 포지션 축소 또는 헤지

### 2.3 리스크 관리

- 최대 포지션 사이즈 제한
- 양쪽 레그 동시 체결 보장 (한쪽만 체결 시 긴급 청산)
- 마진 사용률 모니터링 (양 거래소)
- 슬리피지 제한
- 롤오버 기간 리스크 별도 관리
- 네트워크 장애 대응 (자동 헤지/청산)

### 2.4 페이퍼 트레이딩 구현

- Hyperliquid: 테스트넷 API로 실제 주문 시뮬레이션
- 키움: 모의투자 계좌 사용 (OpenAPI-W 모의투자 모드)
- 시뮬레이션 로그: 모든 신호/주문/체결/PnL 기록
- 일일 리포트 자동 생성

---

## Phase 3: 실거래 전환

### 3.1 사전 체크리스트

- [ ] 페이퍼 트레이딩 최소 2주 이상 안정 운영
- [ ] 수익성 검증 (수수료, 슬리피지, 펀딩 포함)
- [ ] 에지 케이스 테스트 (롤오버, 급변동, 네트워크 장애)
- [ ] Hyperliquid 메인넷 API 키 발급
- [ ] 키움 실투자 계좌 해외선물 거래 개설
- [ ] USDC 입금 및 해외선물 증거금 확보
- [ ] 최소 금액으로 라이브 테스트 (1계약)

### 3.2 운영 모드 전환

- 환경변수로 `PAPER` / `LIVE` 모드 전환
- 라이브 모드: 실제 API 엔드포인트 + 실계좌
- 알림 시스템: Telegram/Discord 봇 연동 (주문 체결, 에러, PnL)

### 3.3 모니터링 & 운영

- Grafana 대시보드: 실시간 베이시스, 포지션, PnL, 펀딩
- 자동 알림: 이상 감지 시 Telegram 알림
- 일일/주간 PnL 리포트
- 로그 중앙화 (파일 + 원격)

---

## 기술 스택

| 구분 | 기술 |
|------|------|
| 언어 | Python 3.11+ |
| Hyperliquid | `hyperliquid-python-sdk` |
| 키움증권 | OpenAPI-W + `koapy` 또는 자체 래퍼 |
| 데이터 저장 | SQLite (초기) → TimescaleDB (확장 시) |
| 스케줄링 | APScheduler 또는 asyncio |
| 모니터링 | Grafana + Prometheus |
| 알림 | Telegram Bot API |
| 환경 | Windows (키움 OCX 의존) + WSL 가능 |

---

## 디렉토리 구조 (예정)

```
rwa_arbitrage/
├── CLAUDE.md                  # 이 파일
├── config/
│   ├── settings.yaml          # 전략 파라미터, 임계값
│   └── secrets.yaml           # API 키 (gitignore)
├── src/
│   ├── exchange/
│   │   ├── hyperliquid.py     # Hyperliquid API 래퍼
│   │   └── kiwoom.py          # 키움 OpenAPI-W 래퍼
│   ├── strategy/
│   │   ├── basis_arb.py       # 베이시스 차익거래 로직
│   │   └── signals.py         # 진입/청산 시그널
│   ├── risk/
│   │   └── manager.py         # 리스크 관리
│   ├── data/
│   │   ├── collector.py       # 실시간 데이터 수집
│   │   └── storage.py         # DB 저장
│   ├── paper/
│   │   └── simulator.py       # 페이퍼 트레이딩 엔진
│   └── utils/
│       ├── logger.py          # 로깅
│       └── notifier.py        # 알림 (Telegram)
├── tests/
│   ├── test_basis_calc.py
│   ├── test_order_flow.py
│   └── test_risk.py
├── notebooks/
│   └── basis_analysis.ipynb   # 베이시스 분석/백테스트
├── data/
│   └── historical/            # 과거 데이터
└── requirements.txt
```

---

## 주요 고려사항

### 롤오버 리스크
- trade.xyz 퍼프 인덱스는 매월 5~10 영업일에 가중 롤오버
- 이 기간 동안 인덱스 가격 = w1 * 근월물 + w2 * 차월물
- 키움 쪽 월물도 동일 시점에 롤오버 필요 → 슬리피지/갭 리스크

### 시간대 차이
- Hyperliquid: 24/7 거래
- CME 선물: 주중 거의 23시간 (일요일 오후~금요일 오후 CT)
- 주말/공휴일에는 퍼프만 거래 가능 → 헤지 불가 구간 존재

### 수수료 구조
- Hyperliquid: maker ~0.01%, taker ~0.035%
- 키움 해외선물: 계약당 수수료 (보통 $3~5/계약)
- 펀딩레이트: 시간당 정산, 포지션 방향에 따라 수취 또는 지급

### 자본 효율성
- Hyperliquid: USDC 담보, 최대 10x 레버리지
- CME 선물: 증거금 제도 (CL ~$6,000/계약, BZ ~$5,500/계약)
- 양쪽 모두 증거금 확보 필요 → 자본 배분 전략 중요

---

## 마일스톤

| 단계 | 목표 | 예상 기간 |
|------|------|-----------|
| M1 | Hyperliquid API 연동 + 시세 수집 | 1주 |
| M2 | 키움 OpenAPI-W 연동 + 시세 수집 | 1~2주 |
| M3 | 베이시스 분석 + 백테스트 | 1주 |
| M4 | 페이퍼 트레이딩 봇 완성 | 2주 |
| M5 | 페이퍼 트레이딩 안정화 + 전략 튜닝 | 2~4주 |
| M6 | 실거래 전환 (최소 규모) | 1주 |
| M7 | 실거래 안정화 + 스케일업 | 지속 |
