# RWA Arbitrage Bot — Oil Perpetual vs Monthly Futures

## 프로젝트 개요

trade.xyz(Hyperliquid 기반)의 원유 퍼페추얼 선물과, 해당 퍼프가 인덱스로 추적하는 CME 월물 선물 간의 베이시스 차익거래(basis arbitrage) 봇.

### 대상 상품

| 구분 | 퍼페추얼 (trade.xyz) | 월물 (KIS 한국투자증권) | 거래소 |
|------|----------------------|------------------------|--------|
| WTI | WTIOIL (ticker: CL perp) | MCL 근월물 (마이크로, 100배럴) | NYMEX/CME |
| Brent | BRENTOIL (ticker: BZ perp) | BZ 근월물 (1,000배럴) | ICE/CME |

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

### 1.2 KIS (한국투자증권) 해외선물 REST/WebSocket API 연동
- **개발자 포털**: https://apiportal.koreainvestment.com/intro
- **GitHub**: https://github.com/koreainvestment/open-trading-api
- **Python 라이브러리**: `python-kis` (PyPI)
- **장점**: REST/WebSocket 기반 → Linux EC2에서 직접 구동 가능 (Windows 불필요)
- **필요 기능**:
  - MCL, BZ 월물 실시간 호가(bid/ask) 조회 (WebSocket `HDFFF010`)
  - 실시간 체결가 조회 (WebSocket `HDFFF020`)
  - 현재가 REST 조회 (`HHDFC55010000`, `HHDFC86000000`)
  - 주문 생성/취소/조회 (REST)
  - 잔고/증거금 조회
- **전제조건**: KIS 계좌 개설, API 키 발급, CME 유료시세 신청 필수

### 1.3 데이터 파이프라인
- Hyperliquid: WebSocket으로 실시간 mark price, index price, funding rate 수신
- KIS: WebSocket으로 실시간 MCL/BZ 호가(bid/ask) 수신 → `collector.update_futures_price()` 호출
- **핵심**: Perp(Hyperliquid)와 Futures(KIS)가 **독립적 데이터 소스**에서 수신되어야 진짜 basis 측정 가능
- 통합 데이터 저장: SQLite
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
│  KIS WebSocket  │────▶│  - Signal Gen    │──▶ PnL Tracker
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

- Futures 시세: KIS WebSocket 실시간 호가 (실제 CME bid/ask)
- Perp 시세: Hyperliquid WebSocket (실제 orderbook)
- 주문 시뮬레이션: KiwoomMock (paper trading 주문용으로 유지)
- 시뮬레이션 로그: 모든 신호/주문/체결/PnL 기록
- 일일 리포트 자동 생성

---

## Phase 3: 실거래 전환

### 3.1 사전 체크리스트

- [ ] 페이퍼 트레이딩 최소 2주 이상 안정 운영
- [ ] 수익성 검증 (수수료, 슬리피지, 펀딩 포함)
- [ ] 에지 케이스 테스트 (롤오버, 급변동, 네트워크 장애)
- [ ] Hyperliquid 메인넷 API 키 발급
- [ ] KIS 실투자 계좌 해외선물 거래 개설
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
| Hyperliquid | `hyperliquid-python-sdk` (HIP-3 perp: trade.xyz) |
| 해외선물 (KIS) | REST + WebSocket API (Linux 네이티브) |
| 데이터 저장 | SQLite |
| 스케줄링 | asyncio |
| 모니터링 | Grafana + Prometheus |
| 알림 | Telegram Bot API |
| 환경 | EC2 Linux (Amazon Linux 2023) |

---

## 디렉토리 구조

**로컬 프로젝트 루트**: `/Users/myunggeunjung/rwa_arbitrage/`
**EC2 프로젝트 루트**: `~/rwa_arbitrage/`

> 참고: 상위에 중첩 폴더 없이 `rwa_arbitrage/` 가 git repo 루트이자 프로젝트 루트임.

```
rwa_arbitrage/   ← git repo root
├── CLAUDE.md                  # 이 파일
├── config/
│   ├── settings.yaml          # 전략 파라미터, 임계값
│   └── secrets.yaml           # API 키 (gitignore)
├── src/
│   ├── exchange/
│   │   ├── hyperliquid.py     # Hyperliquid API 래퍼 (trade.xyz perp)
│   │   ├── kis.py             # KIS 한국투자증권 REST/WebSocket (해외선물 실시간 호가)
│   │   └── kiwoom.py          # KiwoomMock (paper trading 주문 시뮬레이션용)
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
- Hyperliquid (trade.xyz): taker ~0.09bp (HIP-3)
- KIS 해외선물: MCL ~$2.50/계약, BZ ~$7.50/계약
- 펀딩레이트: 시간당 정산, 포지션 방향에 따라 수취 또는 지급

### 자본 효율성
- Hyperliquid: USDC 담보, 최대 10x 레버리지
- CME 선물: 증거금 제도 (CL ~$6,000/계약, BZ ~$5,500/계약)
- 양쪽 모두 증거금 확보 필요 → 자본 배분 전략 중요

---

## 마일스톤

| 단계 | 목표 | 상태 |
|------|------|------|
| M1 | ✅ Hyperliquid API 연동 + 시세 수집 | 완료 |
| M2 | ✅ KiwoomMock 기반 페이퍼 트레이딩 + 데이터 수집 | 완료 |
| M3 | ✅ 베이시스 분석 + 파라미터 튜닝 | 완료 |
| M4 | ✅ 계약 사이징 현실화 (MCL 100배럴, BZ 1000배럴) | 완료 |
| M5 | ✅ KIS API 연동 (실시간 MCL 호가, WebSocket) | 완료 |
| M6 | ✅ executable basis 검증 + 백테스트 그리드서치 | 완료 |
| M7 | 🔄 exit 전략 개선 (entry 기준 profit exit) | **현재 진행** |
| M8 | 페이퍼 트레이딩 수익성 검증 (2주+) | - |
| M9 | KIS 주문 API 연동 → 실거래 전환 (최소 규모) | - |
| M10 | 실거래 안정화 + 스케일업 | 지속 |

---

## 현재 이슈 및 방향 (2026-04-02)

### 핵심 문제: 즉시 청산으로 수수료 손실
- 49건 거래, 승률 16%, 총 PnL -$599.65
- 평균 보유 시간 6초 — mean reversion exit이 즉시 발동
- trading PnL은 +$65 (진입 방향은 올바름) 하지만 수수료 -$665가 이를 초과
- entry spread 23bp+ 인 거래에서만 수익 발생

### 다음 단계: M7 exit 전략 변경 — 스프레드 수렴 기반 청산
- **원리**: perp는 시장 원칙상 futures(인덱스) 가격으로 수렴해야 함. 수렴 안 해도 펀딩 받는 쪽이므로 보유 비용 없음.
- **기존**: mid basis가 mean±3bp에 도달하면 청산 → 수 초 만에 발동
- **변경**: perp 가격 ≈ futures 가격 (spread ≈ 0bp)일 때 청산
  - `convergence_target_bps: 3` → spread가 3bp 이하면 수렴 완료
  - `max_hold_hours: 48` → 수렴 기다리며 펀딩 수취
  - mean reversion exit, target_profit exit 제거
- **기대**: -25bp 진입 → 0bp 근처 수렴 청산 → 25bp 수익 → 수수료 13bp → 순수익 ~12bp

### 운영 환경
- EC2 (Amazon Linux): PM2로 `rwa-arb` 프로세스 관리
- 매일 UTC 00:00 (KST 09:00) PM2 cron restart → KIS 토큰 자동 갱신
- KIS CME/NYMEX 유료시세: 24시간 토큰 유효, 1일 1회 발급 원칙
- WTI(MCL) 전용 (Brent는 ICE 시세 미신청 + 계약 단위 비현실적)
