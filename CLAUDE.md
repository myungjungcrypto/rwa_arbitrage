# RWA Arbitrage Bot — Multi-Exchange Oil Basis & Spread Arbitrage

## 프로젝트 개요

원유(WTI) 페이퍼 트레이딩 봇. 5개 거래소를 허브-스포크 구조로 연결한 멀티 베뉴 차익거래.

- **Web2-Web3 (운영 중)**: trade.xyz(Hyperliquid) WTI perp ↔ KIS MCL 월물
- **Web3-Web3 (확장 진행 중)**: Hyperliquid 허브 ↔ Lighter / Binance / Bybit / OKX 페어

5개 거래소 모두 WTI perp 상장 확인 (2026-04-21 검증).
모든 페어가 HL을 leg_a 허브로 사용 → 단일 USDC 잔고로 전환 + leg_b 다양화.

### 거래소별 perp 사양 (확인 완료)

| 거래소 | 심볼 | Funding | Margin | 비고 |
|---|---|---|---|---|
| Hyperliquid (trade.xyz) | XYZ:CL | 1h | USDC | HIP-3 |
| Lighter | WTI | 1h | USDC | zkEVM L2 |
| Binance | CLUSDT | 4h | USDT | 100x, 2026-04-01 런칭 |
| Bybit | CLUSDT | 4h | USDT | 50x, 2026-03-27 런칭 |
| OKX | CL-USDT-SWAP | 4h | USDT | — |
| KIS (CME 월물) | MCL 근월물 | — | KRW 환산 | 100배럴/계약, dated futures |

### 핵심 메커니즘

- trade.xyz 퍼프는 CME 근월물 가격을 oracle 인덱스로 사용 (HIP-3)
- CME 근월물은 매월 5~10 영업일에 차월물로 가중 롤오버 (오라클 자동 전환)
- 베이시스(perp price - 상대 leg price) 확대 시 진입, 수렴 시 청산
- 양 leg 동시 발주 (`asyncio.gather`)로 한쪽만 체결 위험 최소화
- 펀딩 비대칭 위험 회피를 위해 **CME 휴장 시 전체 페어 OFF** (Strict gate)

---

## 아키텍처

현 상태: **레거시 product-keyed 경로 + 신규 pair-keyed 경로 병존**.
Phase C5(진행 중)에서 main.py가 pair-keyed로 switch하면 레거시 경로는 deprecated.

```
┌─────────────────┐     ┌──────────────────────────┐
│ Hyperliquid WS  │────▶│                          │
│ Lighter WS      │────▶│  ExchangeBase adapters   │
│ Binance WS      │────▶│  (Phase D-G에서 합류)    │
│ Bybit WS        │────▶│                          │
│ OKX WS          │────▶│         │                │
│ KIS WS (CME)    │────▶└─────────┼────────────────┘
└─────────────────┘               │
                                  ▼
                    ┌──────────────────────────────┐
                    │  DataCollector               │
                    │  - register_pair             │
                    │  - update_leg_quote          │
                    │  - on_pair_basis callback    │
                    └────────┬─────────────────────┘
                             │ (pair_id, basis_bps, leg_a, leg_b)
                             ▼
                    ┌──────────────────────────────┐
                    │  PaperTradingEngine          │
                    │  - process_pair_basis_update │
                    │  - SignalGenerator (per-pair)│
                    │  - exec_filter + min_abs gate│
                    │  - dispatch_pair_order       │
                    │    (per-exchange Semaphore)  │
                    └────────┬─────────────────────┘
                             │
                  ┌──────────┼──────────┐
                  ▼          ▼          ▼
              SQLite     Streamlit   PM2 logs
              (v3)       Dashboard
```

### 주요 design 결정

- **`ExchangeBase` Protocol**: 모든 거래소 어댑터 공통 인터페이스 (connect/subscribe_quotes/place_order/get_funding_info 등). 어댑터 추가 시 main loop 수정 불필요.
- **`ArbitragePair` first-class**: `(leg_a: ExchangeLeg, leg_b: ExchangeLeg, gate, params)` dataclass. AppConfig에서 자동 합성 또는 settings.yaml에서 명시.
- **Orderbook-mid 기반 basis 신호**: HL `mark_price`는 oracle 추적이라 자체 orderbook과 ~20bp 괴리 가능. 모든 신호 계산은 `(bid+ask)/2` 기반.
- **`min_abs_entry_bps` floor**: 통계 신호와 무관한 절대값 floor (현재 10bp). statistical band 통과해도 |exec_basis| < 10bp면 진입 차단.
- **Strict CME gate**: Web3-Web3 페어도 CME 휴장 시 OFF (월-목 1h break + 주말/휴일 모두). 펀딩 시스템 비대칭 위험 회피.
- **Per-exchange Semaphore(1)**: 동시 진입 충돌 방지. HL이 모든 페어의 leg_a라 단일 거래소 in-flight 1건 보장.
- **Additive DB 마이그레이션**: rename/drop 없이 컬럼만 추가. 자동 마이그레이션 + 1회 백업 (`*.pre-v2.bak`).

---

## 기술 스택

| 구분 | 기술 |
|---|---|
| 언어 | Python 3.11+ |
| Hyperliquid | `hyperliquid-python-sdk` (HIP-3 perp: trade.xyz) |
| KIS | REST + WebSocket API (Linux 네이티브) |
| 데이터 저장 | SQLite (schema v3) |
| 비동기 | asyncio |
| 대시보드 | Streamlit + pandas + Plotly |
| 알림 | Telegram Bot (예정) |
| 환경 | EC2 Linux (Amazon Linux 2023), PM2 |

---

## 디렉토리 구조

**EC2 프로젝트 루트**: `~/rwa_arbitrage/`

```
rwa_arbitrage/
├── CLAUDE.md                     # 이 파일 (live 진행 상황 추적)
├── config/
│   ├── settings.yaml             # 전략 파라미터, 페어 정의
│   └── secrets.yaml              # API 키 (gitignore)
├── src/
│   ├── exchange/
│   │   ├── base.py               # ExchangeBase protocol + Quote/OrderResult/Position dataclass
│   │   ├── registry.py           # ExchangeRegistry (factory)
│   │   ├── hyperliquid.py        # HL 클라이언트 + HyperliquidExchange adapter
│   │   ├── kis.py                # KIS 클라이언트 + KISExchange adapter
│   │   ├── kiwoom.py             # KiwoomMock (paper-only, KIS 주문 시뮬)
│   │   ├── binance.py            # (Phase E 예정)
│   │   ├── bybit.py              # (Phase F 예정)
│   │   ├── okx.py                # (Phase G 예정)
│   │   └── lighter.py            # (Phase D 예정)
│   ├── strategy/
│   │   ├── signals.py            # SignalGenerator (pair-keyed alias 포함)
│   │   ├── pair.py               # ArbitragePair / ExchangeLeg / PairGate / PairStrategyParams
│   │   ├── market_hours.py       # CME 장 시간 가드
│   │   ├── rollover.py           # 자동 월물 롤오버
│   │   ├── funding_monitor.py    # 거래소 펀딩 주기 런타임 검증
│   │   └── basis_arb.py
│   ├── data/
│   │   ├── collector.py          # 레거시 + pair-keyed API
│   │   └── storage.py            # SQLite (schema v3 자동 마이그레이션)
│   ├── paper/
│   │   └── engine.py             # 레거시 + pair-keyed entry/exit + state snapshot
│   ├── risk/
│   │   └── manager.py
│   └── utils/
│       ├── config.py             # AppConfig.get_pairs() synthesizer
│       └── logger.py
├── dashboard/
│   ├── app.py                    # Streamlit 엔트리
│   ├── queries.py                # 모든 SQL → pandas
│   ├── charts.py                 # Plotly figures
│   └── README.md                 # 설치/실행/SSH 터널 가이드
├── scripts/
│   ├── analyze_paper.py
│   ├── diagnose_drift.py
│   ├── close_zombies.py
│   ├── migrate_storage.py
│   └── run_backtest.py
├── tests/                        # pytest, 270+ tests
└── requirements.txt
```

---

## 마일스톤

| 단계 | 목표 | 상태 |
|---|---|---|
| M1 | Hyperliquid API 연동 + 시세 수집 | ✅ |
| M2 | KiwoomMock 기반 페이퍼 트레이딩 + 데이터 수집 | ✅ |
| M3 | 베이시스 분석 + 파라미터 튜닝 | ✅ |
| M4 | 계약 사이징 현실화 (MCL 100배럴) | ✅ |
| M5 | KIS API 연동 (실시간 MCL 호가, WebSocket) | ✅ |
| M6 | executable basis 검증 + 백테스트 그리드서치 | ✅ |
| M7 | exit 전략 개선 (스프레드 수렴 기반 청산) | ✅ |
| M8 | 자동 롤오버 + 좀비 포지션 정리 | ✅ |
| M9 | 페이퍼 트레이딩 수익성 재검증 (롤오버 픽스 후) | ✅ — 발견된 추가 버그 모두 수정 |
| **M10** | **멀티 거래소 통합 (Web2-Web3 + Web3-Web3)** | **진행 중 (Phase C5)** |
| M11 | KIS 주문 API 연동 → 실거래 전환 (최소 규모) | - |
| M12 | 실거래 안정화 + 스케일업 | - |

### M10 sub-phase 상세

| 단계 | 작업 | 상태 |
|---|---|---|
| **A** | `ExchangeBase` protocol + `ArbitragePair` scaffolding + adapter 래퍼 | ✅ |
| **A+** | `FundingIntervalMonitor` (거래소 펀딩 주기 런타임 검증) | ✅ |
| **B** | DB schema v2 (additive 마이그레이션, pair_id 컬럼 + leg_prices 테이블) | ✅ |
| **C1** | `AppConfig.get_pairs()` synthesizer | ✅ |
| **C2** | `DataCollector` pair-keyed API (`register_pair`, `update_leg_quote`) | ✅ |
| **C3** | `SignalGenerator` pair-keyed alias 메서드 | ✅ |
| **C4a** | `PaperTradingEngine` 인프라 (registry, dispatch helper, Semaphore) | ✅ |
| **C4b** | pair-keyed entry/exit flow (`process_pair_basis_update`) | ✅ |
| **C5** | `main.py` 배선 — pair-keyed 경로로 switch | ✅ |
| **D** | Lighter 어댑터 + `wti_hl_lighter` 페어 (shadow → live) | **현재 작업** |
| E | Binance 어댑터 + `wti_hl_binance` 페어 | - |
| F | Bybit 어댑터 + `wti_hl_bybit` 페어 | - |
| G | OKX 어댑터 + `wti_hl_okx` 페어 | - |
| H | 5개 페어 동시 paper + per-pair risk cap + 멀티 페어 리포트 | - |

### M (Monitoring) sub-phase

| 단계 | 작업 | 상태 |
|---|---|---|
| M1 | DB schema v3 (engine_state 스냅샷 테이블) + 봇 30s dump 루프 | ✅ |
| M2 | Streamlit 대시보드 (queries + charts + Streamlit UI) | ✅ |
| M3 | `requirements.txt` + EC2 PM2 등록 + SSH 터널 접속 | ✅ |

---

## 현재 진행 (2026-04-29)

**Phase D — Lighter 어댑터 + 첫 Web3-Web3 페어**

C5에서 main.py가 pair-keyed 경로로 전환 완료. 이제 Hyperliquid + KIS 외에
새 거래소를 어댑터로 합류시키며 멀티 페어로 확장 시작.

D 단계 작업:
- `src/exchange/lighter.py` 신규 — `LighterExchange(ExchangeBase)` 어댑터
  - REST + WS 클라이언트 (또는 lighter SDK 사용)
  - WTI 심볼 호가/체결 구독 → `Quote` 변환 → `update_leg_quote` push
  - `get_funding_info` 구현 (Lighter 1h funding 검증)
- `config/settings.yaml`에 `pairs:` 블록 신규: `wti_hl_lighter` (initially `enabled: false` shadow)
- main.py: registry에 `LighterExchange` 등록, lighter symbol 구독 시작
- shadow 24h → 분포 분석 → `enabled: true` flip → paper 진입 시작

**다음 단계 (Phase E)**: Binance 어댑터 + `wti_hl_binance` 페어.

---

## 최근 변경사항

(역순; 자세한 commit 메시지는 `git log` 참조)

### 2026-04-29
- `6f9abdb` — **M10 Phase C5**: `main.py` 배선 완료. legacy `on_basis` 콜백이 Quote 빌더 bridge로 변환, `engine.process_pair_basis_update`(async) 가 pair callback으로 등록. ExchangeRegistry 구성 + `HyperliquidExchange` / `KISExchange` 어댑터 등록. 동시 진입/청산 race 방어 (atomic check-after-gather, atomic pop). 280 tests pass.
- `894ff25` — **docs**: CLAUDE.md를 GitHub 진행 추적용 living document로 재작성.
- `f178fc5` — **M10 Phase C4b**: pair-keyed `_handle_pair_entry` / `_handle_pair_exit` + `process_pair_basis_update` 오케스트레이터. KIS는 KiwoomMock으로 paper, perp는 quote bid/ask 시뮬. 274 tests pass.
- `0c7ae9e` — **M10 Phase C4a**: Engine pair-keyed 인프라. `register_pair` / `set_exchange_registry` / `dispatch_pair_order` (per-exchange Semaphore(1)). 265 tests pass.
- `5d6388f` — Dashboard since_date cutoff (default 2026-04-21). zombie cleanup -$20K + 그 이전 데이터 제외. 사이드바 datepicker로 override 가능.
- `00345ba` — **M10 Phase C3**: `SignalGenerator` pair-keyed alias 메서드 (`update_basis_for_pair` 등). dict 키만 product↔pair_id 호환.
- `c6e3d95` — **M10 Phase C2**: `DataCollector` pair-keyed API. `register_pair` + `update_leg_quote(pair_id, leg, Quote)` + `on_pair_basis` 콜백. leg_prices 자동 저장.

### 2026-04-28
- `dc0f482` — Dashboard: jinja2 의존 제거 (Streamlit `column_config` 전환) + 헤더 카드 카운터를 DB 기반으로 (봇 재시작 영향 없음).
- `17fbd9b` — `requirements.txt`: jinja2 추가 (이후 `dc0f482`에서 의존 자체 제거).
- `6f4c049` — **M10 Phase M2-M3**: Streamlit 대시보드 (`dashboard/`). queries + charts + app + README. SSH 터널 접속 (127.0.0.1:8501). 230+ tests.
- `34d56e0` — **M10 Phase M1**: schema v3 + `engine_state` 스냅샷 테이블. 봇이 30s마다 EngineState dump. 대시보드 live counter 백엔드.
- `f40972b` — **M10 Phase C1**: `AppConfig.get_pairs()` synthesizer. legacy products → ArbitragePair 자동 합성.

### 2026-04-27
- `642699e` — **fix**: 신호 basis가 HL `mark_price` 대신 orderbook mid 사용. mark이 oracle 추적이라 자체 orderbook과 ~20bp 괴리하여 phantom 신호 다수 발생.

### 2026-04-26
- `3c579fe` — **M10 Phase B**: DB schema v2 (additive). `pair_id` 컬럼 + `leg_prices` 테이블 + `pairs` dimension. 자동 마이그레이션 + `*.pre-v2.bak` 1회 백업.

### 2026-04-25
- `3bae3fe` — **fix**: collector + engine에서 bid/ask 미수신 시 mid_price fallback 제거. 14건의 sub-10bp 진입 root cause.
- `381506c` — **hotfix**: `min_abs_entry_bps: 10` floor 추가. 통계 신호 통과해도 |exec| < 10bp면 진입 차단.

### 2026-04-22
- `ba811b8` — **M10 Phase A+**: `FundingIntervalMonitor` — 거래소 펀딩 주기 런타임 검증 (Binance 1h→8h 같은 정책 변경 자동 감지).

### 2026-04-21
- `c669571` — **M10 Phase A**: `ExchangeBase` protocol + `ArbitragePair` scaffolding. 기존 클라이언트 무수정, adapter 래퍼만 추가. 회귀 0.

### 2026-04-21 (이전)
- `8002c02` — entry_threshold 25→20bp 인하 + entry near-miss 카운터.
- `0898f23` — M9 prep: CME market-hours gate.

### 2026-04-17 이전
- M8 자동 롤오버 (active contract 매시간 재계산, KIS resubscribe).
- close_zombies.py로 8건 좀비 포지션 청산 (-$20,459, 1회성).
- 그 이전 M1-M7 마일스톤 완료.

---

## 운영 가이드

### EC2 배포 (모든 commit 후)

```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<EC2_IP>
cd ~/rwa_arbitrage
git pull origin claude/check-trading-results-AVTDv
pip install -r requirements.txt --user      # 새 의존성 있을 시
pm2 restart rwa-arb
pm2 restart rwa-arb-dashboard               # 대시보드도 함께
pm2 logs rwa-arb --lines 50 --nostream
```

### 대시보드 SSH 터널 접속

```bash
# ~/.ssh/config 등록 (1회)
Host rwa-ec2
  HostName <EC2_IP>
  User ec2-user
  IdentityFile ~/.ssh/your-key.pem
  LocalForward 8501 localhost:8501

# 접속
ssh rwa-ec2
# 브라우저: http://localhost:8501
```

### DB 백업/마이그레이션

```bash
# 자동 마이그레이션은 Storage.connect() 시 작동. 명시적 검증:
python3 scripts/migrate_storage.py --report

# 자동 백업 위치
ls -la data/arbitrage.db*
# data/arbitrage.db
# data/arbitrage.db.pre-v2.bak    (v1→v2 마이그레이션 직전)
```

### 운영 체크 한 줄 명령

```bash
# 봇 상태
pm2 logs rwa-arb --lines 30 --nostream | tail -30

# DB 카운트
python3 -c "
import sqlite3
con = sqlite3.connect('data/arbitrage.db'); con.row_factory = sqlite3.Row
print('schema:', con.execute(\"SELECT value FROM schema_meta WHERE key='version'\").fetchone()[0])
print('positions:', con.execute('SELECT COUNT(*) FROM positions').fetchone()[0])
print('basis_spread:', con.execute('SELECT COUNT(*) FROM basis_spread').fetchone()[0])
print('engine_state:', con.execute('SELECT COUNT(*) FROM engine_state').fetchone()[0])
"

# 페이퍼 결과 분석
python3 scripts/analyze_paper.py --db data/arbitrage.db
```

### 환경 일정

- 매일 UTC 00:00 (KST 09:00) PM2 cron restart → KIS 토큰 자동 갱신
- KIS CME/NYMEX 유료시세: 24시간 토큰 유효, 1일 1회 발급 원칙
- 다음 CME 롤 window: 2026-05-07 ~ 2026-05-14 (MCLM26 → MCLN26)

---

## 위험 관리

- **HL 단일 leg 집중**: 5개 페어 모두 leg_a가 HL → outage 시 동시 영향. aggregate exposure cap + WS 60s 끊김 시 emergency close.
- **펀딩 비대칭**: 1h(HL/Lighter) vs 4h(Binance/Bybit/OKX). 단기 hold(`max_hold_hours <= 12`) + 주말 OFF로 완화.
- **USDC vs USDT peg**: HL/Lighter는 USDC, 나머지는 USDT. de-peg 이벤트 모니터링 후 50bp 초과 시 Web3-Web3 페어 flatten 옵션.
- **신규 거래소 listing 안정성**: Binance/Bybit CLUSDT는 런칭 1개월 미만. 진입 전 orderbook depth 체크 (`bid_qty + ask_qty >= 5×position_size`).
- **인증 방식 4종 신규**: Binance HMAC, Bybit HMAC v5, OKX 3-header, Lighter signer. 각 어댑터 자체 처리 + Phase D-G 페이퍼는 read-only 키만.
- **DB write 부하**: 5 페어 × 1Hz = 기존 3-5배. WAL 모드 + 1초 배치 INSERT.
- **CME 일일 1h 휴장 (월-목 16:00-17:00 CT)**: Strict gate 사용 → 이 시간 Web3-Web3 페어도 OFF. 데드 시간 비율 확인 후 필요 시 `CME_LONG_CLOSURE_ONLY` 게이트로 전환.

---

## 주요 발견 & 의사결정 기록

### 2026-04-21~04-27 페이퍼 분석 결과
30 trade, 7일 운영. 핵심 발견:
- **mid_basis vs exec_basis gap**: HL `mark_price`는 oracle 추적이라 HL 자체 orderbook과 ~20bp 괴리 가능 → "phantom 신호" 다수 발생. Fix: 신호 계산을 orderbook mid로 전환.
- **bid/ask mid-fallback bug**: collector + engine이 호가 미수신 시 mid_price로 fallback → exec_filter 우회. Fix: 0 전달 + mid fallback 제거.
- **Entry spread bucket 분포**:
  - <10bp: 14건, 14% WR, 손실
  - 10-20bp: 10건, 90% WR, 소익
  - 20-50bp: 6건, 100% WR, 명확한 익
  → `min_abs_entry_bps: 10` floor 추가로 sub-edge 진입 차단.

### 거래소 perp 가용성 (2026-04-21 검증)
사용자 확인 (당초 OKX/lighter는 미상장으로 추정했으나 사용자 정정으로 5개 모두 상장 확인):
- Lighter `app.lighter.xyz/trade/WTI`
- OKX `CL-USDT-SWAP` (funding 4h, 8h 아님 — 사용자 정정)

### Phase 진입 순서 (funding asymmetry 최소화)
1. Lighter (1h vs 1h, USDC-USDC) — 가장 안전
2. Binance (4h vs 1h, USDT-USDC) — 유동성 최대
3. Bybit (4h vs 1h, USDT-USDC)
4. OKX (4h vs 1h, USDT-USDC)
