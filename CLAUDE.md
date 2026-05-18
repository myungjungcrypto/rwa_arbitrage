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
| M10 | 멀티 거래소 통합 (Web2-Web3 + Web3-Web3) | ✅ — 5/5 adapter scaffolding 완료 (Phase A-G) |
| **M11** | **KIS 주문 API 연동 → 실거래 전환 (최소 규모)** | **✅ — LIVE 가동 중 (5/18 첫 거래 발생, latency fix 적용)** |
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
| D | Lighter 어댑터 + `wti_hl_lighter` 페어 | ✅ scaffolding (shadow) |
| E | Binance 어댑터 + `wti_hl_binance` 페어 | ✅ scaffolding (shadow) |
| F | Bybit 어댑터 + `wti_hl_bybit` 페어 | ✅ scaffolding (shadow) |
| G | OKX 어댑터 + `wti_hl_okx` 페어 | ✅ scaffolding (shadow) |
| **H** | 5개 페어 동시 paper + per-pair risk cap + 멀티 페어 리포트 | **다음** |

### M (Monitoring) sub-phase

| 단계 | 작업 | 상태 |
|---|---|---|
| M1 | DB schema v3 (engine_state 스냅샷 테이블) + 봇 30s dump 루프 | ✅ |
| M2 | Streamlit 대시보드 (queries + charts + Streamlit UI) | ✅ |
| M3 | `requirements.txt` + EC2 PM2 등록 + SSH 터널 접속 | ✅ |
| M4 | LIVE 가시성 (mode badge, leg freshness, balance card) | ✅ |

### M11 sub-phase (LIVE 트랙) 상세

| 단계 | 작업 | 상태 |
|---|---|---|
| 11a | KIS REST 실 주문 (OTFM3001U/OTFM1412R, CANO+ACNT_PRDT_CD split) | ✅ |
| 11b | HL eth_account signer + SDK v0.23 호환 (`name=` + `perp_dexs=`) | ✅ |
| 11c | LIVE-mode 분기 + emergency partial-entry unwind | ✅ |
| 11d | Telegram + per-pair cap + WS watchdog + license auto-detect | ✅ |
| 11e | LIVE 가동 + 첫 거래 발생 + spike latency fix | ✅ (5/18) |
| 11f | scale-up (1 MCL → N MCL) + 다거래 PnL 검증 | 진행 예정 |

### LIVE 안전장치 (전체) 상세

| 가드 | 트리거 | 동작 |
|---|---|---|
| Per-pair contract cap | `live_max_contracts_per_pair` | 1 MCL hard cap |
| WS quote freshness watchdog | leg quote 60s+ stale | 알림 + auto_flatten |
| KIS license auto-detect | CME open + futures 5min stale | 라이센스 결제 안내 알림 |
| Rollover blackout | BD 5 시작 1일 전 | 신규 진입 차단 + 보유 flatten |
| Contract alignment monitor | HL index ↔ KIS mid 50bp+ 차이 | 알림 (옵션: auto_flatten) |
| Emergency partial-entry unwind | 한 leg fill + 한 leg fail | 반대 reduce_only 청산 |
| Mock fallback 차단 (LIVE) | KIS connect fail | RuntimeError raise (crash loud) |
| Entry lock per pair | 동일 페어 in-flight | asyncio.Lock (KIS 초당 한도 회피) |
| **EXIT lock per pair + 5s cooldown** | EXIT 동시 호출 / retry | asyncio.Lock + `_last_exit_attempt_ts` (5/18 incident 후 추가) |
| **EXIT reduce_only=True 강제** | EXIT 모든 발사 | HL SDK 자체 enforce + KIS adapter settlement-aware pre-check |
| Daily loss cap | 누적 일일 손실 한도 | 진입 차단 |

---

## 현재 진행 (2026-05-18 09:22 KST)

**M11 LIVE 트랙 — EXIT critical fix 완료 + 재가동**

### LIVE 상태
- `mode: LIVE` 09:22 재기동 (5/18 07:58 incident → 1시간 정지 → fix → 재가동)
- 양 leg 실시간 데이터 + 잔고 표시 + **5/18 critical fix 4종 적용 + round-trip 재검증 통과**
- 다음 rollover blackout: BD 5 (~2026-06-05) 자동 차단

### 5/18 발생 사건 + Fix (긴 하루)
1. **08:00경 첫 LIVE entry** (id=3260): spike latency 14bp loss
   → `404d7cc` HL Exchange cache + ref_price hint (latency 1500ms→400ms)
2. **08:00경 KIS WS silent-stale**: server-close 후 push 미시작
   → `d2bda96` KIS WS reconnect verify + retry
3. **07:58 EXIT retry storm** (id=3261, critical): HL 978배럴 + KIS 2계약 reverse
   → `041c3ba` EXIT lock + 5s cooldown
   → `42fb006` `_fill_pair_leg(reduce_only)` 인자 추가, EXIT path True 명시
   → `157d9f5` → `1e17fbb` KIS adapter reduce_only graceful pre-check
4. **알림 noise 개선**: `cc1a97a` WS STALE 메시지에 거래소/심볼/시각/원인 명시
5. **dashboard 진단**: `aab6d82` schema v5 + Entry diagnostics 섹션
   (signal_bp / exec_bp / slip_bp / latency_ms 한눈에)

### M10 (Multi-exchange paper shadow) — 5/5 어댑터 완료
- Phase D (Lighter), E (Binance), F (Bybit), G (OKX) 모두 scaffolding ready
- 각 페어 `enabled: false` shadow 상태

### 다음 (현재 진행 중)
1. **다음 LIVE entry 모니터 — latency 측정 + spike-chasing 가드 효과 검증**
   - KIS persistent session (475c041) 효과 — KIS leg 200-500ms target
   - max_entry_slip_bps 10 가드 — 5/18 5건 같은 손실 패턴 차단 확인
   - 결과로 HL signing offload 작업 여부 결정
2. **레이턴시 추가 fix** (앞으로 모든 latency 작업은 CLAUDE.md 'Latency 작업
   추적' 표에 기록 — '주요 발견 & 의사결정 기록' 섹션):
   - HL eth_account signing offload (ProcessPoolExecutor) — 100-200ms 추정
   - HL Info object cache (universe lookup) — 측정 후 결정
   - 목표 양 leg 500ms 이내
3. 5/18 incident 손실 정리 (추정 $100-300, HL/KIS 직접 확인)
4. fault injection 회귀 테스트 (EXIT fail retry storm 시뮬)
5. Phase H — 5-pair concurrent + per-pair risk caps + analyze_paper 멀티 페어

---

## 최근 변경사항

(역순; 자세한 commit 메시지는 `git log` 참조)

### 2026-05-18
- `475c041` — **perf: KIS persistent aiohttp session (latency 1500ms→300ms 목표)**.
  5/18 LIVE 5건 거래 분석에서 KIS latency 218ms ~ 3253ms variance 극심
  (avg ~1500ms). 218ms 케이스 = OS가 우연히 TCP connection 재사용한 경우,
  3253ms = 매번 새 ClientSession 만들어서 TCP+TLS+DNS handshake 매번 발생.
  KISAuth에 persistent `aiohttp.ClientSession` (TTL DNS cache 5분 +
  keepalive 60s) 추가, 모든 KIS REST 호출 (token/approval/order/cancel/
  positions/balance/orderbook) 공유. 기대: 안정적 200-500ms.
- `e2dba04` — **fix: spike-chasing 차단 (slip cap + min_hold_seconds)**.
  5/18 LIVE 5건 연속 손실 (-$95 누적) 분석: 모두 signal mid ±23bp인데
  latency 1-7초 동안 spike 사라져 exec basis 거의 0으로 들어감. 3261/3263은
  부호까지 반전 (signal +25 exec -30). 방어적 가드 2종:
  ① `max_entry_slip_bps: 10` — signal-exec gap 10bp 초과 시 entry skip
     (부호 반대 자동 차단). 5/18 5건 모두 slip ≥15bp → 전건 차단 가능.
  ② `min_hold_seconds: 60` — 진입 후 60초 hold 강제 (hold 0초 즉시 청산
     손실 패턴 방지). emergency unwind 등 안전청산은 별도 path 통과.
- `1e17fbb` — **KIS reduce_only pre-check 완화 (settlement delay 인식)**.
  157d9f5의 strict pre-check (positions empty → reject)가 정상 EXIT까지
  reject. KIS `inquire-unpd` (positions) endpoint는 일일 정산 단위라 당일
  신규 거래 즉시 반영 X. 3-tier 변경:
  ① positions row 있고 symbol 매칭 → strict 검증 (size/side/cap).
  ② positions row 있는데 symbol 없음 → reject (다른 symbol만 보유).
  ③ positions 빈 응답 → settlement delay 추정 → WARNING + dispatch 진행.
  5/18 incident 재발 방지는 engine 측 lock + cooldown으로 충분 (KIS pre-check
  은 보조). round-trip 재검증 통과.
- `157d9f5` — **KIS adapter reduce_only pre-check** (1차, 너무 strict).
  KIS REST가 직접 reduce_only flag 없어서 adapter에서 사전 positions 조회 +
  보유 검증. round-trip에서 빈 응답에 모두 reject → 1e17fbb로 완화.
- `42fb006` — **CRITICAL fix: EXIT path reduce_only=True 명시**.
  5/18 07:58 incident root cause. `_fill_pair_leg`가 reduce_only 인자 자체
  없었음 → EXIT/unwind retry 시 reverse position 누적. HL은 SDK 레벨에서
  reduce_only=True면 거래소가 자체 enforce (보유 0 시 reject). engine
  `_do_pair_exit`이 양 leg fill 호출 시 reduce_only=True 명시 전달.
- `041c3ba` — **CRITICAL fix: EXIT lock per pair + 5s cooldown**.
  5/18 07:58 incident: 정상 EXIT 청산 후 1분 21초 동안 같은 EXIT가 1초당
  20+회 무한 retry. ENTRY는 `_pair_entry_locks`로 보호되지만 EXIT은 보호
  안 됨. 추가: `_pair_exit_locks` + `_last_exit_attempt_ts` 5초 cooldown.
  ⚠️ EXIT FAIL Telegram 알림 (KIS HTS 확인 안내).
- `d2bda96` — **KIS WS reconnect verify + 자동 재시도**. 5/18 07:02 silent-stale
  사건 fix. KIS server-close 후 우리 봇이 reconnect + subscribe restore까지는
  성공했지만 KIS 서버가 새 connection에 데이터 push를 시작 안 함 (KIS의
  known quirk — subscribe ack는 주지만 server-side state stale).
  `_reconnect()` 후 15s wait + `_last_msg_ts` 검증, push 안 오면 token + key
  cache clear + 새로 발급 + 재구독 (최대 3회 시도). 12분 silent → 최대 45초
  자동 복구.
- `cc1a97a` — **WS STALE 알림 메시지 enrichment**. 기존
  `🚨 WS STALE wti_cme_hl leg_a age=71s` → 어느 거래소/심볼인지 모름.
  새 형식:
  ```
  🚨 QUOTE STALE [wti_cme_hl]
  leg_a: hyperliquid/xyz:CL (perp) (HL 시세 WS wss://...)
  age=71s (threshold 60s)
  last quote: 14:08:35 KST
  ```
  + KIS_LICENSE 알림에 의심 원인 4가지 (라이센스 / 다른봇 충돌 / KIS
  maintenance / reconnect fail) + 검증 명령 동봉.
- `aab6d82` — **dashboard 진단 섹션 (testbed용)**. schema v5 additive —
  positions에 `signal_basis_bps`, `exec_basis_bps`, `entry_latency_ms_a/b`,
  `signal_ts` 5컬럼 추가. engine `_do_pair_entry`가 entry 시 측정 + 저장.
  대시보드 "🧪 Entry diagnostics" 표에 signal/exec/slip/latency 한눈에.
  LIVE 안정화 후 제거 가능.
- `404d7cc` — **perf: HL entry latency 축소 (Exchange cache + ref_price hint + ms log)**.
  5/18 첫 LIVE 거래에서 signal mid -21.35bp → 실 fill -6.79bp (slippage 14bp,
  spike가 1.4s만 지속한 동안 latency 2.4s로 못 잡음). 3 layer fix:
  ① SDK Exchange 객체 캐싱 (`_build_exchange` per-order → startup 1회).
  ② `ref_price` 인자 — adapter가 in-memory `_latest_meta.mark_price` 전달 →
  `get_market_data()` REST 호출 생략.
  ③ `latency=XXXms` 텔레메트리 로그. 기대: 600-1500ms → 150-400ms.
- `d7369fd` — **asyncio.Lock per pair + unwind Telegram 알림**. 5/15 incident에서
  같은 signal로 1초에 수십 번 entry retry → KIS 초당 거래 한도 초과.
  Lock으로 직렬화 + emergency unwind 결과 (🛟 OK / 🚨 FAILED) Telegram.
- `41461f2` — **HL 5 sig figs round**. `mark * 1.05 = 104.034` (6 sig) → SDK reject
  `Order has invalid price`. round 2 → 104.03 적용.
- `1f1e21c` — **KIS contract auto-advance**. `pair.leg_b.symbol`이 매월 stale 됨
  (settings.yaml에 MCLM26 박혀있는데 봇은 MCLN26 시세 수신). 부팅 시 + 매시간
  rollover_watch_loop에서 `get_active_contract()` 결과로 pair object의 symbol을
  hot-mutate. 매월 수동 sed 불필요.
- `e721604`, `34bda4c`, `9dadcd7` — **scripts/test_live_round_trip.py**.
  수동 1 MCL 양 leg market entry + 5초 후 reduce_only 청산 검증 스크립트.
  KIS rate-limit-friendly (confirm 후 auth) + case-insensitive yes/YES.

### 2026-05-16
- `364f935` — **KIS 시세 라이센스 만료 자가 감지**. 2 layer: ① WS_WATCHDOG STALE
  알림에 KIS leg + dated_futures이면 "라이센스 만료 의심" hint + apiportal URL
  자동 첨부. ② 30분 간격 `kis_license_health_loop` — CME open인데 futures_prices
  5분+ stale이면 한 번 `🚨 KIS LICENSE SUSPECT` 알림 (recovery 시 `✅ RECOVERED`).
  수동 calendar 관리 불필요.
- `829ef66` — **LIVE 모드 mock fallback 차단**. EC2 KIS 토큰 rate limit (EGW00133,
  1분당 1회) 위반 시 봇이 silent하게 KiwoomMock으로 fallback → LIVE 모드인데
  가짜 시세로 HL에만 실 주문 발사 위험. LIVE에서 KIS connect 실패 시 즉시
  RuntimeError raise (crash loud, no single-leg exposure).
- `600b765` — KIS `hts_id` 헤더 wire (`rt_cd=7 계좌에 등록된 HTS ID와 일치하지
  않습니다` 대응). secrets.yaml `kis.hts_id` 필드 추가. 새 KIS app_key 발급 후
  잔고/주문 endpoint 일부에서 검증.

### 2026-05-15
- `1cb11c0` — **rollover blackout 알림 cooldown**. BD 5~10 동안 매 신호마다
  ⛔ ENTRY BLOCKED 알림이 6일간 spam. 상태 전이 시만 1회 알림 (📅 ENTERED /
  ✅ CLEARED), 매 entry attempt에서는 logger.warning만.
- `ecee4f1` — **HL invalid price fix (1st)**. 시장가 IOC에 `limit_px=0` 보내면
  SDK reject. mark price ±5% slippage buffer 적용. (이후 41461f2의 sig fig
  round와 함께 LIVE entry path 안정화.)
- `d7d2d84` — **rollover blackout off-by-one fix**. `get_roll_weights` 공식상
  `(bd - start) / span`이 BD start_day=5에서 weight 0 → 실제 divergence
  시작일은 BD 6. blackout_start를 `(rollover_start_day + 1) - block_days`로
  보정. block_days=1 → BD 5부터 차단.
- `71c6df7` — **rollover blackout (BD-1) + contract alignment monitor**. KIS-leg
  pair 한정. ① 롤 window 시작 N영업일 전부터 진입 차단 + 보유 포지션 자동
  flatten. ② HL `index_price` ↔ KIS `mid_price` |diff_bps|가 임계 초과 시
  `⚠️ CONTRACT ALIGNMENT` 알림. 양 leg가 다른 contract 추종하는 경우
  (예: HL이 롤됐는데 KIS 안 됨) 즉시 감지.

### 2026-05-12 ~ 14
- `1446af4` — **M10 Phase G**: OKX v5 SWAP 어댑터 (paper shadow). 5/5 거래소
  ExchangeBase adapter 완료 (HL/KIS/Lighter/Binance/Bybit/OKX).
  `wti_hl_okx` (CL-USDT-SWAP, 4h funding) shadow pair settings.yaml 추가.
- `53018d1` — **M10 Phase F**: Bybit v5 linear adapter + `wti_hl_bybit` shadow.
- `d12c2c3` — **M10 Phase E**: Binance USDⓈ-M Futures adapter + `wti_hl_binance` shadow.
- `50467de`, `e35b83e`, `44bf330`, `bb99a79` — **KIS 잔고 endpoint 보정 시리즈**.
  최종: tr_id=OTFM1411R + response key=`output` (singular) + `fm_tot_asst_evlu_amt`
  파싱. HL 잔고는 native perp + xyz dex + spot 3 영역 합산.
- `7fc02f6` — **dashboard 잔고 polling 추가**. 봇이 2분마다 각 adapter
  `get_account_value()` → `account_balance` 테이블 (schema v4) → dashboard
  카드 표시. paper-only adapter (lighter/binance/bybit/okx)는 NotImplementedError
  로 raise해서 polling skip.
- `db03099` — **dashboard LIVE 가시성**. 타이틀 mode badge (🔴 LIVE / 📊 PAPER) +
  leg_a/leg_b quote freshness 카드 (🟢 <5s / 🟡 <60s / 🔴 >60s).

### 2026-05-03 ~ 11 (M11 Phase 11 — LIVE 인프라)
- `1a83aa6` — **Phase 11d: LIVE safety**. TelegramConfig wire +
  `live_max_contracts_per_pair` (1 MCL hard cap) + WS quote freshness 워치독
  (60s threshold, auto_flatten on stale).
- `8661d71` — **Phase 11c: LIVE-mode 분기 + emergency unwind**. `_fill_pair_leg`
  가 `config.mode == "LIVE"`면 ExchangeRegistry 통해 실 어댑터 dispatch.
  한 leg만 fill되고 다른 leg fail 시 `_emergency_unwind_partial_entry`로
  체결된 leg를 reduce_only 시장가 청산.
- `77aa690` — **HL SDK v0.23 호환**. SDK가 `coin=` → `name=` 인자 rename +
  `perp_dexs=["xyz"]` 추가 요구. HIP-3 universe 로드 위해.
- `c82295b` — **Phase 11b: HL eth_account signer**. agent wallet PK로 SDK
  Exchange object build + 실 주문/취소 path.
- `3b96d1d` — **Phase 11a: KIS REST 실 주문**. OTFM3001U/OTFM3003U/OTFM1412R
  (실전) + VTFM* (모의). CANO+ACNT_PRDT_CD split.
- **M10 Phase D scaffolding**: Lighter 어댑터 + shadow 페어 인프라.
  - `514b85e` — `LighterExchange(ExchangeBase)` adapter (lighter-sdk optional, factory injection for tests). 18 tests.
  - 후속 commit — `LighterConfig` + `pairs:` YAML 블록 + `pair.enabled` shadow gate + main.py wiring (HL Quote fan-out to all HL-hub pairs). 311 tests pass.

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

### 2026-05-18 EXIT retry storm → HL 978배럴 + KIS 2계약 reverse position (critical incident)

**Timeline (trade id=3261)**:
- 07:56:39 ENTRY long_basis: HL sell 100배럴 (short 100), KIS buy 1 (long 1)
- 07:56:55 EXIT 정상 청산: HL buy 100 → 0, KIS sell 1 → 0
- 07:58:16 ~ 07:58:18 **EXIT가 1초당 20+회 무한 retry** (1분 21초간)
- 결과: HL **long 978배럴** (의도하지 않은 reverse), KIS **short 2계약** (reverse)
- 사용자 수동 청산으로 정리. 추정 손실 $100-$300.

**Root cause 3종 결합**:
1. **EXIT path에 asyncio.Lock 없음** — ENTRY는 `_pair_entry_locks` 보호되지만
   EXIT은 보호 안 됨. process_pair_basis_update가 매 quote tick마다 EXIT
   signal 재생성 → `_handle_pair_exit` 동시 호출.
2. **`reduce_only=False` 누락** — `_fill_pair_leg`가 reduce_only 인자 자체
   없었음. EXIT/unwind 모두 `reduce_only=False`로 dispatch:
   - HL: SDK가 reduce_only=False로 처리 → reverse 누적 가능 → 증거금 한계
     978배럴에서 멈춤
   - KIS: reduce_only flag 자체 없는 REST → 보유 무관 sell 처리 → reverse
     누적 → 증거금 한계 2계약에서 reject 시작
3. **EXIT fail retry 로직** — "will retry" 메시지 후 그냥 return → 다음
   quote tick에 또 호출 → 1초당 20+회 발사.

**Fix (5/18, 4개 commit)**:
- `041c3ba` — `_pair_exit_locks` per pair + 5초 cooldown (`_last_exit_attempt_ts`)
- `42fb006` — `_fill_pair_leg(..., reduce_only)` 인자 추가, EXIT path에서 True 전달
- `157d9f5` — KIS adapter reduce_only pre-check (positions 조회 + 검증) — strict
- `1e17fbb` — KIS pre-check 완화 (settlement delay 인식, dispatch 허용)

**왜 PAPER에서 안 잡혔나** (1달 무탈했던 이유):
1. PAPER `_fill_pair_leg`는 양 leg 100% success 시뮬 (KiwoomMock + quote bid/ask)
   → EXIT fail 케이스 절대 발생 안 함 → retry 루프 trigger 0건.
2. PAPER fill latency 0ms → 양 leg gather가 ms 안에 끝남 → race condition 0.
3. KiwoomMock도 `reduce_only` 무시 (보유 무관 reverse 가능)했지만 EXIT retry
   가 절대 안 일어나서 dormant.
4. 결국 reduce_only=False는 fail-only-path 버그라 PAPER에서 영원히 dormant.
   LIVE 첫 fail (KIS REST 7초 hang)에서 즉시 explosion.

**교훈**: PAPER backtest ≠ LIVE failure mode 검증. fault injection 시뮬 필요.
회귀 테스트도 happy path만 검증 → fail-path explicit 테스트 추가 권장.

**검증**: round-trip 2회 재실행 후 정상 동작 확인 (08:49:38). EXIT lock +
cooldown + reduce_only + KIS settlement-aware pre-check 모두 작동.

### 2026-05-18 KIS WS server-close silent-stale 사건 → 자동 verify+retry
하루 동안 KIS WS가 4번 끊김 (02:04, 03:13, 06:09, 07:02). 마지막 07:02
케이스에서:
1. KIS server가 connection을 unilateral close (`KIS WS closed by server`)
2. 봇이 자동 reconnect 성공 (token+approval_key refresh, subscribe restore)
3. **하지만 KIS server가 새 connection에 push 시작 안 함** — 12분 silent stale
4. 60s 후 WS_WATCHDOG STALE → KIS_LICENSE SUSPECT 알림 → 사용자 수동 pm2 restart로 복구

**Root cause — KIS WS server-side state stale (알려진 quirk)**:
- KIS는 client가 PINGPONG echo-back으로 keep-alive 잘 보내도 일정 시간/조건 후
  unilateral close (load balancer rotation, 또는 server internal policy)
- reconnect 시 subscribe ack까지는 정상 응답하지만 일부 케이스에서 실제 push
  pipeline 활성화 실패
- 알려진 우회: 완전 재초기화 (token + approval_key + WS 처음부터) — 그래야 KIS
  server-side state도 새로 잡힘

**Fix (d2bda96)**:
- `_reconnect()` 후 15s wait + `_last_msg_ts` 비교
- push 없으면 → token/approval_key cache clear → 새로 발급 → 재구독 (최대 3회)
- 3회 실패 시 WS_WATCHDOG (60s)가 잡아서 알림 + 안전장치 발동

**알림 메시지 enrichment (cc1a97a)** — 끊김 발생 시 메시지만 봐도 어느 거래소/
심볼/마지막 시각/채널/원인이 즉시 보이게. `🚨 WS STALE leg_a` → `🚨 QUOTE STALE
[pair] leg_a: hyperliquid/xyz:CL (HL 시세 WS wss://...)`.

**왜 KIS는 자주 끊나** (한국투자증권 WS 운영 특성):
- KIS는 한국 증권시장 (09:00-15:30 KST) 외 시간엔 reliability 우선순위 낮음
- WS 서버는 load balancer 뒤에 있고 정기적 rotation 발생 (시간당 1-수회)
- 일부 maintenance window는 공지 없이 진행
- 잦은 reconnect는 KIS rate limit 트리거 가능 (분당 1회 token 발급 제한)
→ 봇이 자동 verify + retry로 자체 회복하는 게 표준 대응

### 2026-05-18 5건 spike-chasing 손실 + latency 진단 → KIS persistent session

5/18 LIVE 5건 연속 손실 (-$94.61 누적). entry diagnostics 분석:

| Trade | signal | exec | slip | latency_a (HL) | latency_b (KIS) | PnL    |
|-------|--------|------|------|----------------|------------------|--------|
| 3261  | +22.1  | -10.8 | +33  | 7491ms (cold)  | 2633ms          | -15.83 |
| 3262  | -23.3  | -1.0  | -22  | 2235ms         | 1992ms          | -12.85 |
| 3263  | +25.8  | -30.7 | +56  | 3254ms         | 1927ms          | -43.71 |
| 3264  | -23.4  | -8.0  | -15  | 1078ms         | 950ms           | -6.81  |
| 3265  | -23.9  | -4.4  | -19  | 1157ms         | 637ms           | -15.41 |

**Root cause 2층**:
1. **mid-기준 signal**: signal.basis_bps는 mid 기준이지만 fill은 bid/ask
   → 양 거래소 bid-ask spread 14bp 자동 cost로 자동 잠식.
   mid -20bp 진입해도 실 edge ~5bp 미만.
2. **Latency 1000-7000ms**: spike 1-2초 지속 동안 못 잡음.
   3261/3263은 spike가 반대로 튀어 의도와 정반대 방향 진입 (slip 33-56bp).

**Latency 분석**:
- HL: cache fix(404d7cc) 후 첫 거래 2235ms → 후속 865-1157ms (cache 작동
  but SDK signing/order RTT 1초+ 한계)
- KIS: 218ms ~ 3253ms variance 극심
  → 매 호출 새 `aiohttp.ClientSession`이 매번 TCP+TLS+DNS handshake.
  218ms 케이스는 OS connection 우연 재사용.

**Fix**:
- `475c041` KIS persistent session — 200-500ms target
- `e2dba04` spike-chasing 가드 (slip cap + min_hold) — latency 자체는 fix X
  but spike-chasing entry 차단해서 손실 누적 방지

**Spike origin 분석** (사용자 질문 답):
A) 진짜 mispricing — 양 호가창에 실제 cross 가능한 가격차 (잡을 수 있는 edge)
B) HL oracle lag — KIS 가격 jump 후 HL oracle 0.5-1초 뒤 따라옴 (시차 artifact)
C) KIS WS push delay — KIS 일시 stale 후 push 재개 시 spike처럼 보임 (artifact)

봇이 A/B/C 모두 trigger → false positive 많음. 진짜 edge 잡으려면:
- entry_threshold 35bp+ (현재 20bp는 B/C까지 잡음)
- signal smoothing (1 tick spike → 3 tick 연속 유지)
- exec_basis 기반 trigger (bid/ask cost 자동 반영)

다음 entry signal에서 latency 측정 + slip 분포 확인 후 추가 fix 결정.

### Latency 작업 추적 (앞으로 모든 latency fix 여기 기록)

| Commit | 효과 | leg | 단축 |
|---|---|---|---|
| `404d7cc` | HL `_build_exchange` cache + `ref_price` hint | HL | first cold 7491ms → warm 865-1157ms |
| `475c041` | KIS persistent aiohttp session + DNS cache | KIS | 218-3253ms variance → 200-500ms 목표 |
| TBD | HL eth_account signing offload (ProcessPoolExecutor) | HL | 100-200ms 절감 추정 |
| TBD | HL Info object cache (universe lookup) | HL | 추가 측정 후 결정 |

**목표 latency**: 양 leg 500ms 이내. 현재 HL 1000ms + KIS persistent session 후
200-500ms 예상 → 다음 entry로 검증.

### 2026-05-18 LIVE 첫 거래 분석 — spike latency 14bp 손실 → fix
첫 LIVE 거래 (id=3260) 분석에서 **PAPER ↔ LIVE 사이 평균 12bp slippage 차이**
가 발견됨. 원인은 spread 모델 차이가 **아니라 entry latency**.

- Signal 발생 시점 (ts=718.6): basis mid -21.35bp (futures가 102.94 → 103.04로
  점프, 1.4초간 지속).
- 봇 entry fill 시점 (ts=721.0, +2.4초): basis -6.79bp (mid reversion 완료).
- spike profit 14.5bp가 latency에 잡혀먹힘.

**비교 — PAPER 거래 (3251-3259)**:
- in-memory sync fill (ms 단위) → spike 정점 그대로 잡음
- 평균 slippage 1.5-2.5bp만 (대부분 spike 진행 중 fill)

**Root cause** — HL place_order 안에 3 round-trip:
1. `_build_exchange()` 매 호출 (SDK metaAndAssetCtxs REST, 200-500ms)
2. `get_market_data()` (시장가 limit_px 계산용 REST, 200-500ms)
3. 실 `order()` 호출 (200-500ms)

총 600-1500ms/leg, 양 leg gather라 ~1.5s. 1.4s spike에 부적합.

**Fix (404d7cc)**:
1. `_build_exchange()` 캐싱 — 봇 startup 시 1회 build, 이후 재사용.
2. `place_order(ref_price=...)` 인자 — adapter가 in-memory mark_price 전달 →
   REST `get_market_data()` 호출 생략.
3. `latency=XXXms` 로그 — 다음 거래에서 실측 가능.
4. 기대: HL leg 600-1500ms → 150-400ms → 1.4s spike 잡힘 + PAPER 수준 slippage.

**교훈** — PAPER over-optimistic 가설은 부분만 맞음 (KIS 환산비용 등). 진짜 차이는
양 leg fill 시차 동안의 mean reversion. low-latency 경로가 spike 잡는 본질.
다음 LIVE entry에서 `latency=XXXms` 측정값 + 실 exec spread로 검증 필요.

### 2026-05-15 LIVE 첫 진입 시도 incident (자료)
HL fill OK + KIS fail (위험고지 미등록) → 단일 leg 노출. emergency unwind 호출은
됐지만 HL invalid price 버그(당시)로 unwind도 fail → 사용자 수동 청산.

근본 fix들 (이번 incident 직후 commit):
- 41461f2 — HL 5 sig figs round (mark*1.05 → 6 sig → reject 해소)
- d7369fd — asyncio.Lock per pair (KIS 초당 거래 한도 위반 방지)
- d7369fd — unwind 결과 Telegram 알림 (silent failure 인지 가능)
- 600b765 — HTS ID 헤더 (잔고/주문 일부 endpoint 검증)

이 incident 이후 LIVE 안전장치 완전 wire 완료 (다음 거래는 5/18에 성공).

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
