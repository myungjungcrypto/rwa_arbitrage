# rwa_arb 페이퍼 트레이딩 대시보드

Streamlit 기반 read-only 모니터링 인터페이스. 봇과 별도 PM2 프로세스로 동작.

## 무엇을 보여주는가

- **Live state**: open positions, closed trades, cumulative PnL, signals 카운터, near-miss skip 분해
- **Basis chart**: 최근 N시간 basis_bps 시계열 + ±3σ band + 진입/청산 마커 + 임계값 라인
- **Trade history**: 최근 N건 closed trade (entry/exit price, hold, realized/funding/fees, net, W/L)
- **Open positions**: 현재 보유 포지션 + unrealized PnL
- **Daily PnL**: 일별 net PnL bar + 누적 차트 + 테이블
- **Entry funnel**: signals → entry_signals → exec/warmup/min_abs skip → entries
- **Win/Loss buckets**: 진입 spread 절대값 버킷별 WR + total PnL
- **Hold time buckets**: 보유 시간별 avg PnL
- **Entry vs PnL scatter**: 진입 spread vs net 결과 산점도

## 설치

봇이 이미 돌고 있는 EC2에서:

```bash
cd ~/rwa_arbitrage
pip install -r requirements.txt --user      # streamlit + pandas + plotly + streamlit-autorefresh
```

## 실행 (수동)

```bash
streamlit run dashboard/app.py \
  --server.address=127.0.0.1 \
  --server.port=8501 \
  --server.headless=true
```

DB 경로 변경 시 환경변수:
```bash
RWA_DB_PATH=/path/to/arbitrage.db streamlit run dashboard/app.py ...
```

## PM2로 등록 (운영)

```bash
pm2 start --name rwa-arb-dashboard --interpreter none -- \
  streamlit run dashboard/app.py \
    --server.address=127.0.0.1 \
    --server.port=8501 \
    --server.headless=true \
    --browser.gatherUsageStats=false

pm2 save
```

`--server.address=127.0.0.1`로 외부 노출 차단. 접근은 SSH 터널로만.

## 로컬 접속 (SSH 터널)

```bash
ssh -L 8501:localhost:8501 ec2
# 브라우저: http://localhost:8501
```

`~/.ssh/config`에 alias 등록 권장:
```
Host ec2
  HostName <EC2 public IP>
  User ec2-user
  IdentityFile ~/.ssh/your-key.pem
  LocalForward 8501 localhost:8501
```

이러면 `ssh ec2`만으로 자동 포트 포워딩.

## 봇 측 요구사항

봇이 30초마다 `engine_state` 테이블에 스냅샷 dump하고 있어야 live state가 보임.
schema v3 이상 (자동 마이그레이션됨). 확인:

```bash
python3 -c "
import sqlite3
con = sqlite3.connect('data/arbitrage.db')
print(con.execute('SELECT value FROM schema_meta WHERE key=\"version\"').fetchone())
print(con.execute('SELECT COUNT(*) FROM engine_state').fetchone())
"
```

`('3',)` + 1 이상이면 정상.

## 트러블슈팅

**대시보드 비어있음 / "No state snapshot yet"**
- 봇이 안 돌고 있거나 첫 30초 이내 → `pm2 logs rwa-arb` 확인
- v3 마이그레이션 안 됨 → 봇 재시작으로 자동 처리

**DB lock 에러**
- read-only URI(`mode=ro`)로 열기 때문에 거의 발생 안 함
- 발생 시 SQLite가 WAL 모드인지 확인 (`PRAGMA journal_mode`)

**포트 8501 이미 사용 중**
- `lsof -i :8501` 또는 `pm2 list`로 기존 인스턴스 확인
- 다른 포트로 변경 가능 (`--server.port=8502`)

**자동 새로고침 안 됨**
- `pip install streamlit-autorefresh` 누락 → 사이드바 우측에 경고
- 폴백: 브라우저 F5 / Ctrl+R

## 보안

- `--server.address=127.0.0.1` 강제 (외부 차단)
- SSH 터널 외 접근 경로 없음
- 대시보드는 read-only (DB 변경 절대 안 함)
- secrets.yaml 등 절대 노출 안 함
