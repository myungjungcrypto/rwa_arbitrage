"""Streamlit 페이퍼 트레이딩 대시보드.

이 패키지는 SQLite DB를 read-only로 읽어 실시간 모니터링 + 거래 내역 분석
인터페이스 제공. 봇과 별도 프로세스 (`pm2 start rwa-arb-dashboard`).

서브모듈:
  queries — SQL → pandas DataFrame (pure, testable)
  charts  — Plotly Figure 생성기
  app     — Streamlit UI 엔트리 (streamlit run dashboard/app.py)
"""
