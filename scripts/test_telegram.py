"""Telegram notifier 진단 스크립트.

사용법:
    python3 scripts/test_telegram.py

진단 항목:
  1. settings.yaml + secrets.yaml에서 telegram 설정이 올바르게 로드됐는지
  2. notifier가 enabled로 초기화됐는지
  3. 실 메시지 1건 전송 + Telegram 응답 status 확인
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# repo root sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.config import load_config
from src.utils.notifier import TelegramNotifier


async def main() -> int:
    print("=" * 60)
    print("Telegram Notifier Diagnostic")
    print("=" * 60)

    cfg = load_config("config/settings.yaml", "config/secrets.yaml")
    tg = cfg.telegram

    print(f"  settings.yaml `telegram.enabled` = {tg.enabled}")
    print(f"  secrets.yaml  `telegram.bot_token` set = {bool(tg.bot_token)} "
          f"(len={len(tg.bot_token)})")
    print(f"  secrets.yaml  `telegram.chat_id`   = "
          f"{(tg.chat_id[:5] + '...') if tg.chat_id else 'EMPTY'}")
    print()

    if not tg.enabled:
        print("[FAIL] enabled=false in settings.yaml — set to true and re-run")
        return 1
    if not tg.bot_token or not tg.chat_id:
        print("[FAIL] secrets.yaml missing bot_token or chat_id")
        print("       Edit config/secrets.yaml, fill telegram: { bot_token, chat_id }")
        return 1

    notifier = TelegramNotifier(
        bot_token=tg.bot_token, chat_id=tg.chat_id, enabled=True,
    )
    if not notifier.enabled:
        print("[FAIL] notifier.enabled=False after construction (unexpected)")
        return 1

    print("[OK ] config loaded + notifier ENABLED. Sending test message...")
    try:
        await notifier.send(
            "🧪 <b>Telegram test from rwa_arbitrage</b>\n"
            "If you see this, alerts are wired correctly."
        )
    except Exception as e:
        print(f"[FAIL] send() raised: {e}")
        return 2

    print("[OK ] send() returned without exception.")
    print()
    print("Check your Telegram chat. If no message arrived:")
    print("  - bot_token wrong (check @BotFather → your bot)")
    print("  - chat_id wrong (start a chat with your bot, then visit:")
    print("      https://api.telegram.org/bot<TOKEN>/getUpdates")
    print("    and copy 'chat.id' from the latest 'message' object)")
    print("  - bot blocked / not added to group")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
