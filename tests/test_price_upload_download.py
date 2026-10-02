import asyncio

import pytest
from telegram.error import TimedOut

from dieselup.bot import admin


class _TelegramFile:
    def __init__(self, failures: int = 0):
        self.failures = failures
        self.calls = []

    async def download_to_drive(self, **kwargs):
        self.calls.append(kwargs)
        if self.failures:
            self.failures -= 1
            raise TimedOut("download timed out")


class _Bot:
    def __init__(self, tg_file):
        self.tg_file = tg_file
        self.calls = []

    async def get_file(self, file_id, **kwargs):
        self.calls.append((file_id, kwargs))
        return self.tg_file


def test_price_document_download_retries_timeout(monkeypatch):
    tg_file = _TelegramFile(failures=2)
    bot = _Bot(tg_file)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(admin.asyncio, "sleep", no_sleep)
    asyncio.run(admin._download_price_document(bot, "file-123", "/tmp/prices.xls"))

    assert len(bot.calls) == 3
    assert len(tg_file.calls) == 3
    assert bot.calls[0][1]["read_timeout"] == 60.0
    assert tg_file.calls[0]["read_timeout"] == 60.0


def test_price_document_download_raises_after_final_timeout(monkeypatch):
    tg_file = _TelegramFile(failures=3)
    bot = _Bot(tg_file)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(admin.asyncio, "sleep", no_sleep)
    with pytest.raises(TimedOut):
        asyncio.run(admin._download_price_document(bot, "file-456", "/tmp/prices.xls"))

    assert len(bot.calls) == 3
