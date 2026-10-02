import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from dieselup.bot import admin


@pytest.mark.parametrize('status',['failed','held','completed'])
def test_upload_result_obeys_silent_sender_and_records_result(monkeypatch,status):
    document=SimpleNamespace(file_name='prices.xlsx',file_id='fake-local-id')
    message=SimpleNamespace(document=document,chat_id=-1,message_id=42,caption=None,reply_text=AsyncMock())
    receipt={'id':7,'provider':'loves','status':status,'row_count':606,'matched_rows':500,
             'effective_date':'2026-10-02','reason':'Missing date' if status!='completed' else None}
    save=AsyncMock(return_value=receipt);send=AsyncMock()
    monkeypatch.setattr(admin,'_download_price_document',AsyncMock())
    monkeypatch.setattr(admin,'save_import',save)
    monkeypatch.setattr(admin,'safe_send',send)
    asyncio.run(admin.handle_price_upload(SimpleNamespace(effective_message=message),SimpleNamespace(bot=object())))
    message.reply_text.assert_not_awaited();save.assert_awaited_once()
    assert save.call_args.kwargs['upload_key']=='telegram:-1:42'
    assert send.call_args.kwargs['queue_on_failure'] is False
    assert send.call_args.kwargs['alert_type']==('price_upload_completed' if status=='completed' else 'price_upload_rejected')


def test_silent_upload_does_not_call_telegram_send(monkeypatch):
    document=SimpleNamespace(file_name='prices.xlsx',file_id='fake-local-id')
    message=SimpleNamespace(document=document,chat_id=-1,message_id=43,caption=None,reply_text=AsyncMock())
    receipt={'id':8,'provider':'fts','status':'held','row_count':4744,'matched_rows':631,
             'effective_date':None,'reason':'Price date missing'}
    bot=SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(admin.settings,'TELEGRAM_MESSAGING_MODE','silent')
    monkeypatch.setattr(admin,'_download_price_document',AsyncMock())
    monkeypatch.setattr(admin,'save_import',AsyncMock(return_value=receipt))
    asyncio.run(admin.handle_price_upload(SimpleNamespace(effective_message=message),SimpleNamespace(bot=bot)))
    bot.send_message.assert_not_awaited();message.reply_text.assert_not_awaited()


def test_caption_correction_creates_new_idempotent_import(monkeypatch):
    from datetime import datetime,timezone
    document=SimpleNamespace(file_name='prices.xlsx',file_id='fake-local-id',file_unique_id='fixture-unique')
    message=SimpleNamespace(document=document,chat_id=-1,message_id=44,caption=None,edit_date=None)
    receipt={'id':9,'provider':'fts','status':'held','row_count':1,'matched_rows':1,
             'effective_date':None,'reason':'Price date missing'}
    save=AsyncMock(return_value=receipt)
    monkeypatch.setattr(admin,'_download_price_document',AsyncMock())
    monkeypatch.setattr(admin,'save_import',save)
    monkeypatch.setattr(admin,'safe_send',AsyncMock())
    update=SimpleNamespace(effective_message=message);context=SimpleNamespace(bot=object())
    asyncio.run(admin.handle_price_upload(update,context));original=save.call_args.kwargs['upload_key']
    message.caption='effective_date=2026-10-02';message.edit_date=datetime.now(timezone.utc)
    asyncio.run(admin.handle_price_upload(update,context));corrected=save.call_args.kwargs['upload_key']
    assert corrected!=original, 'Edited date caption was ignored because the earlier held import key was reused'
    assert save.call_args.kwargs['caption']=='effective_date=2026-10-02'
    asyncio.run(admin.handle_price_upload(update,context))
    assert save.call_args.kwargs['upload_key']==corrected
