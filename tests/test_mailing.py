import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.methods import SendMessage
from aiogram.types import Update

from main import (Config, EMOJI_DELETE, EMOJI_MAILING, MAILING_ADMIN_ID, MirrorManager,
                  SaveBot, Store, build_dispatcher, send_html_media)
from tests.test_mirrors import FakeBot


@pytest.fixture
async def setup(tmp_path):
    cfg = Config('999:main-token', tmp_path / 'data/app.db', tmp_path / 'data/media', 10000, 'https://example.com')
    store = Store(cfg.database)
    manager = MirrorManager(cfg, store, FakeBot)
    bot = Bot(cfg.token)
    bot.send_message = AsyncMock()
    dp = build_dispatcher(SaveBot(cfg, store), manager)
    yield manager, store, bot, dp
    await manager.shutdown()
    store.close()
    await bot.session.close()


def update(update_id, user_id, text, *, chat_id=None, chat_type='private'):
    entity = []
    if text.startswith('/'):
        entity = [{'type': 'bot_command', 'offset': 0, 'length': len(text.split()[0])}]
    return Update.model_validate({'update_id': update_id, 'message': {
        'message_id': update_id, 'date': 1700000000, 'chat': {'id': chat_id or user_id, 'type': chat_type},
        'from': {'id': user_id, 'is_bot': False, 'first_name': 'User'}, 'text': text, 'entities': entity,
    }})


@pytest.mark.asyncio
async def test_full_mailing_flow_only_admin_and_private_recipients(setup):
    manager, store, bot, dp = setup
    await dp.feed_update(bot, update(1, 101, '/start'))
    await dp.feed_update(bot, update(2, 102, 'любое сообщение'))
    await dp.feed_update(bot, update(3, 103, 'group', chat_id=-100, chat_type='group'))
    await dp.feed_update(bot, update(4, 101, '/mailing unauthorized'))
    assert 'только владельцу' in bot.send_message.await_args.args[1]
    assert not manager.mailings
    assert store.mailing_recipients() == [101, 102]
    bot.send_message.reset_mock()
    await dp.feed_update(bot, update(5, MAILING_ADMIN_ID, '/mailing Привет <everyone> & друзья'))
    await manager.mailings[bot.id]
    calls = bot.send_message.await_args_list
    assert 'Получателей: 3' in calls[0].args[1]
    sent = [c for c in calls if '<b>Рассылка</b>' in c.args[1]]
    assert {c.args[0] for c in sent} == {101, 102, MAILING_ADMIN_ID}
    assert all(c.args[1] == f'{EMOJI_MAILING} <b>Рассылка</b>\n\nПривет &lt;everyone&gt; &amp; друзья' for c in sent)
    assert all(c.kwargs['parse_mode'] == 'HTML' for c in sent)
    assert calls[-1].args[0] == MAILING_ADMIN_ID
    assert 'Доставлено: 3' in calls[-1].args[1]


@pytest.mark.asyncio
async def test_missing_text_and_too_long_do_not_start_mailing(setup):
    manager, store, bot, dp = setup
    await dp.feed_update(bot, update(1, MAILING_ADMIN_ID, '/mailing'))
    assert 'Укажите текст' in bot.send_message.await_args.args[1]
    await dp.feed_update(bot, update(2, MAILING_ADMIN_ID, '/mailing ' + '🙂' * 2001))
    assert 'слишком длинный' in bot.send_message.await_args.args[1]
    assert not manager.mailings


@pytest.mark.asyncio
async def test_main_and_mirror_audiences_are_isolated(setup):
    manager, store, bot, dp = setup
    await manager.add(777, '123:mirror-token')
    endpoint = manager.running[123]
    mirror_bot, mirror_store, mirror_dp = endpoint.bot, endpoint.store, endpoint.dispatcher
    # Feed real Telegram updates through the separate dispatchers.
    await dp.feed_update(bot, update(1, 101, '/start'))
    await mirror_dp.feed_update(mirror_bot, update(2, 202, '/start'))
    assert store.mailing_recipients() == [101]
    assert mirror_store.mailing_recipients() == [202]
    mirror_bot.send_message.reset_mock()
    await mirror_dp.feed_update(mirror_bot, update(3, MAILING_ADMIN_ID, '/mailing только зеркало'))
    await manager.mailings[123]
    sent = [call for call in mirror_bot.send_message.await_args_list if '<b>Рассылка</b>' in call.args[1]]
    assert {call.args[0] for call in sent} == {202, MAILING_ADMIN_ID}
    assert 101 not in {call.args[0] for call in sent}
    # The mirror creator is not automatically granted mailing rights.
    await mirror_dp.feed_update(mirror_bot, update(4, 777, '/mailing не владелец'))
    assert 'только владельцу' in mirror_bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_flood_retry_forbidden_and_send_error_do_not_abort(setup):
    manager, store, bot, dp = setup
    for chat in (11, 12, 13, 14):
        store.remember_mailing_user(chat)
    method = SendMessage(chat_id=11, text='x')
    attempts = {}
    async def send(chat_id, text, **kwargs):
        if '<b>Рассылка</b>' not in text:
            return
        attempts[chat_id] = attempts.get(chat_id, 0) + 1
        if chat_id == 11 and attempts[chat_id] == 1:
            raise TelegramRetryAfter(method=method, message='flood', retry_after=0)
        if chat_id == 12:
            raise TelegramForbiddenError(method=method, message='bot blocked')
        if chat_id == 13:
            raise RuntimeError('network failure')
    bot.send_message.side_effect = send
    assert manager.start_mailing(bot, store, store.mailing_recipients(), 'test')
    await manager.mailings[bot.id]
    assert attempts == {11: 2, 12: 1, 13: 1, 14: 1}
    assert store.mailing_recipients() == [11, 13, 14]
    summary = bot.send_message.await_args.args[1]
    assert 'Доставлено: 2; не доставлено: 2' in summary


@pytest.mark.asyncio
async def test_one_mailing_at_a_time_and_shutdown_cancels(setup):
    manager, store, bot, dp = setup
    entered, release = asyncio.Event(), asyncio.Event()
    async def send(chat_id, text, **kwargs):
        if '<b>Рассылка</b>' in text:
            entered.set()
            await release.wait()
    bot.send_message.side_effect = send
    assert manager.start_mailing(bot, store, [1], 'first')
    await entered.wait()
    assert not manager.start_mailing(bot, store, [2], 'second')
    await manager.shutdown()
    assert not manager.mailings


@pytest.mark.asyncio
async def test_custom_emoji_falls_back_without_losing_message(setup):
    manager, store, bot, dp = setup
    store.remember_mailing_user(101)
    method = SendMessage(chat_id=101, text='x')
    async def send(chat_id, text, **kwargs):
        if '<tg-emoji' in text:
            raise TelegramBadRequest(method=method, message='custom emoji unsupported')
    bot.send_message.side_effect = send
    assert manager.start_mailing(bot, store, [101], 'Текст')
    await manager.mailings[bot.id]
    sent = [c.args[1] for c in bot.send_message.await_args_list if c.args[0] == 101]
    assert sent == [f'{EMOJI_MAILING} <b>Рассылка</b>\n\nТекст', '📢 <b>Рассылка</b>\n\nТекст']
    assert 'Доставлено: 1' in bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_media_custom_emoji_fallback():
    method = SendMessage(chat_id=1, text='x')
    sender = AsyncMock(side_effect=[TelegramBadRequest(method=method, message='custom emoji'), None])
    await send_html_media(sender, 1, 'file', f'{EMOJI_DELETE} <b>Удалено</b>')
    assert sender.await_count == 2
    assert sender.await_args.kwargs['caption'] == '🗑 <b>Удалено</b>'


@pytest.mark.asyncio
async def test_removing_mirror_cancels_its_mailing_before_closing_store(setup):
    manager, store, bot, dp = setup
    await manager.add(777, '123:mirror-token')
    endpoint = manager.running[123]
    entered = asyncio.Event()
    async def wait_send(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()
    endpoint.bot.send_message.side_effect = wait_send
    assert manager.start_mailing(endpoint.bot, endpoint.store, [100], 'test')
    await entered.wait()
    assert await manager.remove(777, 123) == (True, True)
    assert 123 not in manager.mailings
    assert 123 not in manager.running


@pytest.mark.asyncio
async def test_nonprivate_and_other_chat_not_added(setup):
    manager, store, bot, dp = setup
    await dp.feed_update(bot, update(1, 101, 'group', chat_id=-100, chat_type='group'))
    await dp.feed_update(bot, update(2, 101, 'different private chat', chat_id=102))
    assert store.mailing_recipients() == []


@pytest.mark.asyncio
async def test_emoji_warning_falls_back_for_notifications(setup):
    from main import EMOJI_WARNING, send_html
    manager, store, bot, dp = setup
    method = SendMessage(chat_id=101, text='x')
    bot.send_message.side_effect = [TelegramBadRequest(method=method, message='custom emoji not allowed'), None]
    await send_html(bot, 101, f'{EMOJI_WARNING} <b>Ошибка</b>')
    assert bot.send_message.await_args.args[1] == '⚠️ <b>Ошибка</b>'


@pytest.mark.asyncio
async def test_running_mailing_command_does_not_start_another(setup):
    manager, store, bot, dp = setup
    entered = asyncio.Event()
    async def send(chat_id, text, **kwargs):
        if '<b>Рассылка</b>' in text:
            entered.set()
            await asyncio.Event().wait()
    bot.send_message.side_effect = send
    assert manager.start_mailing(bot, store, [101], 'first')
    await entered.wait()
    await dp.feed_update(bot, update(1, MAILING_ADMIN_ID, '/mailing second'))
    assert 'уже выполняется' in bot.send_message.await_args.args[1]
    await manager.stop_mailing(bot.id)


def test_audience_persists_and_is_deduplicated(tmp_path):
    path = tmp_path / 'app.db'
    store = Store(path)
    store.remember_mailing_user(101)
    store.remember_mailing_user(101)
    store.remember_mailing_user(102)
    store.close()
    restored = Store(path)
    try:
        assert restored.mailing_recipients() == [101, 102]
    finally:
        restored.close()
