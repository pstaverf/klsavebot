import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main
from main import Config, Menu, MirrorError, MirrorManager, Store


class FakeBot:
    def __init__(self, token, *, webhook='', bad=False, set_fail=False):
        self.token = token
        self.id = int(token.split(':', 1)[0])
        self.username = f'bot{self.id}'
        self.session = SimpleNamespace(close=AsyncMock())
        self.get_me = AsyncMock(return_value=SimpleNamespace(id=self.id, is_bot=True, username=self.username))
        if bad:
            self.get_me.side_effect = RuntimeError('secret=' + token)
        self.webhook = webhook
        async def set_webhook(**kwargs):
            if set_fail:
                raise RuntimeError('cannot register')
            self.webhook = kwargs['url']
            return True
        async def get_webhook_info():
            return SimpleNamespace(url=self.webhook)
        async def delete_webhook(**kwargs):
            self.webhook = ''
            return True
        self.set_webhook = AsyncMock(side_effect=set_webhook)
        self.get_webhook_info = AsyncMock(side_effect=get_webhook_info)
        self.delete_webhook = AsyncMock(side_effect=delete_webhook)
        self.set_my_commands = AsyncMock()
        self.send_message = AsyncMock(return_value=SimpleNamespace(message_id=91))
        self.delete_message = AsyncMock()
        self.edit_message_text = AsyncMock()


@pytest.fixture
async def mirrors(tmp_path, monkeypatch):
    cfg = Config('999:mainsecret', tmp_path / 'data' / 'app.db', tmp_path / 'data' / 'media', 10000, 'https://example.com')
    store = Store(cfg.database)
    bots = []
    def factory(token):
        bot = FakeBot(token)
        bots.append(bot)
        return bot
    manager = MirrorManager(cfg, store, factory)
    yield manager, bots
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_add_remove_and_owner_isolation(mirrors):
    manager, bots = mirrors
    steps = []
    async def progress(percent, text):
        steps.append(percent)
    token = '123:supersecret'
    me = await manager.add(42, token, progress)
    assert me.id == 123 and steps == [20, 40, 60, 80]
    assert 123 in manager.running
    assert [r['bot_id'] for r in manager.list_for(42)] == [123]
    assert not manager.list_for(43)
    row = manager.store.db.execute('SELECT * FROM mirrors').fetchone()
    assert token.encode() not in row['token_encrypted']
    assert manager.cipher.decrypt(row['token_encrypted']).decode() == token
    assert (manager.cfg.database.parent / 'mirror.key').stat().st_mode & 0o777 == 0o600
    with pytest.raises(MirrorError):
        await manager.add(43, token)
    assert await manager.remove(43, 123) == (False, False)
    assert 123 in manager.running
    assert await manager.remove(42, 123) == (True, True)
    assert 123 not in manager.running and not manager.list_for(42)
    bots[0].session.close.assert_awaited()


@pytest.mark.asyncio
async def test_restore_and_remove_persists(mirrors, monkeypatch):
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    await manager.shutdown()
    assert len(manager.list_for(42)) == 1
    await manager.restore()
    await asyncio.sleep(0)
    assert 123 in manager.running
    assert await manager.remove(42, 123) == (True, True)
    await manager.restore()
    assert not manager.running


@pytest.mark.asyncio
async def test_invalid_duplicate_and_webhook_are_rejected_safely(mirrors):
    manager, bots = mirrors
    manager.bot_factory = lambda token: FakeBot(token, bad=True)
    with pytest.raises(MirrorError) as err:
        await manager.add(42, '123:secret')
    assert 'secret' not in str(err.value)
    manager.bot_factory = FakeBot
    with pytest.raises(MirrorError, match='уже подключён'):
        await manager.add(42, '999:mainsecret')
    assert not manager.list_for(42)


@pytest.mark.asyncio
async def test_failed_worker_is_not_registered(mirrors, monkeypatch):
    manager, bots = mirrors
    manager.bot_factory = lambda token: FakeBot(token, set_fail=True)
    with pytest.raises(MirrorError, match='вебхук'):
        await manager.add(42, '123:secret')
    assert not manager.list_for(42)
    assert 123 not in manager.running



def test_missing_encryption_key_is_not_silently_replaced(tmp_path):
    cfg = Config('999:main', tmp_path / 'app.db', tmp_path / 'media', 10000)
    store = Store(cfg.database)
    store.db.execute("INSERT INTO mirrors VALUES (123, 42, 'bot', X'00')")
    store.db.commit()
    with pytest.raises(RuntimeError, match='mirror.key'):
        MirrorManager(cfg, store)
    assert not (tmp_path / 'mirror.key').exists()
    store.close()


@pytest.mark.asyncio
async def test_mirror_uses_separate_storage_and_webhook(mirrors):
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    endpoint = manager.running[123]
    assert endpoint.store.db.execute('PRAGMA database_list').fetchone()[2] == str(manager.cfg.database.parent / 'mirrors' / '123' / 'app.db')
    assert bots[0].set_webhook.await_args.kwargs['url'] == manager.webhooks.url_for(123)
    assert bots[0].set_webhook.await_args.kwargs['allowed_updates'] == main.UPDATES
    assert bots[0].set_webhook.await_args.kwargs['max_connections'] == 1
    await manager.remove(42, 123)
    bots[0].delete_webhook.assert_awaited_once()
    bots[0].session.close.assert_awaited_once()


def private_message(user_id=42, message_id=1, text=None):
    return SimpleNamespace(from_user=SimpleNamespace(id=user_id), chat=SimpleNamespace(id=user_id, type='private'),
                           message_id=message_id, text=text)


def callback(data, user_id=42, chat_id=None):
    return SimpleNamespace(data=data, from_user=SimpleNamespace(id=user_id),
                           message=SimpleNamespace(chat=SimpleNamespace(id=chat_id or user_id, type='private'), message_id=5),
                           answer=AsyncMock())


@pytest.mark.asyncio
async def test_menu_navigation_cancel_and_secret_input(mirrors):
    manager, _ = mirrors
    menu = Menu(manager)
    bot = FakeBot('999:main')
    await menu.start(private_message(), bot)
    assert 'Добро пожаловать!' in bot.send_message.await_args.args[1]
    await menu.callback(callback('menu:page:1'), bot)
    bot.delete_message.assert_awaited_with(42, 5)
    assert 'Автоматизация чатов' in bot.send_message.await_args.args[1]
    assert '@bot999' in bot.send_message.await_args.args[1]
    await menu.callback(callback('menu:page:2'), bot)
    assert 'Хочешь полный контроль?' in bot.send_message.await_args.args[1]
    await menu.mirrors(private_message(), bot)
    assert (999, 42) in manager.pending
    assert 'Введите токен' in bot.send_message.await_args.args[1]
    await menu.callback(callback('menu:cancel'), bot)
    assert (999, 42) not in manager.pending
    bot.send_message.reset_mock()
    await menu.receive_token(private_message(text='123:secret'), bot)
    bot.send_message.assert_not_awaited()
    await menu.prompt(bot, 42, 42)
    await menu.receive_token(private_message(message_id=13, text='123:secret'), bot)
    bot.delete_message.assert_any_await(42, 13)
    assert not (999, 42) in manager.pending
    assert len(manager.list_for(42)) == 1
    sent = ' '.join(str(x) for x in bot.send_message.await_args_list + bot.edit_message_text.await_args_list)
    assert '123:secret' not in sent
    assert all(f'{n}%' in sent for n in (20, 40, 60, 80, 100))
    await menu.mirrors(private_message(), bot)
    assert '@bot123' in bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_cannot_remove_another_users_mirror(mirrors):
    manager, _ = mirrors
    await manager.add(42, '123:secret')
    bot = FakeBot('999:main')
    await Menu(manager).callback(callback('menu:delete:123', user_id=43), bot)
    assert len(manager.list_for(42)) == 1
    assert 123 in manager.running
    assert 'принадлежит другому' in bot.send_message.await_args.args[1]
    await Menu(manager).callback(callback('menu:delete:123', user_id=42), bot)
    await asyncio.gather(*manager.background)
    assert not manager.list_for(42) and 123 not in manager.running


@pytest.mark.asyncio
async def test_invalid_token_does_not_leak_or_register(mirrors):
    manager, _ = mirrors
    manager.bot_factory = lambda token: FakeBot(token, bad=True)
    menu = Menu(manager)
    bot = FakeBot('999:main')
    await menu.prompt(bot, 42, 42)
    await menu.receive_token(private_message(message_id=13, text='123:private-secret'), bot)
    bot.delete_message.assert_any_await(42, 13)
    assert not manager.list_for(42)
    assert (999, 42) in manager.pending
    assert 'private-secret' not in str(bot.edit_message_text.await_args_list)
    assert 'не удалось' in bot.edit_message_text.await_args.args[0].lower()


@pytest.mark.asyncio
async def test_self_deletion_stops_worker(mirrors):
    manager, _ = mirrors
    await manager.add(42, '123:secret')
    bot = FakeBot('123:secret')
    await Menu(manager).callback(callback('menu:delete:123'), bot)
    await asyncio.gather(*manager.background)
    assert not manager.list_for(42)
    assert 123 not in manager.running
    assert 'остановлено' in bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_cancel_does_not_cancel_another_users_input(mirrors):
    manager, _ = mirrors
    menu = Menu(manager)
    bot = FakeBot('999:main')
    await menu.prompt(bot, 42, 42)
    await menu.prompt(bot, 43, 43)
    await menu.callback(callback('menu:cancel', user_id=43), bot)
    assert (999, 42) in manager.pending and (999, 43) not in manager.pending


def test_dispatcher_registers_both_commands_and_callbacks(mirrors):
    manager, _ = mirrors
    dp = main.build_dispatcher(main.SaveBot(manager.cfg, manager.store), manager)
    assert set(dp.resolve_used_update_types()) >= {'message', 'callback_query', 'business_message',
                                                     'deleted_business_messages', 'edited_business_message'}


@pytest.mark.asyncio
async def test_telegram_dispatcher_handles_start_and_mirrors(mirrors):
    from aiogram import Bot
    from aiogram.types import Update
    manager, _ = mirrors
    bot = Bot('999:test-token')
    bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=99))
    dp = main.build_dispatcher(main.SaveBot(manager.cfg, manager.store), manager)
    def update(update_id, text):
        return Update.model_validate({'update_id': update_id, 'message': {
            'message_id': update_id, 'date': 1700000000, 'chat': {'id': 42, 'type': 'private'},
            'from': {'id': 42, 'is_bot': False, 'first_name': 'User'}, 'text': text,
            'entities': [{'type': 'bot_command', 'offset': 0, 'length': len(text)}],
        }})
    await dp.feed_update(bot, update(1, '/start'))
    assert 'Добро пожаловать!' in bot.send_message.await_args.args[1]
    await dp.feed_update(bot, update(2, '/mirrors'))
    assert 'Введите токен' in bot.send_message.await_args.args[1]
    await bot.session.close()


@pytest.mark.asyncio
async def test_existing_webhook_is_replaced(mirrors):
    manager, bots = mirrors
    manager.bot_factory = lambda token: FakeBot(token, webhook='https://old.example/webhook')
    await manager.add(42, '123:secret')
    endpoint = manager.running[123]
    assert endpoint.bot.webhook == manager.webhooks.url_for(123)
    assert await manager.remove(42, 123) == (True, True)


@pytest.mark.asyncio
async def test_removal_does_not_delete_replaced_external_webhook(mirrors):
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    bots[0].webhook = 'https://different.example/webhook'
    assert await manager.remove(42, 123) == (True, True)
    bots[0].delete_webhook.assert_not_awaited()


@pytest.mark.asyncio
async def test_remote_delete_failure_stops_locally_and_reports(mirrors):
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    bots[0].delete_webhook.side_effect = RuntimeError('network error')
    bot = FakeBot('999:main')
    await Menu(manager).callback(callback('menu:delete:123', user_id=42), bot)
    await asyncio.gather(*manager.background)
    assert not manager.list_for(42) and 123 not in manager.running
    assert 'не удалось отключить' in bot.send_message.await_args.args[1]
    assert manager.webhooks.credentials(123)[0] not in manager.webhooks.endpoints


@pytest.mark.asyncio
async def test_restore_does_not_reactivate_removed_mirror(mirrors):
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    await manager.shutdown()
    gate, release = asyncio.Event(), asyncio.Event()
    original = manager._prepare
    async def delayed(token):
        gate.set()
        await release.wait()
        return await original(token)
    manager._prepare = delayed
    restore = asyncio.create_task(manager.restore())
    await gate.wait()
    remove = asyncio.create_task(manager.remove(42, 123))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(restore, remove)
    assert not manager.list_for(42)
    assert 123 not in manager.running


@pytest.mark.asyncio
async def test_mirror_can_remove_itself_during_webhook_without_deadlock(mirrors):
    from aiohttp.test_utils import TestClient, TestServer
    manager, bots = mirrors
    await manager.add(42, '123:secret')
    endpoint = manager.running[123]
    bot = endpoint.bot
    async def handle(*args):
        await Menu(manager).callback(callback('menu:delete:123'), bot)
    endpoint.dispatcher.feed_raw_update = AsyncMock(side_effect=handle)
    client = TestClient(TestServer(manager.webhooks.app))
    await client.start_server()
    try:
        route, secret = manager.webhooks.credentials(123)
        response = await asyncio.wait_for(client.post('/telegram/' + route, json={'update_id': 21},
            headers={'X-Telegram-Bot-Api-Secret-Token': secret}), timeout=2)
        assert response.status == 200
        await asyncio.wait_for(asyncio.gather(*manager.background), timeout=2)
        assert 123 not in manager.running
        assert not manager.list_for(42)
        assert (await client.post('/telegram/' + route, json={'update_id': 22},
            headers={'X-Telegram-Bot-Api-Secret-Token': secret})).status == 404
    finally:
        await client.close()
