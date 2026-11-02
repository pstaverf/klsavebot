import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer
from cryptography.fernet import Fernet

from main import Config, Store, WebhookServer


class FakeBot:
    def __init__(self, bot_id):
        self.id = bot_id
        self.token = f'{bot_id}:sensitive-token'
        self.set_webhook = AsyncMock(return_value=True)


@pytest.fixture
async def server(tmp_path):
    cfg = Config('1:main', tmp_path / 'app.db', tmp_path / 'media', 10000, 'https://example.com/prefix')
    store = Store(cfg.database)
    app = WebhookServer(cfg, Fernet.generate_key())
    bot = FakeBot(1)
    dispatcher = SimpleNamespace(feed_raw_update=AsyncMock())
    endpoint = await app.register(bot, dispatcher, store)
    client = TestClient(TestServer(app.app))
    await client.start_server()
    yield app, bot, dispatcher, store, endpoint, client
    await client.close()
    store.close()


def path(app, bot_id):
    return app.prefix + '/telegram/' + app.credentials(bot_id)[0]


def headers(app, bot_id):
    return {'X-Telegram-Bot-Api-Secret-Token': app.credentials(bot_id)[1]}


@pytest.mark.asyncio
async def test_register_and_authorized_dispatch(server):
    app, bot, dp, store, endpoint, client = server
    args = bot.set_webhook.await_args.kwargs
    assert args['url'] == 'https://example.com' + path(app, 1)
    assert args['secret_token'] == headers(app, 1)['X-Telegram-Bot-Api-Secret-Token']
    assert 'sensitive-token' not in args['url']
    assert args['max_connections'] == 1 and args['drop_pending_updates'] is False
    response = await client.post(path(app, 1), json={'update_id': 10}, headers=headers(app, 1))
    assert response.status == 200
    dp.feed_raw_update.assert_awaited_once_with(bot, {'update_id': 10})
    # Telegram retries the same update: it is processed only once, even across restarts.
    response = await client.post(path(app, 1), json={'update_id': 10}, headers=headers(app, 1))
    assert response.status == 200 and dp.feed_raw_update.await_count == 1
    assert store.db.execute('SELECT update_id FROM webhook_updates').fetchone()[0] == 10


@pytest.mark.asyncio
async def test_unknown_route_bad_secret_and_malformed_updates(server):
    app, bot, dp, store, endpoint, client = server
    url = path(app, 1)
    assert (await client.post(url, json={'update_id': 1})).status == 403
    assert (await client.post(url, json={'update_id': 1}, headers={'X-Telegram-Bot-Api-Secret-Token': 'wrong'})).status == 403
    assert (await client.post(path(app, 2), json={'update_id': 1}, headers=headers(app, 1))).status == 404
    assert (await client.post(url, data='not json', headers=headers(app, 1))).status == 400
    assert (await client.post(url, json={'message': {}}, headers=headers(app, 1))).status == 400
    assert (await client.post(url, data='x' * (5 * 1024 * 1024 + 1), headers=headers(app, 1))).status == 413
    dp.feed_raw_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_failure_is_retried_without_recording_update(server):
    app, bot, dp, store, endpoint, client = server
    dp.feed_raw_update.side_effect = [RuntimeError('secret should not be logged'), None]
    for expected in (500, 200, 200):
        response = await client.post(path(app, 1), json={'update_id': 2}, headers=headers(app, 1))
        assert response.status == expected
    assert dp.feed_raw_update.await_count == 2


@pytest.mark.asyncio
async def test_two_webhooks_are_isolated_and_use_separate_dedup(server, tmp_path):
    app, bot, dp, store, endpoint, client = server
    other_store = Store(tmp_path / 'other.db')
    other_bot = FakeBot(2)
    other_dp = SimpleNamespace(feed_raw_update=AsyncMock())
    await app.register(other_bot, other_dp, other_store)
    response = await client.post(path(app, 2), json={'update_id': 11}, headers=headers(app, 1))
    assert response.status == 403
    response = await client.post(path(app, 1), json={'update_id': 11}, headers=headers(app, 1))
    assert response.status == 200
    response = await client.post(path(app, 2), json={'update_id': 11}, headers=headers(app, 2))
    assert response.status == 200
    dp.feed_raw_update.assert_awaited_once()
    other_dp.feed_raw_update.assert_awaited_once()
    await app.detach(2)
    assert (await client.post(path(app, 2), json={'update_id': 12}, headers=headers(app, 2))).status == 404
    other_store.close()


@pytest.mark.asyncio
async def test_concurrent_duplicate_delivery_processed_once(server):
    app, bot, dp, store, endpoint, client = server
    async def slow(*args):
        await asyncio.sleep(0.02)
    dp.feed_raw_update.side_effect = slow
    responses = await asyncio.gather(*(client.post(path(app, 1), json={'update_id': 3}, headers=headers(app, 1)) for _ in range(3)))
    assert [response.status for response in responses] == [200, 200, 200]
    assert dp.feed_raw_update.await_count == 1


@pytest.mark.asyncio
async def test_detach_waits_for_inflight_update(server):
    app, bot, dp, store, endpoint, client = server
    entered, release = asyncio.Event(), asyncio.Event()
    async def processing(*args):
        entered.set()
        await release.wait()
    dp.feed_raw_update.side_effect = processing
    request = asyncio.create_task(client.post(path(app, 1), json={'update_id': 5}, headers=headers(app, 1)))
    await entered.wait()
    detach = asyncio.create_task(app.detach(1))
    await asyncio.sleep(0)
    assert not detach.done()
    release.set()
    assert (await request).status == 200
    assert await detach is endpoint
    assert (await client.post(path(app, 1), json={'update_id': 6}, headers=headers(app, 1))).status == 404


def test_config_requires_https_and_valid_port(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('BOT_TOKEN', '1:secret')
    for value in ('', 'http://example.com', 'https://host/?x=1', 'https://user:pass@host', 'https://host/#x'):
        monkeypatch.setenv('WEBHOOK_BASE_URL', value)
        with pytest.raises(ValueError):
            Config.from_env()
    monkeypatch.setenv('WEBHOOK_BASE_URL', 'https://example.com/telegram-proxy/')
    monkeypatch.setenv('WEBHOOK_PORT', '8080')
    assert Config.from_env().webhook_base_url == 'https://example.com/telegram-proxy'
    monkeypatch.setenv('WEBHOOK_PORT', '99999')
    with pytest.raises(ValueError):
        Config.from_env()


@pytest.mark.asyncio
async def test_webhook_routes_real_aiogram_start_handler(tmp_path):
    from aiogram import Bot
    from main import MirrorManager, SaveBot, build_dispatcher
    cfg = Config('1:secret-token', tmp_path / 'app.db', tmp_path / 'media', 10000, 'https://example.com')
    store = Store(cfg.database)
    manager = MirrorManager(cfg, store)
    bot = Bot(cfg.token)
    bot.set_webhook = AsyncMock(return_value=True)
    bot.send_message = AsyncMock()
    dp = build_dispatcher(SaveBot(cfg, store), manager)
    await manager.webhooks.register(bot, dp, store)
    client = TestClient(TestServer(manager.webhooks.app))
    await client.start_server()
    try:
        payload = {'update_id': 50, 'message': {
            'message_id': 4, 'date': 1700000000, 'chat': {'id': 42, 'type': 'private'},
            'from': {'id': 42, 'is_bot': False, 'first_name': 'User'}, 'text': '/start',
            'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}],
        }}
        response = await client.post(path(manager.webhooks, 1), json=payload, headers=headers(manager.webhooks, 1))
        assert response.status == 200
        assert 'Добро пожаловать!' in bot.send_message.await_args.args[1]
        response = await client.post(path(manager.webhooks, 1), json=payload, headers=headers(manager.webhooks, 1))
        assert response.status == 200 and bot.send_message.await_count == 1
    finally:
        await client.close()
        await manager.shutdown()
        store.close()
        await bot.session.close()
