import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.types import BusinessConnection, BusinessMessagesDeleted, Chat, Message, PhotoSize, User

from main import Config, SaveBot, Store, expiring_hint

OWNER = User(id=1, is_bot=False, first_name='Owner')
OTHER = User(id=2, is_bot=False, first_name='Other')
CHAT = Chat(id=2, type='private', first_name='Other')


def msg(mid, body='hello', user=OTHER, **kwargs):
    return Message(message_id=mid, date=datetime.now(timezone.utc), chat=CHAT, from_user=user,
                   business_connection_id='conn', text=body, **kwargs)


def connection(user=OWNER, enabled=True, id='conn'):
    return BusinessConnection(id=id, user=user, user_chat_id=user.id,
                              date=datetime.now(timezone.utc), is_enabled=enabled)


@pytest.fixture
def setup(tmp_path):
    cfg = Config('unused', 1, tmp_path / 'app.db', tmp_path / 'media', 10000)
    store = Store(cfg.database)
    store.save_connection(connection())
    bot = SimpleNamespace(**{name: AsyncMock() for name in ('send_message', 'send_photo', 'send_video',
        'send_animation', 'send_voice', 'send_audio', 'send_document', 'get_file', 'download_file', 'get_business_connection')})
    yield SaveBot(cfg, store), bot
    store.close()


@pytest.mark.asyncio
async def test_incoming_edits_deletes_and_owner_ignored(setup):
    app, bot = setup
    await app.message(msg(10, '<old>'), bot)
    await app.message(msg(10, 'new'), bot, edited=True)
    assert bot.send_message.await_count == 1
    text = bot.send_message.await_args.args[1]
    assert '&lt;old&gt;' in text and 'new' in text
    assert app.store.db.execute('select count(*) from save_edits').fetchone()[0] == 1
    await app.message(msg(10, 'new'), bot, edited=True)
    assert bot.send_message.await_count == 1
    await app.message(msg(11, 'own', OWNER), bot)
    await app.message(msg(11, 'own edit', OWNER), bot, edited=True)
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[10, 11, 999]), bot)
    assert bot.send_message.await_count == 2
    assert app.store.get('conn', 2, 10)['deleted'] == 1
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[10]), bot)
    assert bot.send_message.await_count == 2


@pytest.mark.asyncio
async def test_no_unknown_or_disabled_connection(setup):
    app, bot = setup
    bot.get_business_connection.return_value = connection(user=OTHER, id='bad')
    await app.message(msg(10).model_copy(update={'business_connection_id': 'bad'}), bot)
    assert not app.store.get('bad', 2, 10)
    app.store.save_connection(connection(enabled=False))
    await app.message(msg(10), bot)
    assert not app.store.get('conn', 2, 10)
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_media_download_edit_and_delete(setup):
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='photos/image.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    def photo(mid, unique, caption, spoiler=False):
        return msg(mid, body=None, caption=caption, has_media_spoiler=spoiler,
                   photo=[PhotoSize(file_id=unique, file_unique_id=unique, width=1, height=1, file_size=5)])
    await app.message(photo(20, 'a', 'caption', True), bot)
    original = app.store.get('conn', 2, 20)['media_path']
    assert bot.send_photo.await_count == 1
    await app.message(photo(20, 'a', 'new caption'), bot, edited=True)
    assert bot.download_file.await_count == 1
    await app.message(photo(20, 'b', 'new caption'), bot, edited=True)
    assert bot.download_file.await_count == 2
    assert app.store.get('conn', 2, 20)['media_path'] != original
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[20]), bot)
    assert bot.send_photo.await_count == 2


@pytest.mark.asyncio
async def test_owner_reply_can_save_other_media(setup):
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='x.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    reply = msg(10, body=None, photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    await app.message(msg(11, 'save', OWNER, reply_to_message=reply), bot)
    assert app.store.get('conn', 2, 10)['media_status'] == 'downloaded'
    assert bot.send_photo.await_count == 1
    assert app.store.get('conn', 2, 11) is None


def test_legacy_import_preserves_owner_guard(tmp_path):
    path = tmp_path / 'db.sqlite'
    db = sqlite3.connect(path)
    db.execute('''CREATE TABLE messages (source TEXT, business_connection_id TEXT, chat_id INTEGER,
        message_id INTEGER, sender_id INTEGER, sender_name TEXT, chat_title TEXT, text TEXT, caption TEXT,
        media_type TEXT, media_path TEXT, media_status TEXT, deleted INTEGER, direction TEXT)''')
    db.execute("INSERT INTO messages VALUES ('business','conn',2,1,2,'Other','Chat','hello',NULL,NULL,NULL,NULL,0,'incoming')")
    db.execute("INSERT INTO messages VALUES ('business','conn',2,2,1,'Owner','Chat','own',NULL,NULL,NULL,NULL,0,'outgoing')")
    db.commit()
    db.close()
    store = Store(path)
    assert store.get('conn', 2, 1)['body'] == 'hello'
    assert store.get('conn', 2, 2) is None
    store.close()


def test_spoiler_and_regular_message():
    assert expiring_hint(msg(1, has_media_spoiler=True))
    assert not expiring_hint(msg(2))


@pytest.mark.asyncio
async def test_untracked_edit_and_old_owner_row(setup):
    app, bot = setup
    await app.message(msg(30, 'new'), bot, edited=True)
    assert '[нет старой версии]' in bot.send_message.await_args.args[1]
    app.store.save(('conn', 2, 31, OWNER.id, 'Owner', 'Chat', 'own', None, None, None, None))
    await app.message(msg(31, 'updated', OWNER), bot, edited=True)
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[31]), bot)
    assert bot.send_message.await_count == 1


@pytest.mark.asyncio
async def test_media_size_and_failed_download(setup):
    app, bot = setup
    large = msg(40, body=None, photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1, file_size=10001)])
    await app.message(large, bot)
    assert app.store.get('conn', 2, 40)['media_status'] == 'too_large'
    bot.get_file.assert_not_awaited()
    bot.get_file.side_effect = RuntimeError('API error')
    small = msg(41, body=None, photo=[PhotoSize(file_id='b', file_unique_id='b', width=1, height=1, file_size=1)])
    await app.message(small, bot)
    assert app.store.get('conn', 2, 41)['media_status'] == 'unavailable'
