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
    cfg = Config('unused', tmp_path / 'app.db', tmp_path / 'media', 10000)
    store = Store(cfg.database)
    store.save_connection(connection())
    bot = SimpleNamespace(**{name: AsyncMock() for name in ('send_message', 'send_photo', 'send_video',
        'send_animation', 'send_voice', 'send_audio', 'send_document', 'send_video_note', 'send_sticker', 'get_file', 'download_file', 'get_business_connection')})
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
    bot.get_business_connection.side_effect = RuntimeError('unknown connection')
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


@pytest.mark.asyncio
async def test_two_business_accounts_are_isolated(setup):
    app, bot = setup
    second_owner = User(id=3, is_bot=False, first_name='Second')
    second_connection = connection(user=second_owner, id='second').model_copy(update={'user_chat_id': 333})
    app.store.save_connection(second_connection)
    first = msg(50, 'first secret')
    second = msg(50, 'second secret').model_copy(update={'business_connection_id': 'second'})
    await app.message(first, bot)
    await app.message(second, bot)
    await app.message(first.model_copy(update={'text': 'first changed'}), bot, edited=True)
    await app.message(second.model_copy(update={'text': 'second changed'}), bot, edited=True)
    assert bot.send_message.await_args_list[0].args[0] == 1
    assert 'first secret' in bot.send_message.await_args_list[0].args[1]
    assert bot.send_message.await_args_list[1].args[0] == 333
    assert 'second secret' in bot.send_message.await_args_list[1].args[1]
    await app.message(second.model_copy(update={'from_user': second_owner, 'message_id': 51}), bot)
    await app.message(second.model_copy(update={'from_user': second_owner, 'message_id': 51}), bot, edited=True)
    await app.deleted(BusinessMessagesDeleted(business_connection_id='second', chat=CHAT, message_ids=[50, 51]), bot)
    assert bot.send_message.await_args.args[0] == 333
    assert 'second changed' in bot.send_message.await_args.args[1]
    assert 'first' not in bot.send_message.await_args.args[1]
    assert bot.send_message.await_count == 3
    assert not app.store.get('conn', CHAT.id, 50)['deleted']
    assert app.store.get('second', CHAT.id, 50)['deleted']


@pytest.mark.asyncio
async def test_unknown_connection_resolved_and_owner_checked_per_account(setup):
    app, bot = setup
    second = User(id=3, is_bot=False, first_name='Second')
    bot.get_business_connection.return_value = connection(user=second, id='second')
    own = msg(60, user=second).model_copy(update={'business_connection_id': 'second'})
    await app.message(own, bot)
    assert app.store.get('second', CHAT.id, 60) is None
    await app.message(msg(61).model_copy(update={'business_connection_id': 'second'}), bot, edited=True)
    assert bot.send_message.await_args.args[0] == second.id
    assert bot.get_business_connection.await_count == 1


@pytest.mark.asyncio
async def test_existing_connection_cannot_transfer_archive_to_another_owner(setup):
    app, bot = setup
    await app.message(msg(62, 'secret'), bot)
    await app.connection(connection(user=OTHER), bot)
    assert app.store.connection('conn')['owner_id'] == OWNER.id
    bot.send_message.assert_not_awaited()
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[62]), bot)
    assert bot.send_message.await_args.args[0] == OWNER.id


@pytest.mark.asyncio
async def test_legacy_connection_requires_telegram_verification(tmp_path):
    path = tmp_path / 'app.db'
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE save_connections (connection_id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL, enabled INTEGER NOT NULL)')
    db.execute("INSERT INTO save_connections VALUES ('conn', 1, 1)")
    db.commit()
    db.close()
    store = Store(path)
    app = SaveBot(Config('unused', path, tmp_path / 'media', 10000), store)
    bot = SimpleNamespace(get_business_connection=AsyncMock(return_value=connection(user=OTHER)))
    assert await app.allowed(bot, 'conn') is None
    assert store.connection('conn')['user_chat_id'] is None
    bot.get_business_connection.return_value = connection()
    assert (await app.allowed(bot, 'conn'))['user_chat_id'] == OWNER.id
    store.close()


@pytest.mark.asyncio
async def test_media_and_replies_use_correct_business_owner(setup):
    app, bot = setup
    second_owner = User(id=3, is_bot=False, first_name='Second')
    app.store.save_connection(connection(user=second_owner, id='second').model_copy(update={'user_chat_id': 333}))
    bot.get_file.return_value = SimpleNamespace(file_path='image.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    incoming = msg(70, body=None, has_media_spoiler=True,
                   photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    incoming = incoming.model_copy(update={'business_connection_id': 'second'})
    await app.message(incoming, bot)
    assert bot.send_photo.await_args.args[0] == 333
    assert 'Исчезающее медиа!' in bot.send_photo.await_args.kwargs['caption']
    reply = msg(71, 'save', second_owner, reply_to_message=incoming).model_copy(update={'business_connection_id': 'second'})
    await app.message(reply, bot)
    assert bot.send_photo.await_args.args[0] == 333
    await app.deleted(BusinessMessagesDeleted(business_connection_id='second', chat=CHAT, message_ids=[70]), bot)
    assert bot.send_photo.await_args.args[0] == 333
    assert app.store.get('conn', CHAT.id, 70) is None


def test_config_does_not_require_owner_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('BOT_TOKEN', 'test-token')
    monkeypatch.setenv('WEBHOOK_BASE_URL', 'https://example.com')
    monkeypatch.delenv('OWNER_TELEGRAM_ID', raising=False)
    assert Config.from_env().token == 'test-token'


@pytest.mark.asyncio
@pytest.mark.parametrize('reply_body', ['Спасибо!', '👍', '/save', None])
async def test_any_owner_reply_triggers_media_save(setup, reply_body):
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='x.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    incoming = msg(80, body=None, photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    # An arbitrary non-text message can also be a reply.
    outgoing = msg(81, reply_body, OWNER, reply_to_message=incoming)
    await app.message(outgoing, bot)
    assert app.store.get('conn', 2, 80)['media_status'] == 'downloaded'
    assert app.store.get('conn', 2, 81) is None
    assert bot.send_photo.await_args.args[0] == OWNER.id


@pytest.mark.asyncio
async def test_reply_guards_still_apply(setup):
    app, bot = setup
    media = [PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)]
    # A response to one's own media or to a text-only message must not save anything.
    await app.message(msg(90, 'anything', OWNER, reply_to_message=msg(89, body=None, user=OWNER, photo=media)), bot)
    await app.message(msg(91, 'anything', OWNER, reply_to_message=msg(88)), bot)
    # Incoming replies must not trigger the owner's manual save operation either.
    await app.message(msg(92, 'anything', OTHER, reply_to_message=msg(87, body=None, photo=media)), bot)
    bot.get_file.assert_not_awaited()
    bot.send_message.assert_not_awaited()
    assert app.store.get('conn', 2, 87) is None
    assert app.store.get('conn', 2, 89) is None


@pytest.mark.asyncio
async def test_any_reply_to_unavailable_media_reports_failure(setup):
    app, bot = setup
    bot.get_file.side_effect = RuntimeError('not available')
    incoming = msg(95, body=None, photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    await app.message(msg(96, 'Привет', OWNER, reply_to_message=incoming), bot)
    assert app.store.get('conn', 2, 95)['media_status'] == 'unavailable'
    assert 'Медиа недоступно' in bot.send_message.await_args.args[1]
    bot.send_photo.assert_not_awaited()


@pytest.mark.asyncio
async def test_reply_reuses_previously_saved_media(setup):
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='x.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    incoming = msg(97, body=None, photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    await app.message(incoming, bot)
    saved_path = app.store.get('conn', 2, 97)['media_path']
    bot.get_file.side_effect = RuntimeError('file expired')
    await app.message(msg(98, 'любое сообщение', OWNER, reply_to_message=incoming), bot)
    assert app.store.get('conn', 2, 97)['media_path'] == saved_path
    assert app.store.get('conn', 2, 97)['media_status'] == 'downloaded'
    assert bot.get_file.await_count == 1
    assert bot.send_photo.await_count == 1


@pytest.mark.asyncio
async def test_deleted_text_format_and_escaping(setup):
    app, bot = setup
    incoming = msg(101, '<b>hello & goodbye</b>', OTHER.model_copy(update={'first_name': '<kaff>'}))
    await app.message(incoming, bot)
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[101]), bot)
    assert bot.send_message.await_args.args[1] == (
        '<b>Сообщение было удалено!</b>\n\n'
        '<a href="tg://user?id=2">&lt;kaff&gt;</a> удалил(-а) своё сообщение (#m101):\n\n'
        '<blockquote>Message: &lt;b&gt;hello &amp; goodbye&lt;/b&gt;</blockquote>\n\n'
        '<b>Этот бот KLSave.</b> Максимум функций и меньше ограничений по функционалу.'
    )
    assert bot.send_message.await_args.kwargs['parse_mode'] == 'HTML'


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,method_name', [('photo', 'send_photo'), ('video', 'send_video'),
                                               ('animation', 'send_animation'), ('document', 'send_document')])
async def test_deleted_media_has_caption(setup, kind, method_name):
    app, bot = setup
    path = app.cfg.media_dir / 'example.bin'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'media')
    app.store.save(('conn', 2, 102, 2, 'kaff', 'Chat', None, kind, str(path), 'downloaded', 'id'))
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[102]), bot)
    method = getattr(bot, method_name)
    caption = method.await_args.kwargs['caption']
    assert caption == (f'<b>Сообщение было удалено!</b>\n\n<a href="tg://user?id=2">kaff</a> удалил(-а) своё сообщение (#m102):\n\n'
                       f'Тип сообщения: {kind}\n\n<b>Этот бот KLSave.</b> Максимум функций и меньше ограничений по функционалу.')
    assert method.await_args.kwargs['parse_mode'] == 'HTML'
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_video_note_sent_after_notification(setup):
    app, bot = setup
    path = app.cfg.media_dir / 'note.mp4'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'video')
    app.store.save(('conn', 2, 103, 2, 'kaff', 'Chat', None, 'video_note', str(path), 'downloaded', 'id'))
    calls = []
    bot.send_message.side_effect = lambda *args, **kwargs: calls.append('message')
    bot.send_video_note.side_effect = lambda *args, **kwargs: calls.append('video_note')
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[103]), bot)
    assert calls == ['message', 'video_note']
    assert 'Тип сообщения: video_note' in bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_photo_expiring_caption_and_long_text_limit(setup):
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='image.jpg', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'image')
    bot.download_file.side_effect = download
    incoming = msg(104, body=None, caption='🙂' * 1000, has_media_spoiler=True,
                   photo=[PhotoSize(file_id='a', file_unique_id='a', width=1, height=1)])
    await app.message(incoming, bot)
    caption = bot.send_photo.await_args.kwargs['caption']
    assert caption.startswith('<b>Исчезающее медиа!</b>\n\n<a href="tg://user?id=2">Other</a> отправил(-а) своё сообщение (#m104):\n\nMessage: ')
    assert caption.endswith('<b>Этот бот KLSave.</b> Максимум функций и меньше ограничений по функционалу.')
    assert len(caption.encode('utf-16-le')) // 2 <= 1024
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_media_upload_falls_back_to_notice(setup):
    app, bot = setup
    path = app.cfg.media_dir / 'photo.jpg'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'image')
    app.store.save(('conn', 2, 105, 2, 'kaff', 'Chat', None, 'photo', str(path), 'downloaded', 'id'))
    bot.send_photo.side_effect = RuntimeError('failed to send')
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[105]), bot)
    text = bot.send_message.await_args.args[1]
    assert 'Сообщение было удалено!' in text
    assert 'Медиа недоступно' in text


@pytest.mark.asyncio
async def test_business_mode_activation_and_disable(setup):
    app, bot = setup
    await app.connection(connection(), bot)
    assert bot.send_message.await_args.args[0] == OWNER.id
    text = bot.send_message.await_args.args[1]
    assert text.startswith('<b>Business Mode активирован!</b>\n<i>')
    assert 'бизнес-режим для KLSave' in text
    assert 'следи за новостями в нашем канале' in text
    assert text.endswith('</i>')
    assert bot.send_message.await_args.kwargs['parse_mode'] == 'HTML'
    await app.connection(connection(enabled=False), bot)
    assert 'отключён' in bot.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_edit_format_includes_clickable_author_and_versions(setup):
    app, bot = setup
    await app.message(msg(201, 'До'), bot)
    await app.message(msg(201, 'После'), bot, edited=True)
    assert bot.send_message.await_args.args[1] == (
        '<b>Сообщение было изменено!</b>\n\n'
        '<a href="tg://user?id=2">Other</a> отредактировал(-а) своё сообщение (#m201):\n\n'
        'Было: До\nСтало: После\n\n'
        '<b>Этот бот KLSave.</b> Максимум функций и меньше ограничений по функционалу.'
    )


@pytest.mark.asyncio
async def test_multiple_deletions_grouped_by_author_excluding_owner(setup):
    app, bot = setup
    third = User(id=3, is_bot=False, first_name='Third')
    await app.message(msg(202, 'Ток немецкий номер скажи'), bot)
    await app.message(msg(203, 'Лагична же'), bot)
    await app.message(msg(204, 'different author', third), bot)
    # Simulate an old stored outgoing message; it must never be included.
    app.store.save(('conn', 2, 205, OWNER.id, 'Owner', 'Chat', 'secret', None, None, None, None))
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT,
                                              message_ids=[202, 203, 204, 205, 999]), bot)
    assert bot.send_message.await_count == 2
    group = bot.send_message.await_args_list[0].args[1]
    assert group.startswith('<b>Были удалены сообщения!</b>')
    assert '<a href="tg://user?id=2">Other</a>' in group
    assert '(#m202, #m203)' in group
    assert 'Message: #m202: Ток немецкий номер скажи\n#m203: Лагична же' in group
    assert 'secret' not in group and '#m204' not in group
    single = bot.send_message.await_args_list[1].args[1]
    assert '<a href="tg://user?id=3">Third</a>' in single
    assert '#m204' in single


@pytest.mark.asyncio
async def test_grouped_deletions_deliver_each_media(setup):
    app, bot = setup
    path = app.cfg.media_dir / 'picture.jpg'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'image')
    app.store.save(('conn', 2, 206, 2, 'Other', 'Chat', 'text', None, None, None, None))
    app.store.save(('conn', 2, 207, 2, 'Other', 'Chat', None, 'photo', str(path), 'downloaded', 'id'))
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=[206, 207]), bot)
    assert 'Были удалены сообщения!' in bot.send_message.await_args.args[1]
    assert '#m207: [photo]' in bot.send_message.await_args.args[1]
    assert bot.send_photo.await_args.args[0] == OWNER.id
    assert bot.send_photo.await_args.kwargs['caption'] == '#m207'


@pytest.mark.asyncio
async def test_large_batch_is_split_and_every_id_included(setup):
    app, bot = setup
    ids = list(range(300, 320))
    for i in ids:
        app.store.save(('conn', 2, i, 2, 'Other', 'Chat', '🙂' * 1000, None, None, None, None))
    await app.deleted(BusinessMessagesDeleted(business_connection_id='conn', chat=CHAT, message_ids=ids), bot)
    assert bot.send_message.await_count > 1
    messages = [call.args[1] for call in bot.send_message.await_args_list]
    assert all(len(text.encode('utf-16-le')) // 2 < 4096 for text in messages)
    assert all(any(f'#m{i}:' in text for text in messages) for i in ids)


@pytest.mark.asyncio
async def test_expiring_video_note_follows_text(setup):
    from aiogram.types import VideoNote
    app, bot = setup
    bot.get_file.return_value = SimpleNamespace(file_path='note.mp4', file_size=5)
    async def download(path, destination):
        destination.write_bytes(b'video')
    bot.download_file.side_effect = download
    calls = []
    bot.send_message.side_effect = lambda *args, **kwargs: calls.append('message')
    bot.send_video_note.side_effect = lambda *args, **kwargs: calls.append('video_note')
    incoming = msg(400, body=None, has_media_spoiler=True,
                   video_note=VideoNote(file_id='v', file_unique_id='v', length=10, duration=1))
    await app.message(incoming, bot)
    assert calls == ['message', 'video_note']
    text = bot.send_message.await_args.args[1]
    assert '<b>Исчезающее медиа!</b>' in text
    assert '<a href="tg://user?id=2">Other</a>' in text
    assert '#m400' in text and 'Message:' in text
    assert '<b>Этот бот KLSave.</b>' in text
