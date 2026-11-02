from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import BusinessConnection, BusinessMessagesDeleted, FSInputFile, Message
from dotenv import load_dotenv

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    token: str
    database: Path
    media_dir: Path
    max_media_bytes: int

    @classmethod
    def from_env(cls) -> Config:
        load_dotenv()
        token = os.getenv('BOT_TOKEN', '').strip()
        if not token:
            raise ValueError('Укажите BOT_TOKEN в .env')
        url = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///data/app.db')
        prefix = 'sqlite+aiosqlite:///'
        if not url.startswith(prefix):
            raise ValueError('Поддерживается только SQLite: DATABASE_URL=sqlite+aiosqlite:///data/app.db')
        database = url[len(prefix):]
        if not database or database == ':memory:':
            raise ValueError('Укажите путь к файлу базы данных')
        max_mb = int(os.getenv('MAX_MEDIA_SIZE_MB', os.getenv('SAVE_MEDIA_MAX_MB', '50')))
        if max_mb <= 0:
            raise ValueError('MAX_MEDIA_SIZE_MB должен быть больше 0')
        return cls(token, Path(database), Path(os.getenv('MEDIA_DIR', 'data/media')), max_mb * 1024 * 1024)


class Store:
    """SQLite operations are short and serialized by the sequential update handler."""
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS save_connections (
                connection_id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL, enabled INTEGER NOT NULL,
                user_chat_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS save_messages (
                connection_id TEXT NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL, sender_name TEXT, chat_title TEXT, body TEXT,
                media_type TEXT, media_path TEXT, media_status TEXT, media_unique_id TEXT, deleted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (connection_id, chat_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS save_edits (
                id INTEGER PRIMARY KEY, connection_id TEXT NOT NULL, chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL, old_body TEXT, new_body TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        if 'user_chat_id' not in {r[1] for r in self.db.execute('PRAGMA table_info(save_connections)')}:
            self.db.execute('ALTER TABLE save_connections ADD COLUMN user_chat_id INTEGER')
        if 'media_unique_id' not in {r[1] for r in self.db.execute('PRAGMA table_info(save_messages)')}:
            self.db.execute('ALTER TABLE save_messages ADD COLUMN media_unique_id TEXT')
        self.migrate_legacy()
        self.db.commit()

    def migrate_legacy(self):
        # Import existing business archives once, without modifying the old tables.
        self.db.execute('CREATE TABLE IF NOT EXISTS save_meta (key TEXT PRIMARY KEY)')
        if self.db.execute("SELECT 1 FROM save_meta WHERE key='legacy_imported'").fetchone():
            return
        tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'messages' in tables:
            cols = {r[1] for r in self.db.execute('PRAGMA table_info(messages)')}
            required = {'source', 'business_connection_id', 'chat_id', 'message_id', 'sender_id', 'sender_name', 'chat_title', 'text', 'caption', 'media_type', 'media_path', 'media_status', 'deleted', 'direction'}
            if required <= cols:
                self.db.execute('''INSERT OR IGNORE INTO save_messages (connection_id, chat_id, message_id, sender_id, sender_name, chat_title, body, media_type, media_path, media_status, media_unique_id, deleted)
                    SELECT business_connection_id, chat_id, message_id, sender_id, sender_name, chat_title,
                           COALESCE(text, caption), media_type, media_path, media_status, NULL, COALESCE(deleted, 0)
                    FROM messages WHERE source='business' AND business_connection_id IS NOT NULL
                      AND sender_id IS NOT NULL AND direction='incoming' ''')
        # Connections are verified through Telegram instead of trusting historical credentials.
        self.db.execute("INSERT INTO save_meta VALUES ('legacy_imported')")

    def connection(self, connection_id: str):
        return self.db.execute('SELECT * FROM save_connections WHERE connection_id=?', (connection_id,)).fetchone()

    def save_connection(self, connection: BusinessConnection) -> bool:
        existing = self.connection(connection.id)
        # Never transfer an existing archive to a different account.
        if existing and existing['owner_id'] != connection.user.id:
            log.error('Владелец Business-подключения изменился; событие отклонено')
            return False
        self.db.execute('''INSERT INTO save_connections (connection_id, owner_id, enabled, user_chat_id)
            VALUES (?, ?, ?, ?) ON CONFLICT(connection_id) DO UPDATE SET
            enabled=excluded.enabled, user_chat_id=excluded.user_chat_id''',
            (connection.id, connection.user.id, int(connection.is_enabled), connection.user_chat_id))
        self.db.commit()
        return True

    def get(self, conn: str, chat: int, msg: int):
        return self.db.execute('SELECT * FROM save_messages WHERE connection_id=? AND chat_id=? AND message_id=?',
                               (conn, chat, msg)).fetchone()

    def save(self, row: tuple):
        self.db.execute('''INSERT INTO save_messages
            (connection_id,chat_id,message_id,sender_id,sender_name,chat_title,body,media_type,media_path,media_status,media_unique_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(connection_id,chat_id,message_id) DO UPDATE SET
            sender_id=excluded.sender_id, sender_name=excluded.sender_name, chat_title=excluded.chat_title,
            body=excluded.body, media_type=excluded.media_type, media_path=excluded.media_path,
            media_status=excluded.media_status, media_unique_id=excluded.media_unique_id''', row)
        self.db.commit()

    def edit(self, row: tuple, old: sqlite3.Row):
        with self.db:
            self.db.execute('INSERT INTO save_edits (connection_id,chat_id,message_id,old_body,new_body) VALUES (?,?,?,?,?)',
                            (row[0], row[1], row[2], old['body'], row[6]))
            self.db.execute('''UPDATE save_messages SET sender_id=?, sender_name=?, chat_title=?, body=?,
                media_type=?, media_path=?, media_status=?, media_unique_id=? WHERE connection_id=? AND chat_id=? AND message_id=?''',
                (*row[3:], *row[:3]))

    def mark_deleted(self, conn: str, chat: int, msg: int):
        self.db.execute('UPDATE save_messages SET deleted=1 WHERE connection_id=? AND chat_id=? AND message_id=?',
                        (conn, chat, msg))
        self.db.commit()

    def close(self):
        self.db.close()


def sender_name(message: Message) -> str:
    user = message.from_user
    return ' '.join(filter(None, (user.first_name, user.last_name))) or user.username or str(user.id)


def chat_name(message: Message) -> str:
    chat = message.chat
    return chat.title or ' '.join(filter(None, (chat.first_name, chat.last_name))) or chat.username or str(chat.id)


def extract_media(message: Message):
    if message.photo:
        return 'photo', message.photo[-1]
    # Animation must precede document: Telegram may provide both.
    for kind in ('animation', 'video', 'voice', 'audio', 'video_note', 'sticker', 'document'):
        obj = getattr(message, kind, None)
        if obj:
            return kind, obj
    if getattr(message, 'paid_media', None):
        return 'paid_media', None
    return None, None


def expiring_hint(message: Message) -> bool:
    # Spoilers are not necessarily expiring, but proactively save them as before.
    if message.has_media_spoiler:
        return True
    raw = message.model_dump(exclude_none=True)
    def find(value):
        if isinstance(value, dict):
            return any((any(k in str(key).lower() for k in ('ttl', 'self_destruct', 'view_once', 'expire')) and bool(item))
                       or find(item) for key, item in value.items())
        if isinstance(value, (list, tuple)):
            return any(find(item) for item in value)
        return False
    return find(raw)


async def download_media(bot: Bot, message: Message, cfg: Config):
    kind, media = extract_media(message)
    if not kind:
        return None, None, None
    if media is None or not getattr(media, 'file_id', None):
        return kind, None, 'unavailable'
    if getattr(media, 'file_size', None) and media.file_size > cfg.max_media_bytes:
        return kind, None, 'too_large'
    target = None
    try:
        file = await bot.get_file(media.file_id)
        if not file.file_path:
            return kind, None, 'unavailable'
        if getattr(file, 'file_size', None) and file.file_size > cfg.max_media_bytes:
            return kind, None, 'too_large'
        suffix = Path(file.file_path).suffix.lower()
        if not re.fullmatch(r'\.[a-z0-9]{1,8}', suffix):
            suffix = '.bin'
        namespace = hashlib.sha256(message.business_connection_id.encode()).hexdigest()[:24]
        directory = cfg.media_dir / namespace / str(message.chat.id)
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f'{message.message_id}_{uuid4().hex}{suffix}'
        await bot.download_file(file.file_path, destination=target)
        if target.stat().st_size > cfg.max_media_bytes:
            target.unlink(missing_ok=True)
            return kind, None, 'too_large'
        return kind, str(target), 'downloaded'
    except Exception:
        if target:
            target.unlink(missing_ok=True)
        log.warning('Не удалось скачать медиа: чат %s, сообщение %s', message.chat.id, message.message_id)
        return kind, None, 'unavailable'


def escape(value, limit=1000):
    return html.escape(str(value or '—')[:limit])


async def notify(bot: Bot, cfg: Config, user_chat_id: int, title: str, row, body: str, *, send_media=False):
    text = (f'<b>{escape(title)}</b>\nЧат: {escape(row["chat_title"], 120)} '
            f'(<code>{row["chat_id"]}</code>)\nАвтор: {escape(row["sender_name"], 120)}\n\n{body}')
    path = row['media_path']
    if send_media and path:
        # A database path must never allow sending unrelated local files.
        candidate = Path(path).resolve()
        if candidate.is_relative_to(cfg.media_dir.resolve()) and candidate.is_file():
            try:
                kind = row['media_type']
                method = {'photo': bot.send_photo, 'video': bot.send_video, 'animation': bot.send_animation,
                          'voice': bot.send_voice, 'audio': bot.send_audio}.get(kind, bot.send_document)
                # Send the text separately to avoid Telegram's 1024-character caption limit.
                await bot.send_message(user_chat_id, text, parse_mode='HTML')
                await method(user_chat_id, FSInputFile(candidate))
                return
            except Exception:
                log.exception('Не удалось отправить сохранённое медиа')
                # Text may already have been sent; do not send it twice.
                return
    if send_media and row['media_type']:
        text += f'\n\nМедиа недоступно: {escape(row["media_status"])}'
    await bot.send_message(user_chat_id, text, parse_mode='HTML')


class SaveBot:
    def __init__(self, cfg: Config, store: Store):
        self.cfg, self.store = cfg, store

    async def allowed(self, bot: Bot, connection_id: str | None):
        if not connection_id:
            return None
        row = self.store.connection(connection_id)
        # Older databases did not store the notification chat. Verify them with Telegram.
        if row is None or row['user_chat_id'] is None:
            try:
                connection = await bot.get_business_connection(connection_id)
            except Exception:
                log.warning('Не удалось проверить Business-подключение')
                return None
            if connection.id != connection_id or not self.store.save_connection(connection):
                return None
            row = self.store.connection(connection_id)
        return row if row and row['enabled'] else None

    async def connection(self, connection: BusinessConnection, bot: Bot):
        if not self.store.save_connection(connection):
            return
        status = 'подключён' if connection.is_enabled else 'отключён'
        await bot.send_message(connection.user_chat_id, f'Save Mode {status}.')

    async def start(self, message: Message):
        await message.answer('Save Mode работает. Подключите бота в настройках Telegram Business.')

    async def message(self, message: Message, bot: Bot, edited=False):
        connection = await self.allowed(bot, message.business_connection_id)
        if not connection:
            return
        # No sender means ownership cannot be established safely. Do not notify on it.
        sender = message.from_user
        if not sender:
            return
        if sender.id == connection['owner_id']:
            if not edited:
                await self.save_reply(message, bot, connection)
            return
        conn, chat, msg = message.business_connection_id, message.chat.id, message.message_id
        old = self.store.get(conn, chat, msg)
        # A message previously known to be outgoing must never produce a notification.
        if old and old['sender_id'] == connection['owner_id']:
            return
        if old and old['deleted']:
            return
        body = message.text if message.text is not None else message.caption
        kind, media_obj = extract_media(message)
        # Reuse the stored copy on caption-only edits and duplicate updates.
        unique_id = getattr(media_obj, 'file_unique_id', None)
        if old and unique_id and unique_id == old['media_unique_id'] and kind == old['media_type'] and old['media_path'] and Path(old['media_path']).is_file():
            media = (kind, old['media_path'], old['media_status'])
        else:
            media = await download_media(bot, message, self.cfg)
        row = (conn, chat, msg, sender.id, sender_name(message), chat_name(message), body, *media, unique_id)
        if edited and old:
            if body != old['body'] or media != (old['media_type'], old['media_path'], old['media_status']) or unique_id != old['media_unique_id']:
                self.store.edit(row, old)
                current = self.store.get(conn, chat, msg)
                await notify(bot, self.cfg, connection['user_chat_id'], 'Сообщение изменено', current,
                             f'Было: {escape(old["body"], 1000)}\nСтало: {escape(body, 1000)}')
        else:
            self.store.save(row)
            if edited:
                current = self.store.get(conn, chat, msg)
                await notify(bot, self.cfg, connection['user_chat_id'], 'Сообщение изменено', current,
                             f'Было: [нет старой версии]\nСтало: {escape(body, 1000)}')
        if not edited and not old and kind and expiring_hint(message):
            current = self.store.get(conn, chat, msg)
            await notify(bot, self.cfg, connection['user_chat_id'], 'Исчезающее медиа', current,
                         f'Текст: {escape(body, 1000)}', send_media=True)

    async def save_reply(self, message: Message, bot: Bot, connection):
        reply = message.reply_to_message
        if not reply or not reply.from_user or reply.from_user.id == connection['owner_id']:
            return
        command = (message.text or message.caption or '').strip().lower()
        if command not in {'/save', '.save', 'save', 'сохрани', '.сейв', '/сейв'} and not expiring_hint(reply):
            return
        if not extract_media(reply)[0]:
            return
        reply = reply.model_copy(update={'business_connection_id': message.business_connection_id})
        old = self.store.get(message.business_connection_id, reply.chat.id, reply.message_id)
        if old and (old['deleted'] or old['sender_id'] == connection['owner_id']):
            return
        media = await download_media(bot, reply, self.cfg)
        unique_id = getattr(extract_media(reply)[1], 'file_unique_id', None)
        row = (message.business_connection_id, reply.chat.id, reply.message_id, reply.from_user.id,
               sender_name(reply), chat_name(reply), reply.text if reply.text is not None else reply.caption,
               *media, unique_id)
        self.store.save(row)
        current = self.store.get(message.business_connection_id, reply.chat.id, reply.message_id)
        await notify(bot, self.cfg, connection['user_chat_id'], 'Медиа из ответа', current,
                     f'Текст: {escape(current["body"], 1000)}', send_media=True)

    async def deleted(self, deleted: BusinessMessagesDeleted, bot: Bot):
        connection = await self.allowed(bot, deleted.business_connection_id)
        if not connection:
            return
        for msg_id in deleted.message_ids:
            row = self.store.get(deleted.business_connection_id, deleted.chat.id, msg_id)
            if not row or row['deleted']:
                continue
            self.store.mark_deleted(deleted.business_connection_id, deleted.chat.id, msg_id)
            if row['sender_id'] == connection['owner_id']:
                continue
            await notify(bot, self.cfg, connection['user_chat_id'], 'Сообщение удалено', row,
                         f'Текст: {escape(row["body"], 2000)}', send_media=True)


def build_dispatcher(app: SaveBot) -> Dispatcher:
    dp = Dispatcher()
    dp.business_connection.register(app.connection)
    dp.business_message.register(app.message)
    async def edited(message: Message, bot: Bot):
        await app.message(message, bot, edited=True)
    dp.edited_business_message.register(edited)
    dp.deleted_business_messages.register(app.deleted)
    dp.message.register(app.start, CommandStart(), F.chat.type == 'private')
    return dp


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    cfg = Config.from_env()
    cfg.media_dir.mkdir(parents=True, exist_ok=True)
    store = Store(cfg.database)
    bot = Bot(cfg.token)
    try:
        dp = build_dispatcher(SaveBot(cfg, store))
        # Ordered handling prevents edits/deletes racing with media downloads and saves.
        await dp.start_polling(bot, handle_as_tasks=False, allowed_updates=[
            'message', 'business_connection', 'business_message', 'edited_business_message', 'deleted_business_messages'])
    finally:
        store.close()
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
