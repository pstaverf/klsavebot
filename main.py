from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import signal
import html
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import (BotCommand, BusinessConnection, BusinessMessagesDeleted, CallbackQuery,
                           FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message)
from cryptography.fernet import Fernet
from dotenv import load_dotenv

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    token: str
    database: Path
    media_dir: Path
    max_media_bytes: int
    webhook_base_url: str = ''
    webhook_host: str = '0.0.0.0'
    webhook_port: int = 8080

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
        base = os.getenv('WEBHOOK_BASE_URL', '').strip().rstrip('/')
        parsed = urlsplit(base)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or not re.fullmatch(r'/[A-Za-z0-9/_~.-]*|', parsed.path)
                or '//' in parsed.path):
            raise ValueError('Укажите публичный HTTPS-адрес WEBHOOK_BASE_URL без параметров и фрагмента')
        try:
            port = int(os.getenv('WEBHOOK_PORT', '8080'))
            if not 1 <= port <= 65535:
                raise ValueError
            parsed.port  # Validate the optional public port too.
        except ValueError:
            raise ValueError('Некорректный порт вебхука') from None
        return cls(token, Path(database), Path(os.getenv('MEDIA_DIR', 'data/media')),
                   max_mb * 1024 * 1024, base, os.getenv('WEBHOOK_HOST', '0.0.0.0'), port)


class Store:
    """SQLite operations are short and serialized by the sequential update handler."""
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS mailing_users (
                chat_id INTEGER PRIMARY KEY, last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS webhook_updates (
                update_id INTEGER PRIMARY KEY, processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS mirrors (
                bot_id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL,
                username TEXT NOT NULL, token_encrypted BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS mirrors_by_owner ON mirrors(owner_id);
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

    def remember_mailing_user(self, chat_id: int):
        self.db.execute('''INSERT INTO mailing_users (chat_id) VALUES (?)
            ON CONFLICT(chat_id) DO UPDATE SET last_seen=CURRENT_TIMESTAMP''', (chat_id,))
        self.db.commit()

    def mailing_recipients(self):
        return [row[0] for row in self.db.execute('SELECT chat_id FROM mailing_users ORDER BY chat_id')]

    def forget_mailing_user(self, chat_id: int):
        self.db.execute('DELETE FROM mailing_users WHERE chat_id=?', (chat_id,))
        self.db.commit()

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


def is_expiring_media(message: Message) -> bool:
    """Only classify a media message as expiring when Telegram supplies an explicit hint."""
    raw = message.model_dump(exclude_none=True)
    fields = {'ttl', 'ttl_seconds', 'ttl_period', 'self_destruct', 'self_destruct_time',
              'self_destruct_seconds', 'is_self_destructing', 'view_once', 'is_view_once'}

    def enabled(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            return normalized == 'true' or (normalized.isdecimal() and int(normalized) > 0)
        return False

    # Check only this message and its own media; a replied-to or forwarded message
    # can have different expiration metadata. A spoiler alone is not view-once.
    candidates = [raw]
    for name in ('photo', 'video', 'video_note', 'voice', 'audio', 'animation', 'document'):
        media = raw.get(name)
        if isinstance(media, dict):
            candidates.append(media)
        elif isinstance(media, list):
            candidates.extend(item for item in media if isinstance(item, dict))
    return any(enabled(data.get(field)) for data in candidates for field in fields)


def expiring_hint(message: Message) -> bool:
    # Preserve proactive spoiler saving without classifying spoilers as view-once.
    return bool(message.has_media_spoiler or is_expiring_media(message))


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


# Custom emoji can be unavailable to some bots; fall back to their plain equivalents.
EMOJI_EDIT = '<tg-emoji emoji-id="5879841310902324730">✏️</tg-emoji>'
EMOJI_DELETE = '<tg-emoji emoji-id="5841541824803509441">🗑</tg-emoji>'
EMOJI_EXPIRING = '<tg-emoji emoji-id="5798535677318533269">⏲</tg-emoji>'
EMOJI_WARNING = '<tg-emoji emoji-id="5881702736843511327">⚠️</tg-emoji>'
EMOJI_WELCOME = '<tg-emoji emoji-id="5906995262378741881">🙋</tg-emoji>'
EMOJI_MAILING = '<tg-emoji emoji-id="5771695636411847302">📢</tg-emoji>'
MAILING_ADMIN_ID = 6025818386


def plain_emojis(text: str) -> str:
    return re.sub(r'<tg-emoji emoji-id="[0-9]+">(.*?)</tg-emoji>', r'\1', text)


async def send_html(bot, chat_id: int, text: str, **kwargs):
    try:
        return await bot.send_message(chat_id, text, parse_mode='HTML', **kwargs)
    except TelegramBadRequest:
        if '<tg-emoji ' not in text:
            raise
        return await bot.send_message(chat_id, plain_emojis(text), parse_mode='HTML', **kwargs)


async def send_html_media(method, chat_id: int, file, caption: str):
    try:
        return await method(chat_id, file, caption=caption, parse_mode='HTML')
    except TelegramBadRequest:
        if '<tg-emoji ' not in caption:
            raise
        return await method(chat_id, file, caption=plain_emojis(caption), parse_mode='HTML')


FOOTER = '<b>Этот бот KLSave.</b> Максимум функций и меньше ограничений по функционалу.'


def sender_link(row) -> str:
    name = escape(row['sender_name'] or 'Пользователь', 80)
    sender_id = row['sender_id']
    if isinstance(sender_id, int) and sender_id > 0:
        return f'<a href="tg://user?id={sender_id}">{name}</a>'
    return name


async def notify_edit(bot: Bot, user_chat_id: int, row, old_body, new_body):
    text = (f'{EMOJI_EDIT} <b>Сообщение было изменено!</b>\n\n'
            f'{sender_link(row)} отредактировал(-а) своё сообщение (#m{row["message_id"]}):\n\n'
            f'Было: {escape(old_body if old_body is not None else "[нет старой версии]", 800)}\n'
            f'Стало: {escape(new_body, 800)}\n\n{FOOTER}')
    await send_html(bot, user_chat_id, text)


def event_text(row, *, deleted: bool, media_caption: bool = False, unavailable: bool = False, upload_failed: bool = False, is_expiring: bool = False) -> str:
    """Format the owner-facing notification, keeping captions below Telegram's limit."""
    name = sender_link(row)
    message_id = row['message_id']
    value = row['body'] or ''
    limit = 250 if media_caption else 1500
    body = html.escape(value[:limit] + ('…' if len(value) > limit else ''))
    if deleted:
        text = (f'{EMOJI_DELETE} <b>Сообщение было удалено!</b>\n\n'
                f'{name} удалил(-а) своё сообщение (#m{message_id}):\n\n')
        if row['media_type']:
            text += f'Тип сообщения: {escape(row["media_type"], 50)}'
            if value:
                text += f'\n\n<blockquote>Message: {body}</blockquote>'
        else:
            text += f'<blockquote>Message: {body}</blockquote>'
    else:
        title = 'Исчезающее медиа!' if is_expiring else 'Медиа из сообщения!'
        marker = f'{EMOJI_EXPIRING} ' if is_expiring else ''
        text = (f'{marker}<b>{title}</b>\n\n'
                f'{name} отправил(-а) своё сообщение (#m{message_id}):\n\n'
                f'Message: {body}')
    if upload_failed:
        text += f'\n\n{EMOJI_WARNING} Медиа недоступно: не удалось отправить файл.'
    elif unavailable:
        text += f'\n\n{EMOJI_WARNING} Медиа недоступно: {escape(row["media_status"] or "unavailable", 50)}'
    return f'{text}\n\n{FOOTER}'


async def notify_media_event(bot: Bot, cfg: Config, user_chat_id: int, row, *, deleted: bool, is_expiring: bool = False):
    """Use captions when supported; circles and stickers follow the text separately."""
    kind = row['media_type']
    path = row['media_path']
    candidate = Path(path).resolve() if path else None
    exists = bool(candidate and candidate.is_relative_to(cfg.media_dir.resolve()) and candidate.is_file())
    if not kind or not exists:
        await send_html(bot, user_chat_id, event_text(row, deleted=deleted, unavailable=bool(kind), is_expiring=is_expiring))
        return

    text_first = kind in {'video_note', 'sticker'}
    text = event_text(row, deleted=deleted, media_caption=not text_first, is_expiring=is_expiring)
    if text_first:
        await send_html(bot, user_chat_id, text)
        try:
            method = bot.send_video_note if kind == 'video_note' else bot.send_sticker
            await method(user_chat_id, FSInputFile(candidate))
        except Exception:
            log.exception('Не удалось отправить сохранённое медиа')
            await bot.send_message(user_chat_id, 'Не удалось отправить сохранённое медиа.')
        return

    try:
        method = {'photo': bot.send_photo, 'video': bot.send_video, 'animation': bot.send_animation,
                  'voice': bot.send_voice, 'audio': bot.send_audio}.get(kind, bot.send_document)
        await send_html_media(method, user_chat_id, FSInputFile(candidate), text)
    except Exception:
        log.exception('Не удалось отправить сохранённое медиа')
        await send_html(bot, user_chat_id, event_text(row, deleted=deleted, upload_failed=True, is_expiring=is_expiring))


def deleted_group_text(rows) -> str:
    ids = ', '.join(f'#m{row["message_id"]}' for row in rows)
    lines = []
    for row in rows:
        value = row['body'] or (f'[{row["media_type"]}]' if row['media_type'] else '')
        shortened = value[:400] + ('…' if len(value) > 400 else '')
        lines.append(f'#m{row["message_id"]}: {html.escape(shortened)}')
    entries = '\n'.join(lines)
    return (f'{EMOJI_DELETE} <b>Были удалены сообщения!</b>\n\n'
            f'{sender_link(rows[0])} удалил(-а) несколько своих сообщений ({ids}):\n\n'
            f'Message: {entries}\n\n{FOOTER}')


async def notify_deleted_group(bot: Bot, cfg: Config, user_chat_id: int, rows):
    # Keep each HTML message comfortably under Telegram's 4096 UTF-16 unit limit.
    batches = []
    batch = []
    for row in rows:
        candidate = batch + [row]
        if batch and len(deleted_group_text(candidate).encode('utf-16-le')) // 2 > 3000:
            batches.append(batch)
            batch = [row]
        else:
            batch = candidate
    if batch:
        batches.append(batch)
    for batch in batches:
        await send_html(bot, user_chat_id, deleted_group_text(batch))
        for row in batch:
            kind, path = row['media_type'], row['media_path']
            if not kind:
                continue
            candidate = Path(path).resolve() if path else None
            if not candidate or not candidate.is_relative_to(cfg.media_dir.resolve()) or not candidate.is_file():
                await bot.send_message(user_chat_id, f'#m{row["message_id"]}: медиа недоступно.')
                continue
            try:
                if kind in {'video_note', 'sticker'}:
                    method = bot.send_video_note if kind == 'video_note' else bot.send_sticker
                    await method(user_chat_id, FSInputFile(candidate))
                else:
                    method = {'photo': bot.send_photo, 'video': bot.send_video, 'animation': bot.send_animation,
                              'voice': bot.send_voice, 'audio': bot.send_audio}.get(kind, bot.send_document)
                    await method(user_chat_id, FSInputFile(candidate), caption=f'#m{row["message_id"]}')
            except Exception:
                log.exception('Не удалось отправить сохранённое медиа')
                await bot.send_message(user_chat_id, f'#m{row["message_id"]}: не удалось отправить медиа.')


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
        if connection.is_enabled:
            text = ('<b>Business Mode активирован!</b>\n'
                    '<i>Ты включил(-а) бизнес-режим для KLSave — теперь бот готов принимать сообщения и работать в полном объёме.\n'
                    f'{EMOJI_WARNING} Обрати внимание: данный бот может быть удален из-за правил Telegram, следи за новостями в нашем канале для получения зеркал.</i>')
            await send_html(bot, connection.user_chat_id, text)
        else:
            await bot.send_message(connection.user_chat_id, 'Business Mode отключён.')

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
                await notify_edit(bot, connection['user_chat_id'], current, old['body'], body)
        else:
            self.store.save(row)
            if edited:
                current = self.store.get(conn, chat, msg)
                await notify_edit(bot, connection['user_chat_id'], current, None, body)
        if not edited and not old and kind and expiring_hint(message):
            current = self.store.get(conn, chat, msg)
            await notify_media_event(bot, self.cfg, connection['user_chat_id'], current,
                                     deleted=False, is_expiring=is_expiring_media(message))

    async def save_reply(self, message: Message, bot: Bot, connection):
        reply = message.reply_to_message
        if not reply or not reply.from_user or reply.from_user.id == connection['owner_id']:
            return
        if not extract_media(reply)[0]:
            return
        reply = reply.model_copy(update={'business_connection_id': message.business_connection_id})
        old = self.store.get(message.business_connection_id, reply.chat.id, reply.message_id)
        if old and (old['deleted'] or old['sender_id'] == connection['owner_id']):
            return
        kind, media_obj = extract_media(reply)
        unique_id = getattr(media_obj, 'file_unique_id', None)
        if (old and unique_id and unique_id == old['media_unique_id'] and kind == old['media_type']
                and old['media_path'] and Path(old['media_path']).is_file()):
            media = (kind, old['media_path'], old['media_status'])
        else:
            media = await download_media(bot, reply, self.cfg)
        row = (message.business_connection_id, reply.chat.id, reply.message_id, reply.from_user.id,
               sender_name(reply), chat_name(reply), reply.text if reply.text is not None else reply.caption,
               *media, unique_id)
        self.store.save(row)
        current = self.store.get(message.business_connection_id, reply.chat.id, reply.message_id)
        await notify_media_event(bot, self.cfg, connection['user_chat_id'], current,
                                 deleted=False, is_expiring=is_expiring_media(reply))

    async def deleted(self, deleted: BusinessMessagesDeleted, bot: Bot):
        connection = await self.allowed(bot, deleted.business_connection_id)
        if not connection:
            return
        groups = {}
        for msg_id in deleted.message_ids:
            row = self.store.get(deleted.business_connection_id, deleted.chat.id, msg_id)
            if not row or row['deleted']:
                continue
            self.store.mark_deleted(deleted.business_connection_id, deleted.chat.id, msg_id)
            if row['sender_id'] != connection['owner_id']:
                groups.setdefault(row['sender_id'], []).append(row)
        for rows in groups.values():
            if len(rows) == 1:
                await notify_media_event(bot, self.cfg, connection['user_chat_id'], rows[0], deleted=True)
            else:
                await notify_deleted_group(bot, self.cfg, connection['user_chat_id'], rows)



UPDATES = ['message', 'callback_query', 'business_connection', 'business_message',
           'edited_business_message', 'deleted_business_messages']


async def setup_commands(bot):
    try:
        await bot.set_my_commands([BotCommand(command='start', description='Приветствие'),
                                   BotCommand(command='mirrors', description='Управление зеркалами'),
                                   BotCommand(command='mailing', description='Рассылка (для владельца)')])
    except Exception:
        log.warning('Не удалось установить меню команд бота')


class MirrorError(Exception):
    """A safe error message which never contains a bot token."""


@dataclass
class WebhookEndpoint:
    bot: Bot
    dispatcher: Dispatcher
    secret: str
    url: str
    store: Store | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class WebhookServer:
    """One authenticated route per bot; bot tokens are never included in URLs."""
    def __init__(self, cfg: Config, key: bytes):
        self.cfg = cfg
        self.key = key
        self.endpoints: dict[str, WebhookEndpoint] = {}
        self.prefix = urlsplit(cfg.webhook_base_url).path.rstrip('/')
        self.app = web.Application(client_max_size=5 * 1024 * 1024)
        self.app.router.add_post(self.prefix + '/telegram/{route:[0-9a-f]{64}}', self.handle)
        self.runner = None

    def credentials(self, bot_id: int):
        def derive(purpose):
            return hmac.new(self.key, f'{purpose}:{bot_id}'.encode(), hashlib.sha256).hexdigest()
        return derive('route'), derive('secret')

    def url_for(self, bot_id: int):
        route, _ = self.credentials(bot_id)
        return f'{self.cfg.webhook_base_url}/telegram/{route}'

    async def register(self, bot, dispatcher, store=None):
        route, secret = self.credentials(bot.id)
        if route in self.endpoints:
            raise RuntimeError('Этот вебхук уже зарегистрирован')
        endpoint = WebhookEndpoint(bot, dispatcher, secret, self.url_for(bot.id), store)
        self.endpoints[route] = endpoint
        try:
            ok = await bot.set_webhook(url=endpoint.url, secret_token=secret, allowed_updates=UPDATES,
                                       max_connections=1, drop_pending_updates=False)
            if not ok:
                raise RuntimeError('Telegram не подтвердил установку вебхука')
        except BaseException:
            self.endpoints.pop(route, None)
            async with endpoint.lock:
                pass
            raise
        return endpoint

    async def detach(self, bot_id: int):
        route, _ = self.credentials(bot_id)
        endpoint = self.endpoints.pop(route, None)
        if endpoint:
            # Drain any in-flight update before closing its database and bot session.
            async with endpoint.lock:
                pass
        return endpoint

    async def handle(self, request):
        route = request.match_info['route']
        endpoint = self.endpoints.get(route)
        if not endpoint:
            return web.Response(status=404)
        provided = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if not hmac.compare_digest(provided.encode('utf-8'), endpoint.secret.encode('ascii')):
            return web.Response(status=403)
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return web.Response(status=400)
        if not isinstance(payload, dict) or not isinstance(payload.get('update_id'), int):
            return web.Response(status=400)
        async with endpoint.lock:
            if self.endpoints.get(route) is not endpoint:
                return web.Response(status=404)
            if endpoint.store and endpoint.store.db.execute(
                    'SELECT 1 FROM webhook_updates WHERE update_id=?', (payload['update_id'],)).fetchone():
                return web.Response(status=200, text='OK')
            try:
                await endpoint.dispatcher.feed_raw_update(endpoint.bot, payload)
                if endpoint.store:
                    with endpoint.store.db:
                        endpoint.store.db.execute('INSERT OR IGNORE INTO webhook_updates (update_id) VALUES (?)',
                                                  (payload['update_id'],))
                        if payload['update_id'] % 100 == 0:
                            endpoint.store.db.execute("DELETE FROM webhook_updates WHERE processed_at < datetime('now', '-7 days')")
            except Exception:
                # Do not include request data or credentials in logs.
                log.error('Ошибка обработки обновления вебхука для бота %s', endpoint.bot.id)
                return web.Response(status=500)
        return web.Response(status=200, text='OK')

    async def start(self):
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        try:
            await web.TCPSite(self.runner, self.cfg.webhook_host, self.cfg.webhook_port).start()
        except BaseException:
            await self.runner.cleanup()
            self.runner = None
            raise

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
            self.runner = None


class MirrorManager:
    def __init__(self, cfg: Config, store: Store, bot_factory=None, webhook_server=None):
        self.cfg, self.store = cfg, store
        self.bot_factory = bot_factory or Bot
        self.running: dict[int, WebhookEndpoint] = {}
        self.pending: set[tuple[int, int]] = set()
        self.lock = asyncio.Lock()
        self.background: set[asyncio.Task] = set()
        self.mailings: dict[int, asyncio.Task] = {}
        key = self._load_key()
        self.cipher = Fernet(key)
        self.webhooks = webhook_server or WebhookServer(cfg, key)

    def _load_key(self):
        path = self.cfg.database.parent / 'mirror.key'
        if path.exists():
            return path.read_bytes().strip()
        if self.store.db.execute('SELECT 1 FROM mirrors LIMIT 1').fetchone():
            raise RuntimeError('Отсутствует mirror.key: восстановите ключ из резервной копии.')
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return path.read_bytes().strip()
        key = Fernet.generate_key()
        with os.fdopen(fd, 'wb') as output:
            output.write(key + b'\n')
        return key

    def list_for(self, owner_id: int):
        return self.store.db.execute('SELECT bot_id, username FROM mirrors WHERE owner_id=? ORDER BY bot_id',
                                     (owner_id,)).fetchall()

    async def _prepare(self, token: str):
        bot = None
        try:
            bot = self.bot_factory(token)
            me = await bot.get_me()
            if not me.is_bot or not me.username:
                raise MirrorError('Не удалось получить имя бота. Проверьте токен.')
            return bot, me
        except MirrorError:
            if bot:
                await bot.session.close()
            raise
        except Exception:
            if bot:
                await bot.session.close()
            raise MirrorError('Не удалось проверить токен или подключиться к Telegram. Попробуйте ещё раз.') from None

    async def _activate(self, bot, bot_id: int):
        root = self.cfg.database.parent / 'mirrors' / str(bot_id)
        cfg = Config(bot.token, root / 'app.db', root / 'media', self.cfg.max_media_bytes,
                     self.cfg.webhook_base_url, self.cfg.webhook_host, self.cfg.webhook_port)
        cfg.media_dir.mkdir(parents=True, exist_ok=True)
        store = Store(cfg.database)
        try:
            await setup_commands(bot)
            endpoint = await self.webhooks.register(bot, build_dispatcher(SaveBot(cfg, store), self), store)
        except BaseException:
            store.close()
            raise
        self.running[bot_id] = endpoint

    async def add(self, owner_id: int, token: str, progress=None):
        async def step(percent, text):
            if progress:
                await progress(percent, text)
        await step(20, 'Проверяем токен...')
        bot, me = await self._prepare(token)
        try:
            await step(40, 'Проверяем настройки бота...')
            async with self.lock:
                main_id = self.cfg.token.split(':', 1)[0]
                if str(me.id) == main_id or self.store.db.execute('SELECT 1 FROM mirrors WHERE bot_id=?', (me.id,)).fetchone():
                    raise MirrorError('Этот бот уже подключён. Используйте другой токен.')
                await step(60, 'Сохраняем настройки...')
                encrypted = self.cipher.encrypt(token.encode())
                self.store.db.execute('INSERT INTO mirrors (bot_id, owner_id, username, token_encrypted) VALUES (?, ?, ?, ?)',
                                      (me.id, owner_id, me.username, encrypted))
                self.store.db.commit()
                try:
                    await step(80, 'Подключаем вебхук...')
                    await self._activate(bot, me.id)
                except BaseException:
                    self.store.db.execute('DELETE FROM mirrors WHERE bot_id=?', (me.id,))
                    self.store.db.commit()
                    raise
            return me
        except MirrorError:
            await bot.session.close()
            raise
        except Exception:
            await bot.session.close()
            raise MirrorError('Не удалось подключить вебхук. Проверьте настройки сервера и Telegram.') from None
        except BaseException:
            await bot.session.close()
            raise

    async def restore(self):
        rows = self.store.db.execute('SELECT bot_id FROM mirrors ORDER BY bot_id').fetchall()
        for saved in rows:
            async with self.lock:
                row = self.store.db.execute('SELECT * FROM mirrors WHERE bot_id=?', (saved['bot_id'],)).fetchone()
                if not row or row['bot_id'] in self.running:
                    continue
                bot = None
                try:
                    token = self.cipher.decrypt(row['token_encrypted']).decode()
                    bot, me = await self._prepare(token)
                    if me.id != row['bot_id']:
                        raise MirrorError('ID бота не совпадает.')
                    await self._activate(bot, me.id)
                except Exception:
                    if bot:
                        await bot.session.close()
                    # Do not expose tokens, URLs or request details from Telegram errors.
                    log.warning('Не удалось восстановить зеркало %s; оно осталось в списке', row['bot_id'])

    def start_mailing(self, bot, store: Store, recipients: list[int], text: str):
        existing = self.mailings.get(bot.id)
        if existing and not existing.done():
            return False
        task = asyncio.create_task(self._mailing(bot, store, recipients, text))
        self.mailings[bot.id] = task
        def cleanup(done):
            if self.mailings.get(bot.id) is done:
                self.mailings.pop(bot.id, None)
        task.add_done_callback(cleanup)
        return True

    async def _mailing(self, bot, store: Store, recipients: list[int], text: str):
        body = f'{EMOJI_MAILING} <b>Рассылка</b>\n\n{html.escape(text)}'
        delivered = failed = 0
        for chat_id in recipients:
            for attempt in range(3):
                try:
                    await send_html(bot, chat_id, body)
                    delivered += 1
                    break
                except TelegramRetryAfter as exc:
                    # Respect the cooldown even after the last attempt before moving on.
                    await asyncio.sleep(max(0.1, float(exc.retry_after)))
                    if attempt == 2:
                        failed += 1
                        break
                except TelegramForbiddenError:
                    store.forget_mailing_user(chat_id)
                    failed += 1
                    break
                except TelegramBadRequest as exc:
                    if 'chat not found' in str(exc).lower() or 'user is deactivated' in str(exc).lower():
                        store.forget_mailing_user(chat_id)
                    failed += 1
                    break
                except Exception:
                    # Never log the broadcast text or the bot token.
                    failed += 1
                    break
            # Leave headroom for other bot notifications and respect Telegram flood limits.
            await asyncio.sleep(0.05)
        try:
            await bot.send_message(MAILING_ADMIN_ID,
                f'Рассылка завершена. Доставлено: {delivered}; не доставлено: {failed}.')
        except Exception:
            log.warning('Не удалось отправить владельцу результат рассылки')

    async def stop_mailing(self, bot_id: int):
        task = self.mailings.pop(bot_id, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def remove(self, owner_id: int, bot_id: int):
        async with self.lock:
            row = self.store.db.execute('SELECT * FROM mirrors WHERE bot_id=? AND owner_id=?',
                                        (bot_id, owner_id)).fetchone()
            if not row:
                return False, False
            self.store.db.execute('DELETE FROM mirrors WHERE bot_id=? AND owner_id=?', (bot_id, owner_id))
            self.store.db.commit()
            endpoint = await self.webhooks.detach(bot_id)
            self.running.pop(bot_id, None)
        await self.stop_mailing(bot_id)
        bot = endpoint.bot if endpoint else None
        cleared = False
        try:
            if bot is None:
                bot = self.bot_factory(self.cipher.decrypt(row['token_encrypted']).decode())
            info = await bot.get_webhook_info()
            if info.url == self.webhooks.url_for(bot_id):
                cleared = bool(await bot.delete_webhook(drop_pending_updates=False))
            else:
                # Never remove a webhook installed by someone else.
                cleared = True
        except Exception:
            log.warning('Не удалось отключить вебхук зеркала %s в Telegram', bot_id)
        finally:
            if endpoint and endpoint.store:
                endpoint.store.close()
            if bot:
                await bot.session.close()
        return True, cleared

    def remove_later(self, owner_id: int, bot_id: int, reporting_bot=None, chat_id=None):
        async def stop_and_report():
            removed, cleared = await self.remove(owner_id, bot_id)
            if reporting_bot and reporting_bot.id != bot_id:
                try:
                    if not removed:
                        text = 'Зеркало уже удалено.'
                    elif cleared:
                        text = 'Зеркало остановлено и удалено. Список: /mirrors.'
                    else:
                        text = ('Зеркало остановлено, но не удалось отключить его вебхук в Telegram. '
                                'Проверьте вебхук или отзовите токен в @BotFather.')
                    await reporting_bot.send_message(chat_id, text)
                except Exception:
                    log.warning('Не удалось отправить результат удаления зеркала')
        task = asyncio.create_task(stop_and_report())
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    async def shutdown(self):
        for bot_id in list(self.mailings):
            await self.stop_mailing(bot_id)
        if self.background:
            await asyncio.gather(*self.background, return_exceptions=True)
        for bot_id in list(self.running):
            endpoint = await self.webhooks.detach(bot_id)
            self.running.pop(bot_id, None)
            if endpoint:
                if endpoint.store:
                    endpoint.store.close()
                await endpoint.bot.session.close()


WELCOME = (f'{EMOJI_WELCOME} <b>Добро пожаловать!</b>\n\n'
           '<b>KLsave</b> — твой бот, который поможет в самых неожиданных моментах.\n\n'
           '<b>— Что он делает?</b>\n'
           'Сохраняет доступные медиа, сообщает об удалении и показывает изменения сообщений.')


def navigation(*buttons):
    return InlineKeyboardMarkup(inline_keyboard=[list(buttons)])


def button(text, data):
    return InlineKeyboardButton(text=text, callback_data=data)


class Menu:
    def __init__(self, mirrors: MirrorManager, store: Store | None = None):
        self.manager = mirrors
        self.store = store or mirrors.store

    async def show_page(self, bot, chat_id: int, page: int):
        if page == 0:
            text = WELCOME
            keyboard = navigation(button('Листать далее ›', 'menu:page:1'))
        elif page == 1:
            me = await bot.get_me()
            username = html.escape('@' + me.username) if me.username else 'юзернейм бота'
            text = (f'<b>Автоматизация чатов</b>\n\n'
                    '<blockquote>Подключи бота через раздел <b>«Автоматизация чатов»</b> в профиле. '
                    'Доступность функции зависит от аккаунта и настроек Telegram.</blockquote>\n\n'
                    '<b>Как подключить:</b>\n\n'
                    '<blockquote><b>1.</b> Перейди в свой профиль</blockquote>\n\n'
                    '<blockquote><b>2.</b> Нажми «Изменить»</blockquote>\n\n'
                    '<blockquote><b>3.</b> Найди вкладку «Автоматизация чатов»</blockquote>\n\n'
                    f'<blockquote><b>4.</b> Вставь юзернейм бота — <code>{username}</code></blockquote>\n\n'
                    '<blockquote>• Выбери чаты: «Все личные, кроме…» или «Только выбранные»\n'
                    '• Включи <b>«Управление сообщениями»</b> в разрешениях</blockquote>\n\n'
                    '<b>Жми кнопку ниже, чтобы продолжить.</b>')
            keyboard = navigation(button('‹ Вернуться назад', 'menu:page:0'), button('Листать далее ›', 'menu:page:2'))
        else:
            text = ('<b>Хочешь полный контроль?</b>\n\n'
                    'Теперь ты можешь создать своё «зеркало» на базе KLSave и управлять им по своим правилам.\n\n'
                    '<b>Что даёт зеркало:</b>\n'
                    '<blockquote>• Отдельный бот и собственное подключение к Telegram Business\n'
                    '• Собственное хранилище сообщений и медиа\n'
                    '• Управление запуском через /mirrors</blockquote>\n\n'
                    '<i>«Ты управляешь — мы помогаем.»</i>')
            keyboard = navigation(button('‹ Вернуться назад', 'menu:page:1'), button('Мои зеркала', 'menu:list:0'))
        await send_html(bot, chat_id, text, reply_markup=keyboard)

    async def start(self, message: Message, bot: Bot):
        self.manager.pending.discard((bot.id, message.from_user.id))
        await self.show_page(bot, message.chat.id, 0)

    async def mirrors(self, message: Message, bot: Bot):
        self.manager.pending.discard((bot.id, message.from_user.id))
        if not self.manager.list_for(message.from_user.id):
            await self.prompt(bot, message.chat.id, message.from_user.id)
        else:
            await self.show_mirrors(bot, message.chat.id, message.from_user.id, 0)

    async def mailing(self, message: Message, bot: Bot, command: CommandObject):
        if not message.from_user or message.from_user.id != MAILING_ADMIN_ID:
            await bot.send_message(message.chat.id, 'Рассылка доступна только владельцу бота.')
            return
        text = (command.args or '').strip()
        if not text:
            await bot.send_message(message.chat.id, 'Укажите текст: /mailing текст')
            return
        # Telegram's limit is 4096 UTF-16 units after HTML parsing; reserve space for the heading.
        if len(text.encode('utf-16-le')) // 2 > 4000:
            await bot.send_message(message.chat.id, 'Текст слишком длинный: максимум 4000 единиц UTF-16.')
            return
        existing = self.manager.mailings.get(bot.id)
        if existing and not existing.done():
            await bot.send_message(message.chat.id, 'Рассылка для этого бота уже выполняется.')
            return
        recipients = self.store.mailing_recipients()
        if not recipients:
            await bot.send_message(message.chat.id, 'Пока нет получателей рассылки.')
            return
        await bot.send_message(message.chat.id, f'Рассылка запущена. Получателей: {len(recipients)}.')
        self.manager.start_mailing(bot, self.store, recipients, text)

    async def show_mirrors(self, bot, chat_id: int, owner_id: int, page: int):
        mirrors = self.manager.list_for(owner_id)
        pages = max(1, (len(mirrors) + 9) // 10)
        page = max(0, min(page, pages - 1))
        selected = mirrors[page * 10:page * 10 + 10]
        text = '<b>Мои зеркала</b>\n\n'
        if not mirrors:
            text += 'У тебя пока нет зеркал.'
        else:
            lines = []
            for row in selected:
                status = 'подключено' if row['bot_id'] in self.manager.running else 'остановлено'
                lines.append(f'@{html.escape(row["username"])} — {status}')
            text += '\n'.join(lines) + f'\n\nСтраница {page + 1}/{pages}'
        buttons = [[button(f'Удалить @{row["username"]}', f'menu:delete:{row["bot_id"]}')]
                   for row in selected]
        paging = []
        if page:
            paging.append(button('‹ Назад', f'menu:list:{page - 1}'))
        if page + 1 < pages:
            paging.append(button('Далее ›', f'menu:list:{page + 1}'))
        if paging:
            buttons.append(paging)
        buttons.append([button('➕ Добавить зеркало', 'menu:add')])
        await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode='HTML')

    async def prompt(self, bot, chat_id: int, user_id: int):
        self.manager.pending.add((bot.id, user_id))
        await bot.send_message(chat_id,
            'Введите токен от @BotFather в формате: <code>12345:ABCDE</code>\n\n'
            'Токен — секрет: он попадёт оператору этого бота. Отправляйте его только если доверяете серверу. Существующий вебхук зеркала будет заменён.',
            parse_mode='HTML', reply_markup=navigation(button('Назад', 'menu:cancel')))

    async def callback(self, query: CallbackQuery, bot: Bot):
        message = query.message
        if not message or message.chat.type != 'private' or message.chat.id != query.from_user.id:
            await query.answer('Откройте бота в личном чате.', show_alert=True)
            return
        user_id, chat_id = query.from_user.id, message.chat.id
        data = query.data or ''
        await query.answer()
        if data.startswith('menu:delete:'):
            try:
                bot_id = int(data.rsplit(':', 1)[1])
            except ValueError:
                return
            # Authorize before deleting any mirror; run after the acknowledgement so
            # a mirror can remove itself without cancelling its callback mid-response.
            row = self.manager.store.db.execute('SELECT 1 FROM mirrors WHERE bot_id=? AND owner_id=?',
                                                (bot_id, user_id)).fetchone()
            if not row:
                await bot.send_message(chat_id, 'Зеркало не найдено или принадлежит другому пользователю.')
                return
            await bot.delete_message(chat_id, message.message_id)
            await bot.send_message(chat_id, 'Зеркало удаляется и будет остановлено. Проверить список: /mirrors.')
            # Defer all removals until after callbacks return, avoiding cross-bot lock deadlocks.
            self.manager.remove_later(user_id, bot_id, bot, chat_id)
            return
        await bot.delete_message(chat_id, message.message_id)
        if data == 'menu:cancel':
            self.manager.pending.discard((bot.id, user_id))
            return
        if data == 'menu:add':
            await self.prompt(bot, chat_id, user_id)
        elif data.startswith('menu:page:'):
            self.manager.pending.discard((bot.id, user_id))
            try:
                page = int(data.rsplit(':', 1)[1])
            except ValueError:
                return
            if 0 <= page <= 2:
                await self.show_page(bot, chat_id, page)
        elif data.startswith('menu:list:'):
            try:
                page = int(data.rsplit(':', 1)[1])
            except ValueError:
                return
            self.manager.pending.discard((bot.id, user_id))
            await self.show_mirrors(bot, chat_id, user_id, page)

    async def receive_token(self, message: Message, bot: Bot):
        if not message.from_user or (bot.id, message.from_user.id) not in self.manager.pending:
            return
        # Only private input is accepted; never echo or log the supplied token.
        token = (message.text or '').strip()
        try:
            await bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            await bot.send_message(message.chat.id, 'Не удалось удалить сообщение с токеном. Удалите его вручную.')
        if not token:
            await bot.send_message(message.chat.id, 'Отправьте токен текстом или нажмите «Назад».')
            return
        status = await bot.send_message(message.chat.id, '0% Подготавливаем подключение... ▢▢▢▢▢')
        async def progress(percent, text):
            blocks = '■' * (percent // 20) + '▢' * (5 - percent // 20)
            try:
                await bot.edit_message_text(f'{percent}% {text} {blocks}', chat_id=message.chat.id,
                                            message_id=status.message_id)
            except Exception:
                log.warning('Не удалось обновить индикатор зеркала')
        try:
            me = await self.manager.add(message.from_user.id, token, progress)
        except MirrorError as exc:
            await bot.edit_message_text(f'Не удалось добавить зеркало: {html.escape(str(exc))}',
                                        chat_id=message.chat.id, message_id=status.message_id, parse_mode='HTML')
            return
        self.manager.pending.discard((bot.id, message.from_user.id))
        await progress(100, 'Зеркало запущено!')
        await bot.send_message(message.chat.id, f'Зеркало @{html.escape(me.username)} добавлено. '
                               'Управление доступно через /mirrors. Для получения сообщений включите Business Mode для зеркала в @BotFather.', parse_mode='HTML')


def build_dispatcher(app: SaveBot, mirrors=None) -> Dispatcher:
    dp = Dispatcher()
    dp.business_connection.register(app.connection)
    dp.business_message.register(app.message)
    async def edited(message: Message, bot: Bot):
        await app.message(message, bot, edited=True)
    dp.edited_business_message.register(edited)
    dp.deleted_business_messages.register(app.deleted)
    if mirrors is not None:
        menu = Menu(mirrors, app.store)
        async def record_recipient(handler, event, data):
            # Bot API private chats can only receive messages after the user contacts the bot.
            if (event.chat.type == 'private' and event.from_user and not event.from_user.is_bot
                    and event.chat.id == event.from_user.id):
                app.store.remember_mailing_user(event.chat.id)
            return await handler(event, data)
        dp.message.outer_middleware(record_recipient)
        dp.message.register(menu.start, CommandStart(), F.chat.type == 'private')
        dp.message.register(menu.mirrors, Command('mirrors'), F.chat.type == 'private')
        dp.message.register(menu.mailing, Command('mailing'), F.chat.type == 'private')
        dp.callback_query.register(menu.callback, F.data.startswith('menu:'))
        dp.message.register(menu.receive_token, F.chat.type == 'private')
    return dp


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    cfg = Config.from_env()
    cfg.media_dir.mkdir(parents=True, exist_ok=True)
    store = Store(cfg.database)
    bot = Bot(cfg.token)
    mirrors = None
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    try:
        mirrors = MirrorManager(cfg, store)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        await mirrors.webhooks.start()
        await setup_commands(bot)
        dp = build_dispatcher(SaveBot(cfg, store), mirrors)
        try:
            await mirrors.webhooks.register(bot, dp, store)
        except Exception:
            raise RuntimeError('Не удалось зарегистрировать вебхук основного бота. Проверьте настройки и доступность Telegram.') from None
        await mirrors.restore()
        log.info('Сервер вебхуков запущен на порту %s', cfg.webhook_port)
        await stop.wait()
    finally:
        if mirrors:
            await mirrors.webhooks.stop()
            await mirrors.shutdown()
            await mirrors.webhooks.detach(bot.id)
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        store.close()
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
