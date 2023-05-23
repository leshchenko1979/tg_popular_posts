import asyncio
import contextlib
import datetime as dt
from typing import AsyncIterable

import pyrogram
from icontract import ensure, require
from tqdm import tqdm
from fsspec import AbstractFileSystem

from account import Account
from chat_cache import ChatCache, ChatCacheItem


class Scanner:
    """Выполняет запросы к телеграму, используя коллекцию аккаунтов."""

    def __init__(
        self, /, fs: AbstractFileSystem, phones: list[str] = None, chat_cache=True
    ):
        self.fs = fs
        self.phones = phones or [
            item.split(".session")[0] for item in fs.glob("*.session")
        ]

        if chat_cache:
            self.chat_cache = ChatCache(fs)
            self.chat_cache.load()
        else:
            self.chat_cache = None

        self.pbar = None

    @ensure(lambda self: all(acc.app.is_connected for acc in self.accs))
    @ensure(lambda self: all(acc.app for acc in self.accs))
    async def start_sessions(self):
        self.available_accs = asyncio.Queue()
        self.accs = [Account(phone, self.fs) for phone in self.phones]

        await asyncio.gather(*(acc.start() for acc in self.accs))

        for acc in self.accs:
            self.available_accs.put_nowait(acc)

    async def close_sessions(self):
        await asyncio.gather(*[acc.stop() for acc in self.accs])
        self.accs = []
        self.available_accs = asyncio.Queue()

    @contextlib.asynccontextmanager
    async def session(self, pbar: tqdm = None):
        SESSION_LOCK = ".session_lock"
        if self.fs.exists(SESSION_LOCK):
            raise RuntimeError("Sessions are already in use")

        self.pbar = pbar

        try:
            await self.start_sessions()

            self.fs.touch(SESSION_LOCK)

            yield

        finally:
            self.pbar = None

            self.fs.rm(SESSION_LOCK)

            await self.close_sessions()

            if self.chat_cache:
                self.chat_cache.save()

    async def get_chat(self, chat_id) -> pyrogram.types.Chat:
        if not self.chat_cache:
            return await self.process_command("get_chat", chat_id)

        if chat_id not in self.chat_cache:
            chat = await self.process_command("get_chat", chat_id)
            self.chat_cache[chat_id] = ChatCacheItem(chat)

        return self.chat_cache[chat_id].chat

    async def get_chat_members_count(self, chat_id) -> int:
        if not self.chat_cache:
            return await self.process_command("get_chat_members_count", chat_id)

        chat_cache_item = self.chat_cache[chat_id]
        if not chat_cache_item.members_count:
            chat_cache_item.members_count = await self.process_command(
                "get_chat_members_count", chat_id
            )
        return chat_cache_item.members_count

    async def get_discussion_replies_count(self, chat_id, msg_id) -> int:
        try:
            return await self.process_command(
                "get_discussion_replies_count", chat_id, msg_id
            )
        except pyrogram.errors.MsgIdInvalid:
            return 0

    async def get_chat_history(
        self, chat_id, limit=None, min_date=None
    ) -> AsyncIterable[pyrogram.types.Message]:
        async for msg in self.process_iterator(
            "get_chat_history",
            chat_id,
            limit,
            breaking_trigger=lambda msg: msg.date < min_date if min_date else False,
        ):
            yield msg

    async def get_discussion_replies(
        self, chat_id, msg_id, limit=None
    ) -> AsyncIterable[pyrogram.types.Message]:
        with contextlib.suppress(pyrogram.errors.MsgIdInvalid):
            async for msg in self.process_iterator(
                "get_discussion_replies", chat_id, msg_id, limit
            ):
                yield msg

    async def process_command(self, method: str, *args: list):
        while True:
            async with self.get_acc() as acc:
                return await getattr(acc.app, method)(*args)

    async def process_iterator(
        self, method: str, *args: list, breaking_trigger=lambda x: False
    ):
        while True:
            async with self.get_acc() as acc:
                async for result in getattr(acc.app, method)(*args):
                    if breaking_trigger(result):
                        break
                    yield result
                break

    @contextlib.asynccontextmanager
    async def get_acc(self):
        min_wait = self.min_wait()
        if min_wait and min_wait > 1000:
            available_at = dt.datetime.now() + dt.timedelta(seconds=min_wait)
            raise RuntimeError(
                f"All accounts unavailable. First available at {available_at}."
            )

        acc: Account = await self.available_accs.get()
        acc.busy = True

        try:
            yield acc
            self.available_accs.put_nowait(acc)
            acc.busy = False

        except pyrogram.errors.FloodWait as e:
            asyncio.create_task(self.flood_wait(acc, e.value))

        except Exception as e:
            self.available_accs.put_nowait(acc)
            acc.busy = False
            raise

    def min_wait(self):
        return min(
            (
                acc.flood_wait_timeout
                - (dt.datetime.now() - acc.flood_wait_from).seconds
                for acc in self.accs
                if acc.flood_wait_from
            ),
            default=None,
        )

    async def flood_wait(self, acc: Account, timeout: int):
        acc.flood_wait_from = dt.datetime.now()
        acc.flood_wait_timeout = timeout

        if self.pbar:
            old_postfix = self.pbar.postfix or ""
            self.pbar.set_postfix_str(
                ", ".join([old_postfix, f"flood_wait {timeout} secs"])
            )

        await asyncio.sleep(timeout)

        if self.pbar:
            self.pbar.set_postfix_str(old_postfix)

        self.available_accs.put_nowait(acc)

        acc.flood_wait_from = None
        acc.flood_wait_timeout = 0
