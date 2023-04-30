import load_env

import asyncio
import datetime as dt
import os

import fsspec
import pyrogram



class Account:
    app: pyrogram.Client
    fs: fsspec.spec.AbstractFileSystem
    phone: str
    filename: str

    started: bool
    flood_wait_timeout: int
    flood_wait_from: dt.datetime
    busy: asyncio.Semaphore  # если запущена процедура, занимающая этот аккаунт

    def __init__(self, phone, fs: fsspec.spec.AbstractFileSystem):
        self.filename = f"{phone}.session"
        self.fs = fs
        self.phone = phone
        self.started = False
        self.flood_wait_timeout = 0
        self.flood_wait_from = None
        self.app = None

    async def start(self):
        if self.fs.exists(self.filename):
            with self.fs.open(self.filename, "r") as f:
                session_str = f.read()

            self.app = pyrogram.Client(
                self.phone,
                session_string=session_str,
                in_memory=True,
                no_updates=True,
            )

        else:
            print(self.phone)
            self.app = pyrogram.Client(
                self.phone,
                os.environ["API_ID"],
                os.environ["API_HASH"],
                in_memory=True,
                no_updates=True,
                phone_number=self.phone,
            )

        await self.app.start()

        self.started = True
        self.flood_wait_timeout = 0
        self.flood_wait_from = None
        self.busy = asyncio.Semaphore()

    async def stop(self):
        session_str = await self.app.export_session_string()

        with self.fs.open(self.filename, "w") as f:
            f.write(session_str)

        await self.app.stop()

        self.started = False
