from fsspec.implementations.local import LocalFileSystem

from scanner import Scanner

import asyncio

fs = LocalFileSystem()
scanner = Scanner(
    [
        "79852227949",
        "79934962253",
        "79037895690",
        "79934957590",
    ],
    fs)

async def main():
    async with scanner.session():
        async for msg in scanner.get_chat_history("@sea_aparts"):
            print(msg.text)

asyncio.run(main())
