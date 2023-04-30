import orjson
import contextlib
from tqdm import tqdm


class ProgressKeeper:
    def __init__(self, fs):
        (
            self.scanned_users,
            self.scanned_channels,
            self.new_users,
            self.scanned_users,
        ) = (set(), set(), set(), set())
        self.fs = fs
        self.pbar = None

    def load(self):
        lowering_func = lambda x: x.lower() if isinstance(x, str) else x

        with self.fs.open(".chat_lists.json", "rb") as f:
            lists = orjson.loads(f.read())
            self.scanned_channels = {
                lowering_func(x) for x in lists["scanned_channels"]
            }
            self.scanned_users = {lowering_func(x) for x in lists["scanned_users"]}
            self.new_channels = {lowering_func(x) for x in lists["new_channels"]}
            self.new_users = {lowering_func(x) for x in lists["new_users"]}

    def save(self):
        sorting_func = lambda x: x.lower() if isinstance(x, str) else ""

        lists = {
            "scanned_channels": sorted(list(self.scanned_channels), key=sorting_func),
            "new_channels": sorted(list(self.new_channels), key=sorting_func),
            "scanned_users": sorted(list(self.scanned_users), key=sorting_func),
            "new_users": sorted(list(self.new_users), key=sorting_func),
        }

        with self.fs.open(".chat_lists.json", "wb") as f:
            f.write(orjson.dumps(lists))

    @contextlib.asynccontextmanager
    async def session(self, pbar: tqdm = None):
        self.load()
        self.pbar = pbar
        try:
            yield
        finally:
            self.save()
            self.pbar = None

    def schedule(self, channels, users):
        lowering_func = lambda x: x.lower() if isinstance(x, str) else x

        self.new_channels |= {
            lowering_func(x) for x in channels
        } - self.scanned_channels
        self.new_users |= {lowering_func(x) for x in users} - self.scanned_users

    @contextlib.contextmanager
    def pop_user(self):
        try:
            user_to_scan = self.new_users.pop()

            if self.pbar:
                old_postfix = self.pbar.postfix or ""
                self.pbar.set_postfix_str(", ".join([old_postfix, f"user: {user_to_scan}"]))

            yield user_to_scan

            self.scanned_users |= {user_to_scan}

        except Exception:
            self.new_users.add(user_to_scan)
            raise

        finally:
            if self.pbar:
                self.pbar.set_postfix_str(old_postfix)


    @contextlib.contextmanager
    def pop_channel(self):
        try:
            channel_to_scan = self.new_channels.pop()

            if self.pbar:
                old_postfix = self.pbar.postfix or ""
                self.pbar.set_postfix_str(", ".join([old_postfix, f"user: {channel_to_scan}"]))

            yield channel_to_scan

            self.scanned_channels |= {channel_to_scan}

        except Exception:
            self.new_channels.add(channel_to_scan)
            raise

        finally:
            if self.pbar:
                self.pbar.set_postfix_str(old_postfix)
