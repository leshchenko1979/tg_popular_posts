import datetime as dt
from collections import namedtuple

import pandas as pd

from scanner import Scanner

LIMIT_HISTORY = dt.timedelta(days=30)  # насколько лезть вглубь чата


Msg = namedtuple("Message", "username link reach reactions datetime text")
Channel = namedtuple("Channel", "username subscribers")


class StatsCollector:
    scanner: Scanner

    def __init__(self, scanner, min_date=None):
        self.scanner = scanner
        self.min_date = min_date

    async def collect_all_stats(self, channels, pbar=None):
        msg_stats = []
        channel_stats = []

        async with self.scanner.session(pbar):
            for channel in channels:
                if pbar:
                    pbar.set_postfix_str(channel)

                msg_stats.extend(await self.collect_msg_stats(channel))
                channel_stats.append(await self.collect_channel_stats(channel))

                if pbar:
                    pbar.update()

        self.msgs_df = pd.DataFrame(msg_stats)
        self.channels_df = pd.DataFrame(channel_stats)

        self.calc_msg_popularity()
        self.collect_stats_to_single_df()

    async def collect_msg_stats(self, channel) -> list[Msg]:
        msgs = []

        async for msg in self.scanner.get_chat_history(channel, min_date=self.min_date):
            reactions = (
                (
                    sum(reaction.count for reaction in msg.reactions.reactions)
                    if msg.reactions
                    else 0
                )
                + (msg.forwards or 0)
                + await self.scanner.get_discussion_replies_count(channel, msg.id)
            )
            msgs.append(
                Msg(
                    username=channel,
                    link=msg.link,
                    reach=msg.views or 0,
                    reactions=reactions,
                    datetime=msg.date,
                    text=shorten(msg.text or msg.caption),
                )
            )

        return msgs

    async def collect_channel_stats(self, channel) -> Channel:
        chat = await self.scanner.get_chat(channel)

        return Channel(username=channel, subscribers=chat.members_count)

    def calc_msg_popularity(self):
        self.msgs_df["popularity"] = self.msgs_df.reactions / self.msgs_df.reach

    def collect_stats_to_single_df(self):
        self.stats = self.msgs_df.groupby("username").agg({"reach": "mean"}).astype(int)
        self.stats["subscribers"] = self.channels_df.set_index("username")[
            "subscribers"
        ]
        self.stats.reset_index(inplace=True)


def shorten(text: str, max_length=200):
    return (
        text.encode("utf-8").decode("utf-8")[:max_length] + "..."
        if isinstance(text, str) and len(text) > max_length
        else text
    )
