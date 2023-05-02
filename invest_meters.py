import asyncio
import datetime as dt
import os
from collections import namedtuple

import pandas as pd
import plotly.express as px
import streamlit as st
import supabase
from stqdm import stqdm as tqdm

import load_env
import supabasefs

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from scanner import Scanner

LIMIT_HISTORY = dt.timedelta(days=30)  # насколько лезть вглубь чата

Msg = namedtuple("Message", "username link reach reactions datetime text")
Channel = namedtuple("Channel", "username subscribers")


def main():
    st.title("Подборка статистики для Инвеcт-мэтров")

    prepare()

    st.subheader("Каналы")
    channels

    st.subheader("Статистика охватов и голоса")

    if not loaded_stats.empty:
        chart("История охватов", "reach")
        chart("История подписчиков", "subscribers")

    if st.button("Собрать свежую статистику", type="primary"):
        collect_stats_and_posts()


def collect_stats_and_posts():
    with st.spinner("Собираем статистику, можно пойти покурить..."):
        msg_stats, channel_stats = asyncio.run(collect_all_stats(channels))

    msgs_df = pd.DataFrame(msg_stats)
    channels_df = pd.DataFrame(channel_stats)

    stats = calc_stats(msgs_df, channels_df)
    save_stats(stats)
    stats

    total_reach = stats.reach.sum()
    st.metric("Общий охват", total_reach)

    print_popular_posts(msgs_df)


def chart(caption, y):
    st.caption(caption)
    fig = px.line(
        loaded_stats,
        x="created_at",
        y=y,
        color="username",
        labels={"created_at": "Дата"},
    )
    st.plotly_chart(fig)


def prepare():
    global scanner, channels, loaded_stats, client

    client = supabase.create_client(
        os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
    )
    fs = supabasefs.SupabaseTableFileSystem(client, "sessions")
    scanner = Scanner(fs=fs, chat_cache=False)

    list_of_dicts = client.table("channels").select("username").execute().data
    channels = {item["username"] for item in list_of_dicts}

    loaded_stats = pd.DataFrame(client.table("stats").select("*").execute().data)
    loaded_stats["created_at"] = pd.to_datetime(loaded_stats["created_at"])


async def collect_all_stats(channels) -> tuple[list[Msg], list[Channel]]:
    msg_stats = []
    channel_stats = []

    with tqdm(total=len(channels)) as pbar:
        async with scanner.session(pbar):
            for channel in channels:
                pbar.set_postfix_str(channel)

                msg_stats.extend(await collect_msg_stats(channel))
                channel_stats.append(await collect_channel_stats(channel))

                pbar.update()

    return msg_stats, channel_stats


async def collect_msg_stats(channel) -> Msg:
    msgs = []

    async for msg in scanner.get_chat_history(
        channel, min_date=dt.datetime.now() - LIMIT_HISTORY
    ):
        reactions = (
            (
                sum(reaction.count for reaction in msg.reactions.reactions)
                if msg.reactions
                else 0
            )
            + (msg.forwards or 0)
            + await scanner.get_discussion_replies_count(channel, msg.id)
        )
        msgs.append(
            Msg(
                username=channel,
                link=msg.link,
                reach=msg.views or 0,
                reactions=reactions,
                datetime=msg.date,
                text=msg.text,
            )
        )

    return msgs


async def collect_channel_stats(channel) -> Channel:
    chat = await scanner.get_chat(channel)

    return Channel(username=channel, subscribers=chat.members_count)


def calc_stats(msg_df: pd.DataFrame, channel_df: pd.DataFrame):
    stats = msg_df.groupby("username").agg({"reach": "mean"})
    stats["reach_percent_of_mean"] = stats["reach"] / stats["reach"].mean() * 100
    stats["votes"] = stats.reach / stats.reach.sum() * 100
    msg_df["popularity"] = msg_df.reactions / msg_df.reach

    stats["subscribers"] = channel_df.set_index("username")["subscribers"]

    for col in ["reach", "reach_percent_of_mean", "votes"]:
        stats[col] = pd.to_numeric(stats[col].round(), downcast="integer")

    return stats.sort_values("reach", ascending=False).reset_index()


def save_stats(stats: pd.DataFrame):
    client.table("stats").insert(
        stats[["username", "reach", "subscribers"]].to_dict("records")
    ).execute()


def print_popular_posts(msgs):
    popular_posts = (
        msgs.sort_values("popularity", ascending=False)
        .groupby("username")[["username", "text", "link", "popularity"]]
        .head(5)
    )

    popular_posts["link"] = popular_posts["link"].apply(make_clickable)
    popular_posts["text"] = popular_posts["text"].apply(shorten)
    st.subheader("Популярные посты")
    st.write(popular_posts.to_html(escape=False), unsafe_allow_html=True)


def make_clickable(url):
    return f'<a target="_blank" href="{url}">ссылка</a>'


def shorten(text, max_length=200):
    return text[:max_length] + "..." if len(text) > max_length else text


try:
    main()
except RuntimeError:
    st.error("Кто-то другой использует приложение. Попробуйте через пару минут.")
