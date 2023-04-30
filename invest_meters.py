import asyncio
import datetime as dt
import os
from collections import namedtuple

import pandas as pd
import streamlit as st
import supabase
from stqdm import stqdm as tqdm

import load_env
import supabasefs


def get_or_create_eventloop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return asyncio.get_event_loop()


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from scanner import Scanner

st.title("Подборка статистики для Инвеcт-мэтров")

LIMIT_HISTORY = dt.timedelta(days=30)  # насколько лезть вглубь чата

client = supabase.create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
fs = supabasefs.SupabaseTableFileSystem(client, "sessions")
scanner = Scanner(fs=fs, chat_cache=False)

Msg = namedtuple("Message", "username link reach reactions")

results = []

list_of_dicts = client.table("channels").select("username").execute().data
channels = {item["username"] for item in list_of_dicts}

st.subheader("Каналы")
channels


async def collect_stats(channel) -> int:
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
            )
        )

    return msgs


def calc_stats(msgs: pd.DataFrame):
    stats = msgs.groupby("username").agg({"reach": "mean"})
    stats["reach_percent_of_mean"] = stats["reach"] / stats["reach"].mean() * 100
    stats["votes"] = stats.reach / stats.reach.sum() * 100
    msgs["popularity"] = msgs.reactions / msgs.reach
    for col in ["reach", "reach_percent_of_mean", "votes"]:
        stats[col] = pd.to_numeric(stats[col].round(), downcast="integer")

    return stats.sort_values("reach", ascending=False).reset_index()


async def collect_all_stats(channels) -> list:
    with tqdm(total=len(channels)) as pbar:
        async with scanner.session(pbar):
            for channel in channels:
                pbar.set_postfix_str(channel)
                results.extend(await collect_stats(channel))
                pbar.update()


st.subheader("Статистика охватов и голоса")

if st.button("Собрать"):
    with st.spinner("Собираем статистику, можно пойти покурить..."):
        asyncio.run(collect_all_stats(channels))

    msgs = pd.DataFrame(results)
    stats = calc_stats(msgs)
    stats.to_clipboard()

    stats

    total_reach = stats.reach.sum()
    st.metric("Общий охват", total_reach)

    def make_clickable(url):
        return f'<a target="_blank" href="{url}">ссылка</a>'

    popular_posts = (
        msgs.sort_values("popularity", ascending=False)
        .groupby("username")[["username", "link", "popularity"]]
        .head(5)
    )

    popular_posts["link"] = popular_posts["link"].apply(make_clickable)
    st.subheader("Популярные посты")
    st.write(popular_posts.to_html(escape=False), unsafe_allow_html=True)