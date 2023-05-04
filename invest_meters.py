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

    global scanner, channels, loaded_stats, client

    scanner, channels, loaded_stats, client = prepare()

    st.subheader("Каналы")
    channels

    st.subheader("Статистика охватов и голоса")

    if not loaded_stats.empty:
        display_historical_stats()

    display_fresh_stats_and_posts()


@st.cache_resource(show_spinner="Подготовка...")
def prepare():
    client = supabase.create_client(
        os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
    )
    fs = supabasefs.SupabaseTableFileSystem(client, "sessions")
    scanner = Scanner(fs=fs, chat_cache=False)

    list_of_dicts = client.table("channels").select("username").execute().data
    channels = {item["username"] for item in list_of_dicts}

    loaded_stats = pd.DataFrame(client.table("stats").select("*").execute().data)
    loaded_stats["created_at"] = pd.to_datetime(
        loaded_stats["created_at"], utc=True
    ).dt.tz_convert("Europe/Moscow")

    return [scanner, channels, loaded_stats, client]


def display_historical_stats():
    chart_df = (
        loaded_stats.set_index(["created_at", "username"])
        .stack()
        .reset_index()
        .rename(columns={"level_2": "metric", 0: "value"})
    )

    fig = px.line(
        chart_df,
        x="created_at",
        y="value",
        facet_row="metric",
        facet_row_spacing=0.1,
        color="username",
        labels={"created_at": "Дата"},
    )
    fig.update_yaxes(matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(plot_bgcolor="#202020")
    st.plotly_chart(fig, use_container_width=True)

    max_datetime: dt.datetime = loaded_stats.created_at.max()
    last_stats = loaded_stats[loaded_stats.created_at == max_datetime].sort_values(
        "reach", ascending=False
    )
    del last_stats["created_at"]

    calc_reach_percent_and_votes(last_stats)

    display_stats(last_stats)

    delta = dt.datetime.now(tz=dt.timezone.utc) - max_datetime
    if delta > dt.timedelta(days=30):
        st.caption(f"Собрано {max_datetime.date()}")
    elif delta > dt.timedelta(days=1):
        st.caption(f"Собрана {delta.days} дней назад")
    else:
        st.caption(f"Собрана {delta.seconds // 3600} часов назад")


def calc_reach_percent_and_votes(stats: pd.DataFrame):
    stats["reach_percent_of_mean"] = stats["reach"] * 100 // stats["reach"].mean()
    stats["votes"] = stats.reach * 100 // stats.reach.sum()

    return stats.sort_values("reach", ascending=False).reset_index()


def display_stats(stats):
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Всего охват", stats.reach.sum())

    with col2:
        st.metric("Всего подписчиков", stats.subscribers.sum())

    stats


def display_fresh_stats_and_posts():
    if "stats" in st.session_state:
        msgs_df = st.session_state["msgs_df"]
        stats = st.session_state["stats"]

    elif st.button("Собрать свежую статистику", type="primary"):
        msgs_df, stats = collect_fresh_stats_and_posts()

    else:
        st.stop()

    display_stats(stats)
    display_popular_posts(msgs_df)


def collect_fresh_stats_and_posts():
    with st.spinner("Собираем статистику, можно пойти покурить..."):
        msg_stats, channel_stats = asyncio.run(collect_all_stats(channels))

    msgs_df = pd.DataFrame(msg_stats)
    channels_df = pd.DataFrame(channel_stats)

    calc_msg_popularity(msgs_df)
    stats = collect_stats_to_single_df(msgs_df, channels_df)
    calc_reach_percent_and_votes(stats)

    save_stats(stats)
    st.session_state["msgs_df"] = msgs_df
    st.session_state["stats"] = stats

    return [msgs_df, stats]


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
                text=msg.text or msg.caption,
            )
        )

    return msgs


async def collect_channel_stats(channel) -> Channel:
    chat = await scanner.get_chat(channel)

    return Channel(username=channel, subscribers=chat.members_count)


def calc_msg_popularity(msg_df: pd.DataFrame):
    msg_df["popularity"] = msg_df.reactions / msg_df.reach

    return msg_df


def collect_stats_to_single_df(msg_df: pd.DataFrame, channel_df: pd.DataFrame):
    stats = msg_df.groupby("username").agg({"reach": "mean"}).astype(int)
    stats["subscribers"] = channel_df.set_index("username")["subscribers"]
    stats.reset_index(inplace=True)

    return stats


def save_stats(stats: pd.DataFrame):
    client.table("stats").insert(
        stats[["username", "reach", "subscribers"]].to_dict("records")
    ).execute()


def display_popular_posts(msgs):
    st.subheader("Популярные посты")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        how_many_per_channel = st.number_input(
            "Постов с канала", 1, value=5
        )
    with col2:
        min_days = st.number_input("Не свежее скольки дней", 0, value=1)
    with col3:
        max_days = st.number_input("Не старше скольки дней", 1, value=8)
    with col4:
        sort_by = st.radio("Сортировать по", ["популярности", "каналу"])

    if min_days >= max_days:
        st.error("Минимальная дата должна быть меньше максимальной")
        return

    now = pd.Timestamp("now", tz="UTC")
    filtered_by_date = msgs[
        (pd.to_datetime(msgs.datetime, utc=True) > now - pd.DateOffset(days=max_days))
        & (pd.to_datetime(msgs.datetime, utc=True) < now - pd.DateOffset(days=min_days))
    ]

    sorted_posts = (
        filtered_by_date.sort_values("popularity", ascending=False)
        if sort_by == "популярности"
        else filtered_by_date.sort_values("username")
    )

    popular_posts = sorted_posts.groupby("username")[
        ["username", "text", "link", "popularity"]
    ].head(how_many_per_channel)

    popular_posts["link"] = popular_posts["link"].apply(make_clickable)
    popular_posts["text"] = popular_posts["text"].apply(shorten)

    st.write(popular_posts.to_html(escape=False), unsafe_allow_html=True)


def make_clickable(url):
    return f'<a target="_blank" href="{url}">ссылка</a>'


def shorten(text, max_length=200):
    return (
        text[:max_length] + "..."
        if isinstance(text, str) and len(text) > max_length
        else text
    )


try:
    main()
except RuntimeError:
    st.error("Кто-то другой использует приложение. Попробуйте через пару минут.")
