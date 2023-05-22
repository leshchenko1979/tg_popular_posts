import asyncio
import datetime as dt
import os

import pandas as pd
import plotly.express as px
import streamlit as st
import supabase
from stqdm import stqdm as tqdm

import load_env
import supabasefs
from scanner import Scanner
from stats_collector import StatsCollector

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


HISTORY_LIMIT_DAYS = 30
MIN_DATE = (
    dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=HISTORY_LIMIT_DAYS)
).replace(tzinfo=None)

scanner: Scanner
channels: set[str]
loaded_stats: pd.DataFrame
client: supabase.Client


def main():
    st.title("Подборка статистики для Инвеcт-мэтров")

    global scanner, channels, loaded_stats, client

    scanner, client = prepare_resources()
    channels, loaded_stats = load_data()

    st.subheader("Каналы")
    channels

    st.subheader("Статистика охватов и голоса")

    if not loaded_stats.empty:
        display_historical_stats()

    display_fresh_stats_and_posts()


@st.cache_resource(show_spinner="Подготовка...")
def prepare_resources():
    client = supabase.create_client(
        os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
    )
    fs = supabasefs.SupabaseTableFileSystem(client, "sessions")
    scanner = Scanner(fs=fs, chat_cache=False)

    return [scanner, client]


@st.cache_data(show_spinner="Загружаем историческую статистику", ttl=60)
def load_data():
    list_of_dicts = client.table("channels").select("username").execute().data
    channels = {item["username"] for item in list_of_dicts}

    loaded_stats = pd.DataFrame(client.table("stats").select("*").execute().data)
    loaded_stats["created_at"] = pd.to_datetime(
        loaded_stats["created_at"], utc=True
    ).dt.tz_convert("Europe/Moscow")

    return [channels, loaded_stats]


def display_historical_stats():
    display_historical_chart()

    max_datetime: dt.datetime = loaded_stats.created_at.max()
    last_stats = loaded_stats[loaded_stats.created_at == max_datetime].sort_values(
        "reach", ascending=False
    )
    del last_stats["created_at"]

    last_stats = calc_reach_percent_and_votes(last_stats)

    display_stats(last_stats)

    delta = dt.datetime.now(tz=dt.timezone.utc) - max_datetime
    if delta > dt.timedelta(days=30):
        st.caption(f"Собрано {max_datetime.date()}")
    elif delta > dt.timedelta(days=1):
        st.caption(f"Собрана {delta.days} дней назад")
    else:
        st.caption(f"Собрана {delta.seconds // 3600} часов назад")


def display_historical_chart():
    chart_df = (
        loaded_stats.set_index(["created_at", "username"])
        .stack()
        .reset_index()
        .rename(columns={"level_2": "metric", 0: "value"})
        .sort_values("created_at")
    )

    category_orders = {
        "metric": ["reach", "subscribers"],
        "username": loaded_stats[
            loaded_stats.created_at == loaded_stats.created_at.max()
        ]
        .sort_values("reach", ascending=False)
        .username.tolist(),
    }

    fig = px.line(
        chart_df,
        x="created_at",
        y="value",
        facet_row="metric",
        facet_row_spacing=0.1,
        color="username",
        labels={"created_at": "Дата"},
        category_orders=category_orders,
    )

    fig.update_yaxes(matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(plot_bgcolor="#202020")

    st.plotly_chart(fig, use_container_width=True)


def calc_reach_percent_and_votes(stats: pd.DataFrame):
    stats["reach_percent_of_mean"] = stats["reach"] * 100 // stats["reach"].mean()
    stats["votes"] = stats.reach * 100 // stats.reach.sum()
    stats = stats.sort_values("reach", ascending=False).reset_index()

    return stats


def display_stats(stats):
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Всего охват", stats.reach.sum())

    with col2:
        st.metric("Всего подписчиков", stats.subscribers.sum())

    stats = (
        stats[["username", "reach", "subscribers", "reach_percent_of_mean", "votes"]]
        .set_index("username", drop=True)
        .sort_values("reach", ascending=False)
    )
    stats


def display_fresh_stats_and_posts():
    max_datetime = loaded_stats.created_at.max()
    delta = dt.datetime.now(tz=dt.timezone.utc) - max_datetime
    needs_updating = delta > dt.timedelta(hours=12)

    if "stats" in st.session_state:
        msgs_df = st.session_state["msgs_df"]
        stats = st.session_state["stats"]

    elif st.button("Собрать свежую статистику", type="primary") or needs_updating:
        msgs_df, stats = collect_fresh_stats_and_posts()

    else:
        st.stop()

    display_stats(stats)
    display_popular_posts(msgs_df)


def collect_fresh_stats_and_posts():
    collector = StatsCollector(scanner, MIN_DATE)

    with st.spinner("Собираем статистику, можно пойти покурить..."):
        with tqdm(total=len(channels)) as pbar:
            asyncio.run(collector.collect_all_stats(channels, pbar))

    save_stats(collector.stats)

    stats = calc_reach_percent_and_votes(collector.stats)

    st.session_state["msgs_df"] = collector.msgs_df
    st.session_state["stats"] = stats

    return [collector.msgs_df, stats]


def save_stats(stats: pd.DataFrame):
    data = stats[["username", "reach", "subscribers"]].to_dict("records")
    client.table("stats").insert(data).execute()


def display_popular_posts(msgs):
    st.subheader("Популярные посты")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        how_many_per_channel = st.number_input("Постов с канала", 1, value=5)
    with col2:
        min_days = st.number_input("Не свежее скольки дней", 0, value=1)
    with col3:
        max_days = st.number_input(
            "Не старше скольки дней", min_value=1, max_value=HISTORY_LIMIT_DAYS, value=8
        )
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

    st.write(popular_posts.to_html(escape=False), unsafe_allow_html=True)


def make_clickable(url):
    return f'<a target="_blank" href="{url}">ссылка</a>'


try:
    main()
except RuntimeError:
    st.error("Кто-то другой использует приложение. Попробуйте через пару минут.")
