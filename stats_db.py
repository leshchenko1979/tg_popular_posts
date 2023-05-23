import datetime as dt

import pandas as pd
import supabase


class StatsDatabase:
    """Loads the channel list and the full statistics dataframe from the Supabase database.
    Calculates the last statictics dataframe and timedelta since the last statictics update.
    Saves new statictics to the database."""

    def __init__(self, client: supabase.Client):
        self.client = client

    def load_data(self) -> None:
        """Loads the channel list and the full statistics dataframe from the Supabase database."""
        self.load_channel_list()
        self.load_stats_dataframe()
        self.calc_last_stats_dataframe()
        self.calc_timedelta_since_last_stats_update()

    def load_channel_list(self):
        """Returns the list of channels from the database."""
        list_of_dicts = self.client.table("channels").select("username").execute().data
        self.channels = {item["username"] for item in list_of_dicts}

    def load_stats_dataframe(self):
        """Returns the full statistics dataframe from the database."""
        self.stats_df = pd.DataFrame(
            self.client.table("stats").select("*").execute().data
        )
        self.stats_df["created_at"] = pd.to_datetime(
            self.stats_df["created_at"], utc=True
        ).dt.tz_convert("Europe/Moscow")

    def calc_last_stats_dataframe(self):
        """Calculates the last statictics dataframe from the database."""
        self.max_datetime = self.stats_df.created_at.max()
        self.last_stats_df = self.stats_df[
            self.stats_df.created_at == self.max_datetime
        ]

    def calc_timedelta_since_last_stats_update(self):
        """Calculates the timedelta since the last statictics update."""
        self.delta = dt.datetime.now(dt.timezone.utc) - self.max_datetime

    def save_new_stats_to_db(self, stats_df: pd.DataFrame):
        """Saves the new statictics dataframe to the database."""
        data = stats_df[["username", "reach", "subscribers"]].to_dict("records")
        self.client.table("stats").insert(data).execute()
