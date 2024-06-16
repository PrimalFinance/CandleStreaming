import numpy as np
import pandas as pd

import datetime as dt
import pytz

from Utils.dates import Dates


class Candles:
    def __init__(self, symbol, sleeptime, sleepunit) -> None:
        self.symbol = symbol
        self.sleeptime = sleeptime
        self.sleepunit = sleepunit
        self.path = f"Trader\\LocalCandles\\{symbol.upper()}.csv"

        self.dates = Dates()

    def get_candles(self, timezone: str = "UTC") -> pd.DataFrame:

        zone_mapping = {"PST": "US/Pacific"}
        local_time = dt.datetime.now()
        local_time_zone = pytz.timezone("US/Pacific")
        localized_time = local_time_zone.localize(local_time)
        utc_time = localized_time.astimezone(pytz.utc)
        utc_time = utc_time.replace(tzinfo=None)

        df = pd.read_csv(self.path, index_col="index")
        df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])

        # Convert datetime to specified timezone.
        if timezone != "UTC":  # Pass since data is in UTC by default.
            df["datetime"] = (
                df["datetime"]
                .dt.tz_localize("UTC")
                .dt.tz_convert(zone_mapping[timezone])
            )

            # Make datetime "naive" for future calculations.
            df["datetime"] = df["datetime"].dt.tz_localize(None)

        df.set_index("datetime", inplace=True)
        candle_ranges = self.create_candle_ranges(df)
        new_df = pd.DataFrame(columns=df.columns)
        for c in candle_ranges:

            start, end = c
            temp_df = df.loc[start:end]
            temp_df.loc[:, "side"] = temp_df.loc[:, "side"].map({"buy": 0, "sell": 1})
            side = temp_df["side"].mean()

            if side <= 0.50:
                side = "buy"
            elif side > 0.50:
                side = "sell"

            if timezone != "UTC":
                if dt.datetime.now() < end:  # Make 'end' timestamp naive.
                    if self.sleepunit == "S":
                        end = dt.datetime.now().replace(microsecond=0)
                    elif self.sleepunit == "M":
                        end = dt.datetime.now().replace(second=0, microsecond=0)
                    elif self.sleepunit == "H":
                        end = dt.datetime.now().replace(
                            minute=0, second=0, microsecond=0
                        )
            else:
                if utc_time < end:
                    if self.sleepunit == "S":
                        end = utc_time.replace(microsecond=0)
                    elif self.sleepunit == "M":
                        end = utc_time.replace(second=0, microsecond=0)
                    elif self.sleepunit == "H":
                        end = utc_time.replace(minute=0, second=0, microsecond=0)

            data = {
                "date": end.date(),
                "time": end.time(),
                "price": temp_df["price"].mean(),
                "open_24h": temp_df["open_24h"].mean(),
                "low_24h": temp_df["low_24h"].mean(),
                "high_24h": temp_df["high_24h"].mean(),
                "last_size": temp_df["last_size"].mean(),
                "last_size_value": temp_df["last_size_value"].mean(),
                "bid": temp_df["bid"].mean(),
                "bid_size": temp_df["bid_size"].mean(),
                "ask": temp_df["ask"].mean(),
                "ask_size": temp_df["ask_size"].mean(),
                "volume_24h": temp_df["volume_24h"].mean(),
                "volume_30d": temp_df["volume_30d"].mean(),
                "side": side,
            }

            new_df.loc[end] = data

        return new_df

    def create_candle_ranges(self, df: pd.DataFrame):

        ranges = []
        first_datetime = df.index[0]
        first_datetime = self.dates.round_dates(
            first_datetime, timeunit=self.sleepunit, round_to=self.sleeptime
        )

        index = 0
        start_records = []
        for d in df.index:
            if index > 0:
                start = self.dates.round_dates(
                    df.index[index - 1],
                    timeunit=self.sleepunit,
                    round_to=self.sleeptime,
                )
                if start not in start_records:
                    start_records.append(start)
                    end = self.dates.add(start, self.sleeptime, self.sleepunit)
                    ranges.append((start, end))
            index += 1

        return ranges


if __name__ == "__main__":
    c = Candles("BTC-USD", 5, "M")
    c.get_candles()
