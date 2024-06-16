import os
import asyncio
import websockets
import json
import pandas as pd
import datetime as dt

cwd = os.getcwd()

show_exceptions = False


class Coinbase:
    def __init__(self, candle_export_path: str) -> None:
        self.socket_url = "wss://ws-feed.pro.coinbase.com"
        self.export_path = candle_export_path

    async def subscribe_to_socket(self, product_id):
        async with websockets.connect(self.socket_url) as ws:
            subscribe_message = {
                "type": "subscribe",
                "channels": [{"name": "ticker", "product_ids": [product_id]}],
            }
            await ws.send(json.dumps(subscribe_message))
            index = 0
            while True:
                message = await ws.recv()
                print(f"Message from {self.socket_url}: {message}")
                await self.write_to_csv(message, index)
                index += 1

    async def write_to_csv(self, data: dict, index: int) -> None:
        if type(data) == str:
            # Message comes in a string by default, this converts it to dictionary.
            data = json.loads(data)

        # df = pd.read_csv(f"{cwd}\\CandleStorage\\Crypto\\BTC.csv")

        try:
            ticker = data["product_id"]  # .split("-")[0]
            _date, _time = self.parse_time(data["time"])
            last_size_value = float(data["price"]) * float(data["last_size"])
            data_to_write = {
                "index": index,
                "date": _date,
                "time": _time,
                "price": data["price"],
                "open_24h": data["open_24h"],
                "low_24h": data["low_24h"],
                "high_24h": data["high_24h"],
                "last_size": data["last_size"],
                "last_size_value": last_size_value,
                "bid": data["best_bid"],
                "bid_size": data["best_bid_size"],
                "ask": data["best_ask"],
                "ask_size": data["best_ask_size"],
                "volume_24h": data["volume_24h"],
                "volume_30d": data["volume_30d"],
                "side": data["side"],
            }

            csv_file = f"{self.export_path}\\{ticker}.csv"
            # Convert dictionary to dataframe.
            df = pd.DataFrame([data_to_write], index=[data_to_write["index"]])
            try:
                df.to_csv(
                    csv_file,
                    mode="a",
                    index=False,
                    header=not pd.read_csv(csv_file).shape[0],
                )
            except FileNotFoundError as e:
                if show_exceptions:
                    print(f"[Error]: {e}")
                df.to_csv(csv_file, index=False)
            except pd.errors.EmptyDataError:
                if show_exceptions:
                    print(f"[Error]: {e}")
                df.to_csv(csv_file, index=False)

        except Exception as e:
            if show_exceptions:
                print(f"[Error]: {e}")
            pass

    def clear_csv_files(self, file_name="", all_files: bool = False):
        dir_path = f"Trader\\LocalCandles"
        if file_name == "" and all_files:

            # Get the list of all files in the directory
            files = [
                f
                for f in os.listdir(dir_path)
                if os.path.isfile(os.path.join(dir_path, f))
            ]
            for f in files:
                f_path = f"{dir_path}\\{f}"
                # Opening the file in write mode will clear the contents.
                with open(f_path, "w", newline="") as file:
                    pass
        elif file_name != "" and not all_files:
            if "." not in file_name:
                # If this exception occurs, manually add the extension.
                file_name = f"{file_name.upper()}.csv"

            f_path = f"{dir_path}\\{file_name}"
            with open(f_path, "w", newline="") as file:
                pass

    def parse_time(self, timestamp: str) -> str:
        _date, _time = timestamp.split("T")
        _time = _time.replace("Z", "")
        return _date, _time

    def convert_to_UTC(self, timestamp: str) -> dt.datetime:
        # Parse the time string to a datetime object
        dt_object = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Convert datetime object to UTC timestamp
        utc_timestamp = dt_object.timestamp()
        return utc_timestamp


async def main(ticker: str, market: str = "USD"):
    socket_urls = [
        "wss://ws-feed.pro.coinbase.com",
    ]

    c = Coinbase("Temp")
    tasks = []
    tasks.append(c.subscribe_to_socket(ticker))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main("BTC-USD"))
    # clear_csv_files(all_files=True)
