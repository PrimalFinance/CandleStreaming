import argparse
import asyncio

from Sockets.coinbase import Coinbase


async def main(ticker: str, market: str = "USD"):
    socket_urls = [
        "wss://ws-feed.pro.coinbase.com",
    ]
    # PATH WHERE CANDLES WILL BE STORED
    c = Coinbase("Temp")
    tasks = []
    tasks.append(c.subscribe_to_socket(ticker))

    await asyncio.gather(*tasks)


# if __name__ == "__main__":
#     asyncio.run(main("ETH-USD"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inputs")
    parser.add_argument("ticker", type=str, help="Ticker as input")
    market = "USD"
    args = parser.parse_args()

    arg = args.ticker.upper()

    if "-" not in arg:
        arg = f"{arg}-{market}"
    asyncio.run(main(arg))
