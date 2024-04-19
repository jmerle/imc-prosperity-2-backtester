from collections import defaultdict
from dataclasses import dataclass
from prosperity2bt.datamodel import Symbol, Trade
from prosperity2bt.file_reader import FileReader
from typing import Optional

LIMITS = {
    "AMETHYSTS": 20,
    "STARFRUIT": 20,
    "ORCHIDS": 100,
    "CHOCOLATE": 250,
    "STRAWBERRIES": 350,
    "ROSES": 60,
    "GIFT_BASKET": 60,
    "COCONUT": 300,
    "COCONUT_COUPON": 600,
}

@dataclass
class PriceRow:
    day: int
    timestamp: int
    product: Symbol
    bid_prices: list[int]
    bid_volumes: list[int]
    ask_prices: list[int]
    ask_volumes: list[int]
    mid_price: float
    profit_loss: float

def get_column_values(columns: list[str], indices: list[int]) -> list[int]:
    values = []

    for index in indices:
        value = columns[index]
        if value == "":
            break

        values.append(int(value))

    return values

@dataclass
class BacktestData:
    round_num: int
    day_num: int

    prices: dict[int, dict[Symbol, PriceRow]]
    trades: dict[int, dict[Symbol, list[Trade]]]
    products: list[Symbol]
    profit_loss: dict[Symbol, int]

def create_backtest_data(round_num: int, day_num: int, prices: list[PriceRow], trades: list[Trade]) -> BacktestData:
    prices_by_timestamp: dict[int, dict[Symbol, PriceRow]] = defaultdict(dict)
    for row in prices:
        prices_by_timestamp[row.timestamp][row.product] = row

    trades_by_timestamp: dict[int, dict[Symbol, list[Trade]]] = defaultdict(lambda: defaultdict(list))
    for trade in trades:
        trades_by_timestamp[trade.timestamp][trade.symbol].append(trade)

    products = sorted(set(row.product for row in prices))
    profit_loss = {product: 0 for product in products}

    return BacktestData(
        round_num=round_num,
        day_num=day_num,
        prices=prices_by_timestamp,
        trades=trades_by_timestamp,
        products=products,
        profit_loss=profit_loss,
    )

def read_day_data(file_reader: FileReader, round_num: int, day_num: int) -> Optional[BacktestData]:
    prices = []
    with file_reader.file([f"round{round_num}", f"prices_round_{round_num}_day_{day_num}.csv"]) as file:
        if file is None:
            return None

        for line in file.read_text(encoding="utf-8").splitlines()[1:]:
            columns = line.split(";")

            prices.append(PriceRow(
                day=int(columns[0]),
                timestamp=int(columns[1]),
                product=columns[2],
                bid_prices=get_column_values(columns, [3, 5, 7]),
                bid_volumes=get_column_values(columns, [4, 6, 8]),
                ask_prices=get_column_values(columns, [9, 11, 13]),
                ask_volumes=get_column_values(columns, [10, 12, 14]),
                mid_price=float(columns[15]),
                profit_loss=float(columns[16]),
            ))

    trades = []
    for suffix in ["wn", "nn"]:
        with file_reader.file([f"round{round_num}", f"trades_round_{round_num}_day_{day_num}_{suffix}.csv"]) as file:
            if file is None:
                continue

            for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                columns = line.split(";")

                trades.append(Trade(
                    symbol=columns[3],
                    price=int(float(columns[5])),
                    quantity=int(columns[6]),
                    buyer=columns[1],
                    seller=columns[2],
                    timestamp=int(columns[0]),
                ))

            break

    return create_backtest_data(round_num, day_num, prices, trades)
