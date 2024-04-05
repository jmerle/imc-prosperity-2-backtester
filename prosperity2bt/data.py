from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from prosperity2bt.datamodel import Symbol, Trade
from typing import Iterator, Optional

LIMITS = {
    "AMETHYSTS": 20,
    "STARFRUIT": 20,
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

@dataclass
class DayData:
    round: int
    day: int

    prices: list[PriceRow]
    trades: list[Trade]

@contextmanager
def wrap_in_context_manager(path: Path) -> Iterator[Path]:
    yield path

def get_column_values(columns: list[str], indices: list[int]) -> list[int]:
    values = []

    for index in indices:
        value = columns[index]
        if value == "":
            break

        values.append(int(value))

    return values

def read_day_data(data_root: Optional[Path], round: int, day: int) -> DayData:
    prices_file_name = f"prices_round_{round}_day_{day}.csv"
    trades_file_name = f"trades_round_{round}_day_{day}_nn.csv"

    if data_root is not None:
        round_dir = data_root / f"round{round}"
        prices_file = wrap_in_context_manager(round_dir / prices_file_name)
        trades_file = wrap_in_context_manager(round_dir / trades_file_name)
    else:
        round_dir = resources.files(f"prosperity2bt.resources.round{round}")
        prices_file = resources.as_file(round_dir / prices_file_name)
        trades_file = resources.as_file(round_dir / trades_file_name)

    prices = []
    with prices_file as file:
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
    with trades_file as file:
        for line in file.read_text(encoding="utf-8").splitlines()[1:]:
            columns = line.split(";")

            trades.append(Trade(
                symbol=columns[3],
                price=int(columns[5]),
                quantity=int(columns[6]),
                buyer=columns[1],
                seller=columns[2],
                timestamp=int(columns[0]),
            ))

    return DayData(round, day, prices, trades)

def read_round_data(data_root: Optional[Path], round: int) -> list[DayData]:
    if data_root is not None:
        files = (data_root / f"round{round}").iterdir()
    else:
        files = resources.files(f"prosperity2bt.resources.round{round}").iterdir()

    days = []
    for file in files:
        if file.name.startswith("prices_round_"):
            day = int(file.stem.split("_")[-1])
            days.append(read_day_data(data_root, round, day))

    return days
