import json
from abc import abstractmethod
from dataclasses import dataclass
from prosperity2bt.datamodel import Order, Symbol, Trade, TradingState
from prosperity2bt.file_reader import FileReader
from typing import Any, Optional

@dataclass
class SandboxLogRow:
    timestamp: int
    sandbox_log: str
    lambda_log: str

    def with_offset(self, timestamp_offset: int) -> "SandboxLogRow":
        return SandboxLogRow(
            self.timestamp + timestamp_offset,
            self.sandbox_log,
            self.lambda_log.replace(f"[[{self.timestamp},", f"[[{self.timestamp + timestamp_offset},"),
        )

    def __str__(self) -> str:
        return json.dumps({
            "sandboxLog": self.sandbox_log,
            "lambdaLog": self.lambda_log,
            "timestamp": self.timestamp,
        }, indent=2)

@dataclass
class ActivityLogRow:
    columns: list[Any]

    @property
    def timestamp(self) -> int:
        return self.columns[1]

    def with_offset(self, timestamp_offset: int, profit_loss_offset: float) -> "ActivityLogRow":
        new_columns = self.columns[:]
        new_columns[1] += timestamp_offset
        new_columns[-1] += profit_loss_offset

        return ActivityLogRow(new_columns)

    def __str__(self) -> str:
        return ";".join(map(str, self.columns))

@dataclass
class TradeRow:
    trade: Trade

    @property
    def timestamp(self) -> int:
        return self.trade.timestamp

    def with_offset(self, timestamp_offset: int) -> "TradeRow":
        return TradeRow(Trade(
            self.trade.symbol,
            self.trade.price,
            self.trade.quantity,
            self.trade.buyer,
            self.trade.seller,
            self.trade.timestamp + timestamp_offset,
        ))

    def __str__(self) -> str:
        return "  " + f"""
  {{
    "timestamp": {self.trade.timestamp},
    "buyer": "{self.trade.buyer}",
    "seller": "{self.trade.seller}",
    "symbol": "{self.trade.symbol}",
    "currency": "SEASHELLS",
    "price": {self.trade.price},
    "quantity": {self.trade.quantity},
  }}
        """.strip()

@dataclass
class DayResults:
    round_num: int
    day_num: int

    sandbox_logs: list[SandboxLogRow]
    activity_logs: list[ActivityLogRow]
    trades: list[TradeRow]

class Component:
    """Component implementations contain the backtesting logic for a product or a group of products."""

    @staticmethod
    @abstractmethod
    def create(file_reader: FileReader, round_num: int, day_num: int) -> Optional["Component"]:
        """Creates the component for a given round/day combination, returns None if the round/day combination is not supported."""
        raise NotImplementedError()

    def supported_timestamps(self) -> list[int]:
        """Returns the timestamps the component has data for."""
        pass

    def pre_run(self, args: Any, state: TradingState) -> None:
        """Called before every Trader.run call, can be used to initialize the passed state."""
        pass

    def post_run(
            self,
            args: Any,
            state: TradingState,
            orders: dict[Symbol, list[Order]],
            conversions: Optional[int],
            results: DayResults,
            sandbox_row: SandboxLogRow,
        ) -> None:
        """Called after every Trader.run call, processes the result, updates `state` and `out` accordingly."""
        pass
